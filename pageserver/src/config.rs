//! Functions for handling page server configuration options
//!
//! Configuration options can be set in the pageserver.toml configuration
//! file, or on the command line.
//! See also `settings.md` for better description on every parameter.

use anyhow::{bail, ensure, Context, Result};
use toml_edit;
use toml_edit::{Document, Item};
use zenith_utils::postgres_backend::AuthType;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use std::convert::TryInto;
use std::env;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use crate::layered_repository::TIMELINES_SEGMENT_NAME;

pub mod defaults {
    use const_format::formatcp;

    pub const DEFAULT_PG_LISTEN_PORT: u16 = 64000;
    pub const DEFAULT_PG_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_PG_LISTEN_PORT}");
    pub const DEFAULT_HTTP_LISTEN_PORT: u16 = 9898;
    pub const DEFAULT_HTTP_LISTEN_ADDR: &str = formatcp!("127.0.0.1:{DEFAULT_HTTP_LISTEN_PORT}");

    // FIXME: This current value is very low. I would imagine something like 1 GB or 10 GB
    // would be more appropriate. But a low value forces the code to be exercised more,
    // which is good for now to trigger bugs.
    pub const DEFAULT_CHECKPOINT_DISTANCE: u64 = 256 * 1024 * 1024;
    pub const DEFAULT_CHECKPOINT_PERIOD: &str = "1 s";

    pub const DEFAULT_GC_HORIZON: u64 = 64 * 1024 * 1024;
    pub const DEFAULT_GC_PERIOD: &str = "100 s";

    pub const DEFAULT_SUPERUSER: &str = "zenith_admin";
    pub const DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNC: usize = 100;
    pub const DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS: u32 = 10;

    pub const DEFAULT_PAGE_CACHE_SIZE: usize = 8192;
    pub const DEFAULT_MAX_FILE_DESCRIPTORS: usize = 100;

    ///
    /// Default built-in configuration file.
    ///
    pub const DEFAULT_CONFIG_FILE: &str = formatcp!(
        r###"
# Initial configuration file created by 'pageserver --init'

#listen_pg_addr = '{DEFAULT_PG_LISTEN_ADDR}'
#listen_http_addr = '{DEFAULT_HTTP_LISTEN_ADDR}'

#checkpoint_distance = {DEFAULT_CHECKPOINT_DISTANCE} # in bytes
#checkpoint_period = '{DEFAULT_CHECKPOINT_PERIOD}'

#gc_period = '{DEFAULT_GC_PERIOD}'
#gc_horizon = {DEFAULT_GC_HORIZON}

#max_file_descriptors = {DEFAULT_MAX_FILE_DESCRIPTORS}

# initial superuser role name to use when creating a new tenant
#initial_superuser_name = '{DEFAULT_SUPERUSER}'

# [remote_storage]

"###
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageServerConf {
    /// Example (default): 127.0.0.1:64000
    pub listen_pg_addr: String,
    /// Example (default): 127.0.0.1:9898
    pub listen_http_addr: String,

    // Flush out an inmemory layer, if it's holding WAL older than this
    // This puts a backstop on how much WAL needs to be re-digested if the
    // page server crashes.
    pub checkpoint_distance: u64,
    pub checkpoint_period: Duration,

    pub gc_horizon: u64,
    pub gc_period: Duration,
    pub superuser: String,

    pub page_cache_size: usize,
    pub max_file_descriptors: usize,

    // Repository directory, relative to current working directory.
    // Normally, the page server changes the current working directory
    // to the repository, and 'workdir' is always '.'. But we don't do
    // that during unit testing, because the current directory is global
    // to the process but different unit tests work on different
    // repositories.
    pub workdir: PathBuf,

    pub pg_distrib_dir: PathBuf,

    pub auth_type: AuthType,

    pub auth_validation_public_key_path: Option<PathBuf>,
    pub remote_storage_config: Option<RemoteStorageConfig>,
}

/// External backup storage configuration, enough for creating a client for that storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStorageConfig {
    /// Max allowed number of concurrent sync operations between pageserver and the remote storage.
    pub max_concurrent_sync: NonZeroUsize,
    /// Max allowed errors before the sync task is considered failed and evicted.
    pub max_sync_errors: NonZeroU32,
    /// The storage connection configuration.
    pub storage: RemoteStorageKind,
}

/// A kind of a remote storage to connect to, with its connection configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteStorageKind {
    /// Storage based on local file system.
    /// Specify a root folder to place all stored relish data into.
    LocalFs(PathBuf),
    /// AWS S3 based storage, storing all relishes into the root
    /// of the S3 bucket from the config.
    AwsS3(S3Config),
}

/// AWS S3 bucket coordinates and access credentials to manage the bucket contents (read and write).
#[derive(Clone, PartialEq, Eq)]
pub struct S3Config {
    /// Name of the bucket to connect to.
    pub bucket_name: String,
    /// The region where the bucket is located at.
    pub bucket_region: String,
    /// A "subfolder" in the bucket, to use the same bucket separately by multiple pageservers at once.
    pub prefix_in_bucket: Option<String>,
    /// "Login" to use when connecting to bucket.
    /// Can be empty for cases like AWS k8s IAM
    /// where we can allow certain pods to connect
    /// to the bucket directly without any credentials.
    pub access_key_id: Option<String>,
    /// "Password" to use when connecting to bucket.
    pub secret_access_key: Option<String>,
    /// A base URL to send S3 requests to.
    /// By default, the endpoint is derived from a region name, assuming it's
    /// an AWS S3 region name, erroring on wrong region name.
    /// Endpoint provides a way to support other S3 flavors and their regions.
    ///
    /// Example: `http://127.0.0.1:5000`
    pub endpoint: Option<String>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket_name", &self.bucket_name)
            .field("bucket_region", &self.bucket_region)
            .field("prefix_in_bucket", &self.prefix_in_bucket)
            .finish()
    }
}

impl PageServerConf {
    //
    // Repository paths, relative to workdir.
    //

    pub fn tenants_path(&self) -> PathBuf {
        self.workdir.join("tenants")
    }

    pub fn tenant_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenants_path().join(tenantid.to_string())
    }

    pub fn tags_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join("refs").join("tags")
    }

    pub fn tag_path(&self, tag_name: &str, tenantid: &ZTenantId) -> PathBuf {
        self.tags_path(tenantid).join(tag_name)
    }

    pub fn timelines_path(&self, tenantid: &ZTenantId) -> PathBuf {
        self.tenant_path(tenantid).join(TIMELINES_SEGMENT_NAME)
    }

    pub fn timeline_path(&self, timelineid: &ZTimelineId, tenantid: &ZTenantId) -> PathBuf {
        self.timelines_path(tenantid).join(timelineid.to_string())
    }

    pub fn ancestor_path(&self, timelineid: &ZTimelineId, tenantid: &ZTenantId) -> PathBuf {
        self.timeline_path(timelineid, tenantid).join("ancestor")
    }

    //
    // Postgres distribution paths
    //

    pub fn pg_bin_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("bin")
    }

    pub fn pg_lib_dir(&self) -> PathBuf {
        self.pg_distrib_dir.join("lib")
    }

    /// Parse a configuration file (pageserver.toml) into a PageServerConf struct,
    /// validating the input and failing on errors.
    ///
    /// This leaves any options not present in the file in the built-in defaults.
    pub fn parse_and_validate(toml: &Document, workdir: &Path) -> Result<Self> {
        use defaults::*;

        let mut conf = PageServerConf {
            workdir: workdir.to_path_buf(),

            listen_pg_addr: DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            checkpoint_distance: DEFAULT_CHECKPOINT_DISTANCE,
            checkpoint_period: humantime::parse_duration(DEFAULT_CHECKPOINT_PERIOD)?,
            gc_horizon: DEFAULT_GC_HORIZON,
            gc_period: humantime::parse_duration(DEFAULT_GC_PERIOD)?,
            page_cache_size: DEFAULT_PAGE_CACHE_SIZE,
            max_file_descriptors: DEFAULT_MAX_FILE_DESCRIPTORS,

            pg_distrib_dir: PathBuf::new(),
            auth_validation_public_key_path: None,
            auth_type: AuthType::Trust,

            remote_storage_config: None,

            superuser: DEFAULT_SUPERUSER.to_string(),
        };

        for (key, item) in toml.iter() {
            match key {
                "listen_pg_addr" => conf.listen_pg_addr = parse_toml_string(key, item)?,
                "listen_http_addr" => conf.listen_http_addr = parse_toml_string(key, item)?,
                "checkpoint_distance" => conf.checkpoint_distance = parse_toml_u64(key, item)?,
                "checkpoint_period" => conf.checkpoint_period = parse_toml_duration(key, item)?,
                "gc_horizon" => conf.gc_horizon = parse_toml_u64(key, item)?,
                "gc_period" => conf.gc_period = parse_toml_duration(key, item)?,
                "initial_superuser_name" => conf.superuser = parse_toml_string(key, item)?,
                "page_cache_size" => conf.page_cache_size = parse_toml_u64(key, item)? as usize,
                "max_file_descriptors" => {
                    conf.max_file_descriptors = parse_toml_u64(key, item)? as usize
                }
                "pg_distrib_dir" => {
                    conf.pg_distrib_dir = PathBuf::from(parse_toml_string(key, item)?)
                }
                "auth_validation_public_key_path" => {
                    conf.auth_validation_public_key_path =
                        Some(PathBuf::from(parse_toml_string(key, item)?))
                }
                "auth_type" => conf.auth_type = parse_toml_auth_type(key, item)?,
                "remote_storage" => {
                    conf.remote_storage_config = Some(Self::parse_remote_storage_config(item)?)
                }
                _ => bail!("unrecognized pageserver option '{}'", key),
            }
        }

        if conf.auth_type == AuthType::ZenithJWT {
            let auth_validation_public_key_path = conf
                .auth_validation_public_key_path
                .get_or_insert_with(|| workdir.join("auth_public_key.pem"));
            ensure!(
                auth_validation_public_key_path.exists(),
                format!(
                    "Can't find auth_validation_public_key at '{}'",
                    auth_validation_public_key_path.display()
                )
            );
        }

        if conf.pg_distrib_dir == PathBuf::new() {
            conf.pg_distrib_dir = env::current_dir()?.join("tmp_install")
        };
        if !conf.pg_distrib_dir.join("bin/postgres").exists() {
            bail!(
                "Can't find postgres binary at {}",
                conf.pg_distrib_dir.display()
            );
        }

        Ok(conf)
    }

    /// subroutine of parse_config(), to parse the `[remote_storage]` table.
    fn parse_remote_storage_config(toml: &toml_edit::Item) -> anyhow::Result<RemoteStorageConfig> {
        let local_path = toml.get("local_path");
        let bucket_name = toml.get("bucket_name");
        let bucket_region = toml.get("bucket_region");

        let max_concurrent_sync: NonZeroUsize = if let Some(s) = toml.get("max_concurrent_sync") {
            parse_toml_u64("max_concurrent_sync", s)
                .and_then(|toml_u64| {
                    toml_u64.try_into().with_context(|| {
                        format!("'max_concurrent_sync' value {} is too large", toml_u64)
                    })
                })
                .ok()
                .and_then(NonZeroUsize::new)
                .context("'max_concurrent_sync' must be a non-zero positive integer")?
        } else {
            NonZeroUsize::new(defaults::DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNC).unwrap()
        };
        let max_sync_errors: NonZeroU32 = if let Some(s) = toml.get("max_sync_errors") {
            parse_toml_u64("max_sync_errors", s)
                .and_then(|toml_u64| {
                    toml_u64.try_into().with_context(|| {
                        format!("'max_sync_errors' value {} is too large", toml_u64)
                    })
                })
                .ok()
                .and_then(NonZeroU32::new)
                .context("'max_sync_errors' must be a non-zero positive integer")?
        } else {
            NonZeroU32::new(defaults::DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS).unwrap()
        };

        let storage = match (local_path, bucket_name, bucket_region) {
            (None, None, None) => bail!("no 'local_path' nor 'bucket_name' option"),
            (_, Some(_), None) => {
                bail!("'bucket_region' option is mandatory if 'bucket_name' is given ")
            }
            (_, None, Some(_)) => {
                bail!("'bucket_name' option is mandatory if 'bucket_region' is given ")
            }
            (None, Some(bucket_name), Some(bucket_region)) => RemoteStorageKind::AwsS3(S3Config {
                bucket_name: parse_toml_string("bucket_name", bucket_name)?,
                bucket_region: parse_toml_string("bucket_region", bucket_region)?,
                access_key_id: toml
                    .get("access_key_id")
                    .map(|access_key_id| parse_toml_string("access_key_id", access_key_id))
                    .transpose()?,
                secret_access_key: toml
                    .get("secret_access_key")
                    .map(|secret_access_key| {
                        parse_toml_string("secret_access_key", secret_access_key)
                    })
                    .transpose()?,
                prefix_in_bucket: toml
                    .get("prefix_in_bucket")
                    .map(|prefix_in_bucket| parse_toml_string("prefix_in_bucket", prefix_in_bucket))
                    .transpose()?,
                endpoint: toml
                    .get("endpoint")
                    .map(|endpoint| parse_toml_string("endpoint", endpoint))
                    .transpose()?,
            }),
            (Some(local_path), None, None) => RemoteStorageKind::LocalFs(PathBuf::from(
                parse_toml_string("local_path", local_path)?,
            )),
            (Some(_), Some(_), _) => bail!("local_path and bucket_name are mutually exclusive"),
        };

        Ok(RemoteStorageConfig {
            max_concurrent_sync,
            max_sync_errors,
            storage,
        })
    }

    #[cfg(test)]
    pub fn test_repo_dir(test_name: &str) -> PathBuf {
        PathBuf::from(format!("../tmp_check/test_{}", test_name))
    }

    #[cfg(test)]
    pub fn dummy_conf(repo_dir: PathBuf) -> Self {
        PageServerConf {
            checkpoint_distance: defaults::DEFAULT_CHECKPOINT_DISTANCE,
            checkpoint_period: Duration::from_secs(10),
            gc_horizon: defaults::DEFAULT_GC_HORIZON,
            gc_period: Duration::from_secs(10),
            page_cache_size: defaults::DEFAULT_PAGE_CACHE_SIZE,
            max_file_descriptors: defaults::DEFAULT_MAX_FILE_DESCRIPTORS,
            listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
            listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
            superuser: "zenith_admin".to_string(),
            workdir: repo_dir,
            pg_distrib_dir: PathBuf::new(),
            auth_type: AuthType::Trust,
            auth_validation_public_key_path: None,
            remote_storage_config: None,
        }
    }
}

// Helper functions to parse a toml Item

fn parse_toml_string(name: &str, item: &Item) -> Result<String> {
    let s = item
        .as_str()
        .with_context(|| format!("configure option {} is not a string", name))?;
    Ok(s.to_string())
}

fn parse_toml_u64(name: &str, item: &Item) -> Result<u64> {
    // A toml integer is signed, so it cannot represent the full range of an u64. That's OK
    // for our use, though.
    let i: i64 = item
        .as_integer()
        .with_context(|| format!("configure option {} is not an integer", name))?;
    if i < 0 {
        bail!("configure option {} cannot be negative", name);
    }
    Ok(i as u64)
}

fn parse_toml_duration(name: &str, item: &Item) -> Result<Duration> {
    let s = item
        .as_str()
        .with_context(|| format!("configure option {} is not a string", name))?;

    Ok(humantime::parse_duration(s)?)
}

fn parse_toml_auth_type(name: &str, item: &Item) -> Result<AuthType> {
    let v = item
        .as_str()
        .with_context(|| format!("configure option {} is not a string", name))?;
    AuthType::from_str(v)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::{tempdir, TempDir};

    use super::*;

    const ALL_BASE_VALUES_TOML: &str = r#"
# Initial configuration file created by 'pageserver --init'

listen_pg_addr = '127.0.0.1:64000'
listen_http_addr = '127.0.0.1:9898'

checkpoint_distance = 111 # in bytes
checkpoint_period = '111 s'

gc_period = '222 s'
gc_horizon = 222

page_cache_size = 444
max_file_descriptors = 333

# initial superuser role name to use when creating a new tenant
initial_superuser_name = 'zzzz'

    "#;

    #[test]
    fn parse_defaults() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;
        // we have to create dummy pathes to overcome the validation errors
        let config_string = format!("pg_distrib_dir='{}'", pg_distrib_dir.display());
        let toml = config_string.parse()?;

        let parsed_config =
            PageServerConf::parse_and_validate(&toml, &workdir).unwrap_or_else(|e| {
                panic!("Failed to parse config '{}', reason: {}", config_string, e)
            });

        assert_eq!(
            parsed_config,
            PageServerConf {
                listen_pg_addr: defaults::DEFAULT_PG_LISTEN_ADDR.to_string(),
                listen_http_addr: defaults::DEFAULT_HTTP_LISTEN_ADDR.to_string(),
                checkpoint_distance: defaults::DEFAULT_CHECKPOINT_DISTANCE,
                checkpoint_period: humantime::parse_duration(defaults::DEFAULT_CHECKPOINT_PERIOD)?,
                gc_horizon: defaults::DEFAULT_GC_HORIZON,
                gc_period: humantime::parse_duration(defaults::DEFAULT_GC_PERIOD)?,
                superuser: defaults::DEFAULT_SUPERUSER.to_string(),
                page_cache_size: defaults::DEFAULT_PAGE_CACHE_SIZE,
                max_file_descriptors: defaults::DEFAULT_MAX_FILE_DESCRIPTORS,
                workdir,
                pg_distrib_dir,
                auth_type: AuthType::Trust,
                auth_validation_public_key_path: None,
                remote_storage_config: None,
            },
            "Correct defaults should be used when no config values are provided"
        );

        Ok(())
    }

    #[test]
    fn parse_basic_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;

        let config_string = format!(
            "{}pg_distrib_dir='{}'",
            ALL_BASE_VALUES_TOML,
            pg_distrib_dir.display()
        );
        let toml = config_string.parse()?;

        let parsed_config =
            PageServerConf::parse_and_validate(&toml, &workdir).unwrap_or_else(|e| {
                panic!("Failed to parse config '{}', reason: {}", config_string, e)
            });

        assert_eq!(
            parsed_config,
            PageServerConf {
                listen_pg_addr: "127.0.0.1:64000".to_string(),
                listen_http_addr: "127.0.0.1:9898".to_string(),
                checkpoint_distance: 111,
                checkpoint_period: Duration::from_secs(111),
                gc_horizon: 222,
                gc_period: Duration::from_secs(222),
                superuser: "zzzz".to_string(),
                page_cache_size: 444,
                max_file_descriptors: 333,
                workdir,
                pg_distrib_dir,
                auth_type: AuthType::Trust,
                auth_validation_public_key_path: None,
                remote_storage_config: None,
            },
            "Should be able to parse all basic config values correctly"
        );

        Ok(())
    }

    #[test]
    fn parse_remote_fs_storage_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;

        let local_storage_path = tempdir.path().join("local_remote_storage");

        let identical_toml_declarations = &[
            format!(
                r#"[remote_storage]
local_path = '{}'"#,
                local_storage_path.display()
            ),
            format!(
                "remote_storage={{local_path='{}'}}",
                local_storage_path.display()
            ),
        ];

        for remote_storage_config_str in identical_toml_declarations {
            let config_string = format!(
                r#"{}
pg_distrib_dir='{}'

{}"#,
                ALL_BASE_VALUES_TOML,
                pg_distrib_dir.display(),
                remote_storage_config_str,
            );

            let toml = config_string.parse()?;

            let parsed_remote_storage_config = PageServerConf::parse_and_validate(&toml, &workdir)
                .unwrap_or_else(|e| {
                    panic!("Failed to parse config '{}', reason: {}", config_string, e)
                })
                .remote_storage_config
                .expect("Should have remote storage config for the local FS");

            assert_eq!(
            parsed_remote_storage_config,
            RemoteStorageConfig {
                max_concurrent_sync: NonZeroUsize::new(
                    defaults::DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNC
                )
                .unwrap(),
                max_sync_errors: NonZeroU32::new(defaults::DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS)
                    .unwrap(),
                storage: RemoteStorageKind::LocalFs(local_storage_path.clone()),
            },
            "Remote storage config should correctly parse the local FS config and fill other storage defaults"
        );
        }
        Ok(())
    }

    #[test]
    fn parse_remote_s3_storage_config() -> anyhow::Result<()> {
        let tempdir = tempdir()?;
        let (workdir, pg_distrib_dir) = prepare_fs(&tempdir)?;

        let bucket_name = "some-sample-bucket".to_string();
        let bucket_region = "eu-north-1".to_string();
        let prefix_in_bucket = "test_prefix".to_string();
        let access_key_id = "SOMEKEYAAAAASADSAH*#".to_string();
        let secret_access_key = "SOMEsEcReTsd292v".to_string();
        let endpoint = "http://localhost:5000".to_string();
        let max_concurrent_sync = NonZeroUsize::new(111).unwrap();
        let max_sync_errors = NonZeroU32::new(222).unwrap();

        let identical_toml_declarations = &[
            format!(
                r#"[remote_storage]
max_concurrent_sync = {}
max_sync_errors = {}
bucket_name = '{}'
bucket_region = '{}'
prefix_in_bucket = '{}'
access_key_id = '{}'
secret_access_key = '{}'
endpoint = '{}'"#,
                max_concurrent_sync, max_sync_errors, bucket_name, bucket_region, prefix_in_bucket, access_key_id, secret_access_key, endpoint
            ),
            format!(
                "remote_storage={{max_concurrent_sync={}, max_sync_errors={}, bucket_name='{}', bucket_region='{}', prefix_in_bucket='{}', access_key_id='{}', secret_access_key='{}', endpoint='{}'}}",
                max_concurrent_sync, max_sync_errors, bucket_name, bucket_region, prefix_in_bucket, access_key_id, secret_access_key, endpoint
            ),
        ];

        for remote_storage_config_str in identical_toml_declarations {
            let config_string = format!(
                r#"{}
pg_distrib_dir='{}'

{}"#,
                ALL_BASE_VALUES_TOML,
                pg_distrib_dir.display(),
                remote_storage_config_str,
            );

            let toml = config_string.parse()?;

            let parsed_remote_storage_config = PageServerConf::parse_and_validate(&toml, &workdir)
                .unwrap_or_else(|e| {
                    panic!("Failed to parse config '{}', reason: {}", config_string, e)
                })
                .remote_storage_config
                .expect("Should have remote storage config for S3");

            assert_eq!(
                parsed_remote_storage_config,
                RemoteStorageConfig {
                    max_concurrent_sync,
                    max_sync_errors,
                    storage: RemoteStorageKind::AwsS3(S3Config {
                        bucket_name: bucket_name.clone(),
                        bucket_region: bucket_region.clone(),
                        access_key_id: Some(access_key_id.clone()),
                        secret_access_key: Some(secret_access_key.clone()),
                        prefix_in_bucket: Some(prefix_in_bucket.clone()),
                        endpoint: Some(endpoint.clone())
                    }),
                },
                "Remote storage config should correctly parse the S3 config"
            );
        }
        Ok(())
    }

    fn prepare_fs(tempdir: &TempDir) -> anyhow::Result<(PathBuf, PathBuf)> {
        let tempdir_path = tempdir.path();

        let workdir = tempdir_path.join("workdir");
        fs::create_dir_all(&workdir)?;

        let pg_distrib_dir = tempdir_path.join("pg_distrib");
        fs::create_dir_all(&pg_distrib_dir)?;
        let postgres_bin_dir = pg_distrib_dir.join("bin");
        fs::create_dir_all(&postgres_bin_dir)?;
        fs::write(postgres_bin_dir.join("postgres"), "I'm postgres, trust me")?;

        Ok((workdir, pg_distrib_dir))
    }
}
