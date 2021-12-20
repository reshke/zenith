//! This module is responsible for locating and loading paths in a local setup.
//!
//! Now it also provides init method which acts like a stub for proper installation
//! script which will use local paths.

use anyhow::{bail, Context};
use nix::NixPath;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::{env, fs};
use zenith_utils::auth::{encode_from_key_file, Claims, Scope};
use zenith_utils::zid::ZTenantId;

pub use pojo_structs::{LocalEnv, PageServerConf, SafekeeperConf};

impl LocalEnv {
    /// Locate and load config
    pub fn load_config() -> anyhow::Result<Self> {
        let repopath = base_path();

        if !repopath.exists() {
            bail!(
                "Zenith config is not found in {}. You need to run 'zenith init' first",
                repopath.to_str().unwrap()
            );
        }

        let config = fs::read_to_string(repopath.join("config"))?;
        let mut result = Self::parse_config(&config)?;
        result.set_base_data_dir(repopath);

        Ok(result)
    }

    // this function is used only for testing purposes in CLI e g generate tokens during init
    pub fn generate_auth_token(&self, claims: &Claims) -> anyhow::Result<String> {
        let private_key_path = if self.private_key_path().is_absolute() {
            self.private_key_path().to_path_buf()
        } else {
            self.base_data_dir().join(self.private_key_path())
        };

        let key_data = fs::read(private_key_path)?;
        encode_from_key_file(claims, &key_data)
    }

    //
    // Initialize a new Zenith repository
    //
    pub fn init(&mut self) -> anyhow::Result<()> {
        // check if config already exists
        let base_path = self.base_data_dir().to_path_buf();
        if base_path.is_empty() {
            bail!("repository base path is missing");
        }
        if base_path.exists() {
            bail!(
                "directory '{}' already exists. Perhaps already initialized?",
                base_path.to_str().unwrap()
            );
        }

        fs::create_dir(&base_path)?;

        // generate keys for jwt
        // openssl genrsa -out private_key.pem 2048
        let private_key_path;
        if self.private_key_path().is_empty() {
            private_key_path = base_path.join("auth_private_key.pem");
            let keygen_output = Command::new("openssl")
                .arg("genrsa")
                .args(&["-out", private_key_path.to_str().unwrap()])
                .arg("2048")
                .stdout(Stdio::null())
                .output()
                .with_context(|| "failed to generate auth private key")?;
            if !keygen_output.status.success() {
                bail!(
                    "openssl failed: '{}'",
                    String::from_utf8_lossy(&keygen_output.stderr)
                );
            }
            self.set_private_key_path(PathBuf::from("auth_private_key.pem"));

            let public_key_path = base_path.join("auth_public_key.pem");
            // openssl rsa -in private_key.pem -pubout -outform PEM -out public_key.pem
            let keygen_output = Command::new("openssl")
                .arg("rsa")
                .args(&["-in", private_key_path.to_str().unwrap()])
                .arg("-pubout")
                .args(&["-outform", "PEM"])
                .args(&["-out", public_key_path.to_str().unwrap()])
                .stdout(Stdio::null())
                .output()
                .with_context(|| "failed to generate auth private key")?;
            if !keygen_output.status.success() {
                bail!(
                    "openssl failed: '{}'",
                    String::from_utf8_lossy(&keygen_output.stderr)
                );
            }
        }

        let new_auth_token = self.generate_auth_token(&Claims::new(None, Scope::PageServerApi))?;
        self.set_pageserver_auth_token(new_auth_token);

        fs::create_dir_all(self.pg_data_dirs_path())?;

        for safekeeper in self.safekeepers() {
            fs::create_dir_all(self.safekeeper_data_dir(&safekeeper.name))?;
        }

        // Currently, the user first passes a config file with 'zenith init --config=<path>'
        // We read that in, in `create_config`, and fill any missing defaults. Then it's saved
        // to .zenith/config.

        fs::write(base_path.join("config"), self.toml_string())?;

        Ok(())
    }
    pub fn fill_defaults(&mut self) -> anyhow::Result<()> {
        // Find postgres binaries.
        // Follow POSTGRES_DISTRIB_DIR if set, otherwise look in "tmp_install".
        if self.pg_distrib_dir().is_empty() {
            let default_pg_distrib_dir =
                if let Some(postgres_bin) = env::var_os("POSTGRES_DISTRIB_DIR") {
                    postgres_bin.into()
                } else {
                    let cwd = env::current_dir()?;
                    cwd.join("tmp_install")
                };

            self.set_pg_distrib_dir(default_pg_distrib_dir);
        }

        let pg_binary_subpath = Path::new("bin").join("postgres");
        if !self.pg_distrib_dir().join(&pg_binary_subpath).exists() {
            bail!(
                "Can't find postgres binary ('{}') in pg distrib dir '{}'",
                pg_binary_subpath.display(),
                self.pg_distrib_dir().display()
            );
        }

        // Find zenith binaries.
        if self.zenith_distrib_dir().is_empty() {
            let zenith_distrib_dir = env::current_exe()?.parent().unwrap().to_owned();
            self.set_zenith_distrib_dir(zenith_distrib_dir);
        }
        for binary in ["pageserver", "safekeeper"] {
            if !self.zenith_distrib_dir().join(binary).exists() {
                bail!(
                    "Can't find binary '{}' in zenith distrib dir '{}'",
                    binary,
                    self.zenith_distrib_dir().display()
                );
            }
        }

        // If no initial tenant ID was given, generate it.
        if self.default_tenantid().is_none() {
            self.set_default_tenantid(ZTenantId::generate())
        }

        Ok(())
    }
}

fn base_path() -> PathBuf {
    match std::env::var_os("ZENITH_REPO_DIR") {
        Some(val) => PathBuf::from(val.to_str().unwrap()),
        None => ".zenith".into(),
    }
}

// TODO kb explain pojo and `toml: ` caveats.
mod pojo_structs {
    use std::{
        convert::TryFrom,
        path::{Path, PathBuf},
        str::FromStr,
    };

    use anyhow::{bail, ensure, Context};
    use toml_edit::{Document, Item, Table, Value};
    use zenith_utils::{postgres_backend::AuthType, zid::ZTenantId};

    //
    // This data structures represents zenith CLI config
    //
    // It is deserialized from the .zenith/config file, or the config file passed
    // to 'zenith init --config=<path>' option. See control_plane/simple.conf for
    // an example.
    //
    #[derive(Clone, Debug)]
    pub struct LocalEnv {
        // Base directory for all the nodes (the pageserver, safekeepers and
        // compute nodes).
        //
        // This is not stored in the config file. Rather, this is the path where the
        // config file itself is. It is read from the ZENITH_REPO_DIR env variable or
        // '.zenith' if not given.
        base_data_dir: PathBuf,

        // Path to postgres distribution. It's expected that "bin", "include",
        // "lib", "share" from postgres distribution are there. If at some point
        // in time we will be able to run against vanilla postgres we may split that
        // to four separate paths and match OS-specific installation layout.
        pg_distrib_dir: PathBuf,

        // Path to pageserver binary.
        zenith_distrib_dir: PathBuf,

        // Default tenant ID to use with the 'zenith' command line utility, when
        // --tenantid is not explicitly specified.
        default_tenantid: Option<ZTenantId>,

        // used to issue tokens during e.g pg start
        private_key_path: PathBuf,

        pageserver: PageServerConf,

        safekeepers: Vec<SafekeeperConf>,

        // The original toml file (with possible changes from defaults and overrides
        // from command line). All the fields above are derived from this.
        toml: Document,
    }

    impl LocalEnv {
        // installation paths
        pub fn pg_bin_dir(&self) -> PathBuf {
            self.pg_distrib_dir.join("bin")
        }
        pub fn pg_lib_dir(&self) -> PathBuf {
            self.pg_distrib_dir.join("lib")
        }

        pub fn pageserver_bin(&self) -> anyhow::Result<PathBuf> {
            Ok(self.zenith_distrib_dir.join("pageserver"))
        }

        pub fn safekeeper_bin(&self) -> anyhow::Result<PathBuf> {
            Ok(self.zenith_distrib_dir.join("safekeeper"))
        }

        pub fn pg_data_dirs_path(&self) -> PathBuf {
            self.base_data_dir.join("pgdatadirs").join("tenants")
        }

        pub fn pg_data_dir(&self, tenantid: &ZTenantId, branch_name: &str) -> PathBuf {
            self.pg_data_dirs_path()
                .join(tenantid.to_string())
                .join(branch_name)
        }

        // TODO: move pageserver files into ./pageserver
        pub fn pageserver_data_dir(&self) -> PathBuf {
            self.base_data_dir.clone()
        }

        pub fn safekeeper_data_dir(&self, node_name: &str) -> PathBuf {
            self.base_data_dir.join("safekeepers").join(node_name)
        }

        pub fn base_data_dir(&self) -> &Path {
            &self.base_data_dir
        }

        pub fn set_base_data_dir(&mut self, base_data_dir: PathBuf) {
            // non-toml property, so skip amending the toml
            self.base_data_dir = base_data_dir;
        }

        pub fn safekeepers(&self) -> &[SafekeeperConf] {
            &self.safekeepers
        }

        pub fn pageserver(&self) -> &PageServerConf {
            &self.pageserver
        }

        pub fn set_pageserver_auth_token(&mut self, auth_token: String) {
            self.pageserver.set_auth_token(auth_token);
            self.toml.insert(
                "pageserver",
                toml_edit::Item::Table(self.pageserver.toml.clone()),
            );
        }

        pub fn pg_distrib_dir(&self) -> &Path {
            &self.pg_distrib_dir
        }

        pub fn set_pg_distrib_dir(&mut self, pg_distrib_dir: PathBuf) {
            self.toml.insert(
                "pg_distrib_dir",
                toml_edit::value(pg_distrib_dir.display().to_string()),
            );
            self.pg_distrib_dir = pg_distrib_dir;
        }

        pub fn zenith_distrib_dir(&self) -> &Path {
            &self.zenith_distrib_dir
        }

        pub fn set_zenith_distrib_dir(&mut self, zenith_distrib_dir: PathBuf) {
            self.toml.insert(
                "zenith_distrib_dir",
                toml_edit::value(zenith_distrib_dir.display().to_string()),
            );
            self.zenith_distrib_dir = zenith_distrib_dir;
        }

        pub fn default_tenantid(&self) -> Option<ZTenantId> {
            self.default_tenantid
        }

        pub fn set_default_tenantid(&mut self, default_tenantid: ZTenantId) {
            self.default_tenantid = Some(default_tenantid);
            self.toml.insert(
                "default_tenantid",
                toml_edit::value(default_tenantid.to_string()),
            );
        }

        /// Locate and load config
        pub fn parse_config(toml_str: &str) -> anyhow::Result<Self> {
            // load and parse file
            let mut toml = toml_str.parse::<Document>()?;
            set_disclaimer_prefix(&mut toml);

            let mut env = Self {
                toml,
                base_data_dir: PathBuf::new(),
                pg_distrib_dir: PathBuf::new(),
                zenith_distrib_dir: PathBuf::new(),
                default_tenantid: None,
                private_key_path: PathBuf::new(),
                pageserver: PageServerConf::from_toml(Table::new())?,
                safekeepers: Vec::new(),
            };

            for (key, value) in env.toml.iter() {
                match key {
                    "pg_distrib_dir" => {
                        if let Some(val) = value.as_str() {
                            env.pg_distrib_dir = PathBuf::from(val);
                        } else {
                            bail!("value of option {} is not a string", key);
                        }
                    }
                    "zenith_distrib_dir" => {
                        if let Some(val) = value.as_str() {
                            env.zenith_distrib_dir = PathBuf::from(val);
                        } else {
                            bail!("value of option {} is not a string", key);
                        }
                    }
                    "default_tenantid" => {
                        if let Some(val) = value.as_str() {
                            env.default_tenantid = Some(ZTenantId::from_str(val)?);
                        } else {
                            bail!("value of option {} is not a string", key);
                        }
                    }
                    "private_key_path" => {
                        if let Some(val) = value.as_str() {
                            env.private_key_path = PathBuf::from(val);
                        } else {
                            bail!("value of option {} is not a string", key);
                        }
                    }
                    "pageserver" => {
                        // We pass any pageserver options as is to the pageserver.toml config
                        // file, but we also extract and parse some options that we need to know
                        // about.
                        if let Some(val) = value.as_table() {
                            env.pageserver = PageServerConf::from_toml(val.clone())?;
                        } else {
                            bail!("value of option {} is not a table", key);
                        }
                    }
                    "safekeepers" => {
                        if let Some(aot) = value.as_array_of_tables() {
                            let mut safekeepers = Vec::new();
                            for tbl in aot.iter() {
                                safekeepers.push(SafekeeperConf::from_toml(tbl)?);
                            }
                            env.safekeepers = safekeepers;
                        } else {
                            bail!("'safekeepers' is not an array of tables");
                        }
                    }
                    _ => bail!("unrecognized option \"{}\" in config file", key),
                }
            }

            env.set_base_data_dir(super::base_path());

            Ok(env)
        }

        pub fn toml_string(&self) -> String {
            self.toml.to_string()
        }

        pub fn private_key_path(&self) -> &Path {
            &self.private_key_path
        }

        pub fn set_private_key_path(&mut self, private_key_path: PathBuf) {
            self.toml.insert(
                "private_key_path",
                toml_edit::value(private_key_path.display().to_string()),
            );
            self.private_key_path = private_key_path;
        }
    }

    #[derive(Clone, Debug)]
    pub struct PageServerConf {
        // Pageserver connection settings
        listen_pg_addr: String,
        listen_http_addr: String,

        // used to determine which auth type is used
        auth_type: AuthType,

        // jwt auth token used for communication with pageserver
        auth_token: String,

        toml: Table,
    }

    impl PageServerConf {
        fn from_toml(mut toml: Table) -> anyhow::Result<Self> {
            set_disclaimer_prefix(&mut toml);
            let mut result = PageServerConf {
                toml,
                listen_pg_addr: String::new(),
                listen_http_addr: String::new(),
                auth_type: AuthType::Trust,
                auth_token: String::new(),
            };

            for (key, value) in result.toml.iter() {
                match key {
                    "listen_pg_addr" => result.listen_pg_addr = parse_string(key, value)?,
                    "listen_http_addr" => result.listen_http_addr = parse_string(key, value)?,
                    "auth_type" => {
                        result.auth_type = AuthType::from_str(&parse_string(key, value)?)?
                    }
                    "auth_token" => result.auth_token = parse_string(key, value)?,
                    _ => {}
                }
            }

            Ok(result)
        }

        pub fn toml_string(&self) -> String {
            self.toml.to_string()
        }

        pub fn auth_type(&self) -> AuthType {
            self.auth_type
        }

        pub fn auth_token(&self) -> &str {
            &self.auth_token
        }

        pub fn set_auth_token(&mut self, auth_token: String) {
            self.toml
                .insert("auth_token", toml_edit::value(&auth_token));
            self.auth_token = auth_token;
        }

        pub fn listen_pg_addr(&self) -> &str {
            &self.listen_pg_addr
        }

        pub fn listen_http_addr(&self) -> &str {
            &self.listen_http_addr
        }
    }

    fn parse_string(name: &str, val: &Item) -> anyhow::Result<String> {
        if let Item::Value(Value::String(val)) = val {
            Ok(val.value().to_string())
        } else {
            bail!("option {} is not a string", name);
        }
    }

    fn parse_port(name: &str, val: &Item) -> anyhow::Result<u16> {
        if let Item::Value(Value::Integer(val)) = val {
            let port = u16::try_from(*val.value()).context(format!(
                "value {} for option {} is not a valid port number",
                val, name
            ))?;
            ensure!(
                port > 0,
                "value {} for option {} is out of range",
                val,
                name
            );
            Ok(port)
        } else {
            bail!("option {} is not an integer", name);
        }
    }

    fn parse_bool(name: &str, val: &Item) -> anyhow::Result<bool> {
        if let Item::Value(Value::Boolean(val)) = val {
            Ok(*val.value())
        } else {
            bail!("option {} is not a boolean", name);
        }
    }

    #[derive(Clone, Debug)]
    pub struct SafekeeperConf {
        pub name: String,
        pub pg_port: u16,
        pub http_port: u16,
        pub sync: bool,
    }

    impl SafekeeperConf {
        fn from_toml(toml: &Table) -> anyhow::Result<Self> {
            let mut result = SafekeeperConf {
                name: String::new(),
                pg_port: 0,
                http_port: 0,
                sync: true,
            };

            for (key, value) in toml.iter() {
                match key {
                    "name" => result.name = parse_string(key, value)?,
                    "pg_port" => result.pg_port = parse_port(key, value)?,
                    "http_port" => result.http_port = parse_port(key, value)?,
                    "sync" => result.sync = parse_bool(key, value)?,
                    _ => {}
                }
            }
            Ok(result)
        }
    }

    // set the prefix for the first element, due to Table::to_string not printing its own decorations
    // if there's no element to prefix, a dummy one is created and inserted into the table.
    fn set_disclaimer_prefix(table: &mut Table) {
        const DISCLAIMER_PREFIX: &str = r#"
# NOTE: This configuration file was created by the 'zenith start' command
# and will be OVERWRITTEN when you run 'zenith start' again. When using
# the 'zenith' CLI to start the page server, set the configuration in
# the parent 'config' file.

"#;

        if table.is_empty() {
            table.insert("_disclaimer", Item::None);
        }

        let (mut first_key, _) = table
            .iter_mut()
            .next()
            .expect("Should have at least one key in the table");

        first_key.decor_mut().set_prefix(DISCLAIMER_PREFIX);
    }
}
