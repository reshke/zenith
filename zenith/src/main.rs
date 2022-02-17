use anyhow::{bail, Context, Result};
use clap::{App, AppSettings, Arg, ArgMatches};
use control_plane::compute::ComputeControlPlane;
use control_plane::local_env;
use control_plane::local_env::LocalEnv;
use control_plane::safekeeper::SafekeeperNode;
use control_plane::storage::PageServerNode;
use pageserver::config::defaults::{
    DEFAULT_HTTP_LISTEN_ADDR as DEFAULT_PAGESERVER_HTTP_ADDR,
    DEFAULT_PG_LISTEN_ADDR as DEFAULT_PAGESERVER_PG_ADDR,
};
use std::collections::{BTreeSet, HashMap};
use std::process::exit;
use std::str::FromStr;
use walkeeper::defaults::{
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_SAFEKEEPER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_SAFEKEEPER_PG_PORT,
};
use zenith_utils::auth::{Claims, Scope};
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend::AuthType;
use zenith_utils::zid::{ZTenantId, ZTimelineId};
use zenith_utils::GIT_VERSION;

use pageserver::timelines::TimelineInfo;

// Default name of a safekeeper node, if not specified on the command line.
const DEFAULT_SAFEKEEPER_NAME: &str = "single";

fn default_conf() -> String {
    format!(
        r#"
# Default built-in configuration, defined in main.rs
[pageserver]
listen_pg_addr = '{pageserver_pg_addr}'
listen_http_addr = '{pageserver_http_addr}'
auth_type = '{pageserver_auth_type}'

[[safekeepers]]
name = '{safekeeper_name}'
pg_port = {safekeeper_pg_port}
http_port = {safekeeper_http_port}
"#,
        pageserver_pg_addr = DEFAULT_PAGESERVER_PG_ADDR,
        pageserver_http_addr = DEFAULT_PAGESERVER_HTTP_ADDR,
        pageserver_auth_type = AuthType::Trust,
        safekeeper_name = DEFAULT_SAFEKEEPER_NAME,
        safekeeper_pg_port = DEFAULT_SAFEKEEPER_PG_PORT,
        safekeeper_http_port = DEFAULT_SAFEKEEPER_HTTP_PORT,
    )
}

///
/// Timelines tree element used as a value in the HashMap.
///
struct TimelineTreeEl {
    /// `TimelineInfo` received from the `pageserver` via the `timeline_list` libpq API call.
    pub info: TimelineInfo,
    /// Holds all direct children of this timeline referenced using `timeline_id`.
    pub children: BTreeSet<ZTimelineId>,
}

// Main entry point for the 'zenith' CLI utility
//
// This utility helps to manage zenith installation. That includes following:
//   * Management of local postgres installations running on top of the
//     pageserver.
//   * Providing CLI api to the pageserver
//   * TODO: export/import to/from usual postgres
fn main() -> Result<()> {
    let pg_node_arg = Arg::new("node").index(1).help("Node name").required(true);

    let safekeeper_node_arg = Arg::new("node").index(1).help("Node name").required(false);

    let timeline_id_arg = Arg::new("timeline")
        .help("Timeline id. Represented as a hexadecimal string 32 symbols length")
        .required(false);

    let tenantid_arg = Arg::new("tenantid")
        .long("tenantid")
        .help("Tenant id. Represented as a hexadecimal string 32 symbols length")
        .takes_value(true)
        .required(false);

    let port_arg = Arg::new("port")
        .long("port")
        .required(false)
        .value_name("port");

    let stop_mode_arg = Arg::new("stop-mode")
        .short('m')
        .takes_value(true)
        .possible_values(&["fast", "immediate"])
        .help("If 'immediate', don't flush repository data at shutdown")
        .required(false)
        .value_name("stop-mode");

    let pageserver_config_args = Arg::new("pageserver-config-override")
        .long("pageserver-config-override")
        .takes_value(true)
        .number_of_values(1)
        .multiple_occurrences(true)
        .help("Additional pageserver's configuration options or overrides, refer to pageserver's 'config-override' CLI parameter docs for more")
        .required(false);

    let lsn_arg = Arg::new("lsn")
        .long("lsn")
        .help("Specify Lsn on the timeline to start from. By default, end of the timeline would be used.")
        .takes_value(true)
        .required(false);

    let matches = App::new("Zenith CLI")
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(GIT_VERSION)
        .subcommand(
            App::new("init")
                .about("Initialize a new Zenith repository")
                .arg(pageserver_config_args.clone())
                .arg(
                    Arg::new("config")
                        .long("config")
                        .required(false)
                        .value_name("config"),
                )
        )
        .subcommand(
            App::new("timeline")
            .about("Manage timelines")
            .subcommand(App::new("list").arg(tenantid_arg.clone()))
            .subcommand(App::new("create")
                .arg(tenantid_arg.clone())
                .arg(timeline_id_arg.clone().help("Id of the new timeline, optional. If not specified, it will be generated randomly"))
                .arg(Arg::new("ancestor-timeline-id").long("ancestor-timeline-id").takes_value(true)
                    .help("Use last Lsn of another timeline (and its data) as base when creating the new timeline").required(false))
                .arg(Arg::new("ancestor-start-lsn").long("ancestor-start-lsn").takes_value(true)
                    .help("When using another timeline as base, use a specific Lsn in it instead of the latest one").required(false))
                )
        ).subcommand(
            App::new("tenant")
            .setting(AppSettings::ArgRequiredElseHelp)
            .about("Manage tenants")
            .subcommand(App::new("list"))
            .subcommand(App::new("create").arg(tenantid_arg.clone().index(1)))
        )
        .subcommand(
            App::new("pageserver")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("Manage pageserver")
                .subcommand(App::new("status"))
                .subcommand(App::new("start").about("Start local pageserver").arg(pageserver_config_args.clone()))
                .subcommand(App::new("stop").about("Stop local pageserver")
                            .arg(stop_mode_arg.clone()))
                .subcommand(App::new("restart").about("Restart local pageserver").arg(pageserver_config_args.clone()))
        )
        .subcommand(
            App::new("safekeeper")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("Manage safekeepers")
                .subcommand(App::new("start")
                            .about("Start local safekeeper")
                            .arg(safekeeper_node_arg.clone())
                )
                .subcommand(App::new("stop")
                            .about("Stop local safekeeper")
                            .arg(safekeeper_node_arg.clone())
                            .arg(stop_mode_arg.clone())
                )
                .subcommand(App::new("restart")
                            .about("Restart local safekeeper")
                            .arg(safekeeper_node_arg.clone())
                            .arg(stop_mode_arg.clone())
                )
        )
        .subcommand(
            App::new("pg")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("Manage postgres instances")
                .subcommand(App::new("list").arg(tenantid_arg.clone()))
                .subcommand(App::new("create")
                    .about("Create a postgres compute node")
                    .arg(pg_node_arg.clone().index(1))
                    .arg(timeline_id_arg.clone().index(2))
                    .arg(tenantid_arg.clone())
                    .arg(lsn_arg.clone())
                    .arg(port_arg.clone())
                    .arg(
                        Arg::new("config-only")
                            .help("Don't do basebackup, create compute node with only config files")
                            .long("config-only")
                            .required(false)
                    ))
                .subcommand(App::new("start")
                    .about("Start a postgres compute node.\n This command actually creates new node from scratch, but preserves existing config files")
                    .arg(pg_node_arg.clone().index(1))
                    .arg(timeline_id_arg.clone().index(2))
                    .arg(tenantid_arg.clone())
                    .arg(lsn_arg.clone())
                    .arg(port_arg.clone()))
                .subcommand(
                    App::new("stop")
                        .arg(pg_node_arg.clone())
                        .arg(tenantid_arg.clone())
                        .arg(
                            Arg::new("destroy")
                                .help("Also delete data directory (now optional, should be default in future)")
                                .long("destroy")
                                .required(false)
                        )
                    )

        )
        .subcommand(
            App::new("start")
                .about("Start page server and safekeepers")
                .arg(pageserver_config_args)
        )
        .subcommand(
            App::new("stop")
                .about("Stop page server and safekeepers")
                .arg(stop_mode_arg.clone())
        )
        .get_matches();

    let (sub_name, sub_args) = match matches.subcommand() {
        Some(subcommand_data) => subcommand_data,
        None => bail!("no subcommand provided"),
    };

    // Check for 'zenith init' command first.
    let subcmd_result = if sub_name == "init" {
        handle_init(sub_args)
    } else {
        // all other commands need an existing config
        let mut env = match LocalEnv::load_config() {
            Ok(conf) => conf,
            Err(e) => {
                eprintln!("Error loading config: {}", e);
                exit(1);
            }
        };

        match sub_name {
            "tenant" => handle_tenant(sub_args, &mut env),
            "timeline" => handle_timeline(sub_args, &env),
            "start" => handle_start_all(sub_args, &env),
            "stop" => handle_stop_all(sub_args, &env),
            "pageserver" => handle_pageserver(sub_args, &env),
            "pg" => handle_pg(sub_args, &env),
            "safekeeper" => handle_safekeeper(sub_args, &env),
            _ => bail!("unexpected subcommand {}", sub_name),
        }
    };
    if let Err(e) = subcmd_result {
        eprintln!("command failed: {:#}", e);
        exit(1);
    }

    Ok(())
}

///
/// Prints timelines list as a tree-like structure.
///
fn print_timelines_tree(timelines: Vec<TimelineInfo>) -> Result<()> {
    let mut timelines_hash = timelines
        .iter()
        .map(|t| {
            (
                t.timeline_id(),
                TimelineTreeEl {
                    info: t.clone(),
                    children: BTreeSet::new(),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    // Memorize all direct children of each timeline.
    for timeline in &timelines {
        if let TimelineInfo::Local {
            ancestor_timeline_id: Some(tid),
            ..
        } = timeline
        {
            timelines_hash
                .get_mut(tid)
                .context("missing timeline info in the HashMap")?
                .children
                .insert(timeline.timeline_id());
        }
    }

    for timeline in timelines_hash.values() {
        // Start with root local timelines (no ancestors) first.
        if let TimelineInfo::Local {
            ancestor_timeline_id,
            ..
        } = &timeline.info
        {
            if ancestor_timeline_id.is_none() {
                print_timeline(0, &Vec::from([true]), timeline, &timelines_hash)?;
            }
        }
    }

    Ok(())
}

///
/// Recursively prints timeline info with all its children.
///
fn print_timeline(
    nesting_level: usize,
    is_last: &[bool],
    timeline: &TimelineTreeEl,
    timelines: &HashMap<ZTimelineId, TimelineTreeEl>,
) -> Result<()> {
    let local_or_remote = match timeline.info {
        TimelineInfo::Local { .. } => "(L)",
        TimelineInfo::Remote { .. } => "(R)",
    };
    // Draw main padding
    print!("{} ", local_or_remote);

    if nesting_level > 0 {
        let lsn_string = match timeline.info {
            TimelineInfo::Local { ancestor_lsn, .. } => ancestor_lsn
                .map(|lsn| lsn.to_string())
                .unwrap_or_else(|| "Unknown local Lsn".to_string()),
            TimelineInfo::Remote { .. } => "unknown Lsn (remote)".to_string(),
        };
        let mut br_sym = "┣━";

        // Draw each nesting padding with proper style
        // depending on whether its timeline ended or not.
        if nesting_level > 1 {
            for l in &is_last[1..is_last.len() - 1] {
                if *l {
                    print!("   ");
                } else {
                    print!("┃  ");
                }
            }
        }

        // We are the last in this sub-timeline
        if *is_last.last().unwrap() {
            br_sym = "┗━";
        }

        print!("{} @{}: ", br_sym, lsn_string);
    }

    // Finally print a timeline name with new line
    println!("{}", timeline.info.timeline_id());

    let len = timeline.children.len();
    let mut i: usize = 0;
    let mut is_last_new = Vec::from(is_last);
    is_last_new.push(false);

    for child in &timeline.children {
        i += 1;

        // Mark that the last padding is the end of the timeline
        if i == len {
            if let Some(last) = is_last_new.last_mut() {
                *last = true;
            }
        }

        print_timeline(
            nesting_level + 1,
            &is_last_new,
            timelines
                .get(child)
                .context("missing timeline info in the HashMap")?,
            timelines,
        )?;
    }

    Ok(())
}

/// Returns a map of timeline IDs to timeline_id@lsn strings.
/// Connects to the pageserver to query this information.
fn get_timeline_infos(
    env: &local_env::LocalEnv,
    tenantid: &ZTenantId,
) -> Result<HashMap<ZTimelineId, TimelineInfo>> {
    let page_server = PageServerNode::from_env(env);
    let timeline_infos: Vec<TimelineInfo> = page_server.timeline_list(tenantid)?;
    let timeline_infos: HashMap<ZTimelineId, TimelineInfo> = timeline_infos
        .into_iter()
        .map(|timeline_info| (timeline_info.timeline_id(), timeline_info))
        .collect();

    Ok(timeline_infos)
}

// Helper function to parse --tenantid option, or get the default from config file
fn get_tenant_id(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> anyhow::Result<ZTenantId> {
    if let Some(tenantid_cmd) = sub_match.value_of("tenantid") {
        Ok(
            ZTenantId::from_str(tenantid_cmd)
                .context("Failed to parse tenant id from arguments")?,
        )
    } else if let Some(tenantid_conf) = env.default_tenantid {
        Ok(tenantid_conf)
    } else {
        bail!("No tenantid. Use --tenantid, or set 'default_tenantid' in the config file");
    }
}

fn get_timeline_id(
    sub_match: &ArgMatches,
    tenant_id: ZTenantId,
    env: &local_env::LocalEnv,
) -> anyhow::Result<ZTimelineId> {
    if let Some(timeline_id) = sub_match.value_of("timeline") {
        Ok(ZTimelineId::from_str(timeline_id)
            .context("Failed to parse timeline id from arguments")?)
    } else if let Some(&initial_timeline_id) = env.initial_timelines.get(&tenant_id) {
        Ok(initial_timeline_id)
    } else {
        bail!("No timeline id, specify one in the subcommand's arguments");
    }
}

fn handle_init(init_match: &ArgMatches) -> Result<()> {
    // Create config file
    let toml_file: String = if let Some(config_path) = init_match.value_of("config") {
        // load and parse the file
        std::fs::read_to_string(std::path::Path::new(config_path))
            .with_context(|| format!("Could not read configuration file \"{}\"", config_path))?
    } else {
        // Built-in default config
        default_conf()
    };

    let mut env =
        LocalEnv::create_config(&toml_file).context("Failed to create zenith configuration")?;
    env.init()
        .context("Failed to initialize zenith repository")?;

    // Call 'pageserver init'.
    let pageserver = PageServerNode::from_env(&env);
    if let Err(e) = pageserver.init(
        // default_tenantid was generated by the `env.init()` call above
        Some(&env.default_tenantid.unwrap().to_string()),
        &pageserver_config_overrides(init_match),
    ) {
        eprintln!("pageserver init failed: {}", e);
        exit(1);
    }

    Ok(())
}

fn pageserver_config_overrides(init_match: &ArgMatches) -> Vec<&str> {
    init_match
        .values_of("pageserver-config-override")
        .into_iter()
        .flatten()
        .collect()
}

fn handle_tenant(tenant_match: &ArgMatches, env: &mut local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(env);
    match tenant_match.subcommand() {
        Some(("list", _)) => {
            for t in pageserver.tenant_list()? {
                println!("{} {}", t.id, t.state);
            }
        }
        Some(("create", create_match)) => {
            let tenant_id = match create_match.value_of("tenantid") {
                Some(id) => ZTenantId::from_str(id)?,
                None => ZTenantId::generate(),
            };
            println!("using tenant id {}", tenant_id);
            let initial_timeline_id = pageserver.tenant_create(tenant_id)?;
            env.initial_timelines.insert(tenant_id, initial_timeline_id);
            println!("tenant successfully created on the pageserver");
        }
        Some((sub_name, _)) => bail!("Unexpected tenant subcommand '{}'", sub_name),
        None => bail!("no tenant subcommand provided"),
    }
    Ok(())
}

fn handle_timeline(timeline_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(env);

    match timeline_match.subcommand() {
        Some(("list", list_match)) => {
            let tenant_id = get_tenant_id(list_match, env)?;
            let timelines = pageserver.timeline_list(&tenant_id)?;
            print_timelines_tree(timelines)?;
        }
        Some(("create", create_match)) => {
            let tenant_id = get_tenant_id(create_match, env)?;
            let timeline_id = get_timeline_id(create_match, tenant_id, env)
                .unwrap_or_else(|_| ZTimelineId::generate());
            let ancestor_timeline_id = create_match
                .value_of("ancestor-timeline-id")
                .map(ZTimelineId::from_str)
                .transpose()
                .context("Failed to parse ancestor timeline id from the request")?;
            let start_lsn = create_match
                .value_of("ancestor-start-lsn")
                .map(Lsn::from_str)
                .transpose()
                .context("Failed to parse ancestor start Lsn from the request")?;
            let timeline = pageserver.timeline_create(
                tenant_id,
                timeline_id,
                start_lsn,
                ancestor_timeline_id,
            )?;

            let last_record_lsn = match timeline {
                TimelineInfo::Local {
                    last_record_lsn, ..
                } => last_record_lsn,
                TimelineInfo::Remote { .. } => {
                    bail!("Timeline {} was created as remote, not local", timeline_id)
                }
            };
            let base_message = format!(
                "Created timeline '{}' at Lsn {} for tenant: {}",
                timeline.timeline_id(),
                last_record_lsn,
                tenant_id,
            );
            if let Some(ancestor_timeline_id) = ancestor_timeline_id {
                println!(
                    "{} Ancestor timeline: '{}'",
                    base_message, ancestor_timeline_id
                );
            } else {
                println!("{} No ancestor timeline", base_message);
            }
        }
        Some((sub_name, _)) => bail!("Unexpected tenant subcommand '{}'", sub_name),
        None => bail!("no tenant subcommand provided"),
    }

    Ok(())
}

fn handle_pg(pg_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let (sub_name, sub_args) = match pg_match.subcommand() {
        Some(pg_subcommand_data) => pg_subcommand_data,
        None => bail!("no pg subcommand provided"),
    };

    let mut cplane = ComputeControlPlane::load(env.clone())?;

    // All subcommands take an optional --tenantid option
    let tenant_id = get_tenant_id(sub_args, env)?;

    match sub_name {
        "list" => {
            let timeline_infos = get_timeline_infos(env, &tenant_id).unwrap_or_else(|e| {
                eprintln!("Failed to load timeline info: {}", e);
                HashMap::new()
            });

            println!("NODE\tADDRESS\t\tTIMELINE\tLSN\t\tSTATUS");
            for ((_, node_name), node) in cplane
                .nodes
                .iter()
                .filter(|((node_tenantid, _), _)| node_tenantid == &tenant_id)
            {
                // FIXME: This shows the LSN at the end of the timeline. It's not the
                // right thing to do for read-only nodes that might be anchored at an
                // older point in time, or following but lagging behind the primary.
                let lsn_str = timeline_infos
                    .get(&node.timeline_id)
                    .map(|bi| match bi {
                        TimelineInfo::Local {
                            last_record_lsn, ..
                        } => last_record_lsn.to_string(),
                        TimelineInfo::Remote { .. } => "? (remote)".to_string(),
                    })
                    .unwrap_or_else(|| '?'.to_string());

                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    node_name,
                    node.address,
                    node.timeline_id,
                    lsn_str,
                    node.status(),
                );
            }
        }
        "create" => {
            let node_name = sub_args.value_of("node").unwrap_or("main");
            let lsn = sub_args
                .value_of("lsn")
                .map(Lsn::from_str)
                .transpose()
                .context("Failed to parse Lsn from the request")?;
            let timeline_id = get_timeline_id(sub_args, tenant_id, env)?;

            let port: Option<u16> = match sub_args.value_of("port") {
                Some(p) => Some(p.parse()?),
                None => None,
            };
            cplane.new_node(tenant_id, node_name, timeline_id, lsn, port)?;
        }
        "start" => {
            let node_name = sub_args.value_of("node").unwrap_or("main");

            let port: Option<u16> = match sub_args.value_of("port") {
                Some(p) => Some(p.parse()?),
                None => None,
            };

            let node = cplane.nodes.get(&(tenant_id, node_name.to_owned()));

            let auth_token = if matches!(env.pageserver.auth_type, AuthType::ZenithJWT) {
                let claims = Claims::new(Some(tenant_id), Scope::Tenant);

                Some(env.generate_auth_token(&claims)?)
            } else {
                None
            };

            if let Some(node) = node {
                println!("Starting existing postgres {}...", node_name);
                node.start(&auth_token)?;
            } else {
                let timeline_id = get_timeline_id(sub_args, tenant_id, env)?;
                let lsn = sub_args
                    .value_of("lsn")
                    .map(Lsn::from_str)
                    .transpose()
                    .context("Failed to parse Lsn from the request")?;
                // when used with custom port this results in non obvious behaviour
                // port is remembered from first start command, i e
                // start --port X
                // stop
                // start <-- will also use port X even without explicit port argument
                println!(
                    "Starting new postgres {} on timeline {} ...",
                    node_name, timeline_id
                );
                let node = cplane.new_node(tenant_id, node_name, timeline_id, lsn, port)?;
                node.start(&auth_token)?;
            }
        }
        "stop" => {
            let node_name = sub_args.value_of("node").unwrap_or("main");
            let destroy = sub_args.is_present("destroy");

            let node = cplane
                .nodes
                .get(&(tenant_id, node_name.to_owned()))
                .with_context(|| format!("postgres {} is not found", node_name))?;
            node.stop(destroy)?;
        }

        _ => bail!("Unexpected pg subcommand '{}'", sub_name),
    }

    Ok(())
}

fn handle_pageserver(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(env);

    match sub_match.subcommand() {
        Some(("start", start_match)) => {
            if let Err(e) = pageserver.start(&pageserver_config_overrides(start_match)) {
                eprintln!("pageserver start failed: {}", e);
                exit(1);
            }
        }

        Some(("stop", stop_match)) => {
            let immediate = stop_match.value_of("stop-mode") == Some("immediate");

            if let Err(e) = pageserver.stop(immediate) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }
        }

        Some(("restart", restart_match)) => {
            //TODO what shutdown strategy should we use here?
            if let Err(e) = pageserver.stop(false) {
                eprintln!("pageserver stop failed: {}", e);
                exit(1);
            }

            if let Err(e) = pageserver.start(&pageserver_config_overrides(restart_match)) {
                eprintln!("pageserver start failed: {}", e);
                exit(1);
            }
        }
        Some((sub_name, _)) => bail!("Unexpected pageserver subcommand '{}'", sub_name),
        None => bail!("no pageserver subcommand provided"),
    }
    Ok(())
}

fn get_safekeeper(env: &local_env::LocalEnv, name: &str) -> Result<SafekeeperNode> {
    if let Some(node) = env.safekeepers.iter().find(|node| node.name == name) {
        Ok(SafekeeperNode::from_env(env, node))
    } else {
        bail!("could not find safekeeper '{}'", name)
    }
}

fn handle_safekeeper(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let (sub_name, sub_args) = match sub_match.subcommand() {
        Some(safekeeper_command_data) => safekeeper_command_data,
        None => bail!("no safekeeper subcommand provided"),
    };

    // All the commands take an optional safekeeper name argument
    let node_name = sub_args.value_of("node").unwrap_or(DEFAULT_SAFEKEEPER_NAME);
    let safekeeper = get_safekeeper(env, node_name)?;

    match sub_name {
        "start" => {
            if let Err(e) = safekeeper.start() {
                eprintln!("safekeeper start failed: {}", e);
                exit(1);
            }
        }

        "stop" => {
            let immediate = sub_args.value_of("stop-mode") == Some("immediate");

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {}", e);
                exit(1);
            }
        }

        "restart" => {
            let immediate = sub_args.value_of("stop-mode") == Some("immediate");

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {}", e);
                exit(1);
            }

            if let Err(e) = safekeeper.start() {
                eprintln!("safekeeper start failed: {}", e);
                exit(1);
            }
        }

        _ => {
            bail!("Unexpected safekeeper subcommand '{}'", sub_name)
        }
    }
    Ok(())
}

fn handle_start_all(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let pageserver = PageServerNode::from_env(env);

    // Postgres nodes are not started automatically

    if let Err(e) = pageserver.start(&pageserver_config_overrides(sub_match)) {
        eprintln!("pageserver start failed: {}", e);
        exit(1);
    }

    for node in env.safekeepers.iter() {
        let safekeeper = SafekeeperNode::from_env(env, node);
        if let Err(e) = safekeeper.start() {
            eprintln!("safekeeper '{}' start failed: {}", safekeeper.name, e);
            exit(1);
        }
    }
    Ok(())
}

fn handle_stop_all(sub_match: &ArgMatches, env: &local_env::LocalEnv) -> Result<()> {
    let immediate = sub_match.value_of("stop-mode") == Some("immediate");

    let pageserver = PageServerNode::from_env(env);

    // Stop all compute nodes
    let cplane = ComputeControlPlane::load(env.clone())?;
    for (_k, node) in cplane.nodes {
        if let Err(e) = node.stop(false) {
            eprintln!("postgres stop failed: {}", e);
        }
    }

    if let Err(e) = pageserver.stop(immediate) {
        eprintln!("pageserver stop failed: {}", e);
    }

    for node in env.safekeepers.iter() {
        let safekeeper = SafekeeperNode::from_env(env, node);
        if let Err(e) = safekeeper.stop(immediate) {
            eprintln!("safekeeper '{}' stop failed: {}", safekeeper.name, e);
        }
    }
    Ok(())
}
