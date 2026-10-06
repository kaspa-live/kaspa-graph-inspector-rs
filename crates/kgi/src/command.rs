//! Typed command resolution at the process boundary.

#![allow(dead_code, reason = "dispatch is added with process composition")]

use std::{ffi::OsString, path::Path};

use clap::Parser;
use kgi_core::config::{DatabaseUrl, KgiConfig, LoggingConfig, NetworkConfig, NodeConfig};
use thiserror::Error;

use crate::{
    cli::{Cli, Command, DatabaseSubcommand},
    config::{
        ConfigError, ConfigLayer, DatabaseLayer, Environment, HttpLayer, LoggingLayer, NetworkLayer, NodeLayer, ResolveRequest,
        WebLayer, resolve,
    },
};

/// Fully validated command selected for this process invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Invocation {
    /// Start the ordinary long-lived KGI service graph.
    Service(ServiceInvocation),
    /// Execute one administrative database reinitialization and exit.
    DatabaseReinitialize(DatabaseReinitializeInvocation),
}

/// Validated ordinary service invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServiceInvocation {
    /// Complete resolved configuration awaiting distribution during composition.
    pub(crate) config: KgiConfig,
    /// Whether startup requests an initial processing-data rebuild.
    pub(crate) clear_database: bool,
}

/// Validated one-shot database reinitialization invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DatabaseReinitializeInvocation {
    /// Exact network and optional consensus override required for node validation.
    pub(crate) network: NetworkConfig,
    /// Node RPC endpoint required to validate network identity and obtain Genesis.
    pub(crate) node: NodeConfig,
    /// Protected database connection URL.
    pub(crate) database_url: DatabaseUrl,
    /// Logger values used by the one-shot command.
    pub(crate) logging: LoggingConfig,
    /// Whether the operator explicitly bypassed interactive confirmation.
    pub(crate) assume_yes: bool,
}

/// Parses an injected argument sequence before loading or validating configuration.
pub(crate) fn parse_and_resolve_from<I, T>(
    arguments: I,
    environment: &Environment,
    executable_path: &Path,
) -> Result<Invocation, EntryError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::try_parse_from(arguments)?;
    resolve_cli(cli, environment, executable_path).map_err(EntryError::from)
}

fn resolve_cli(cli: Cli, environment: &Environment, executable_path: &Path) -> Result<Invocation, CommandError> {
    let is_administrative = cli.command.is_some();
    let administrative_token = match &cli.command {
        Some(Command::Database(database)) => match &database.command {
            DatabaseSubcommand::Reinitialize(options) => options.reinitialize_db_token.is_some(),
        },
        None => false,
    };
    if is_administrative
        && let Some(option) = cli.service.first_present_option().or(administrative_token.then_some("--reinitialize-db-token"))
    {
        return Err(CommandError::ServiceOptionWithAdministrativeCommand { option });
    }

    let clear_database = cli.service.clear_db;
    let config_file = cli.common.config_file.clone();
    let layer = cli_layer(&cli)?;
    let config = resolve(ResolveRequest { cli_config_file: config_file.as_deref(), cli: &layer, environment, executable_path })?;

    match cli.command {
        None => {
            if clear_database && (config.database.initialize || config.database.reinitialization_token.is_some()) {
                return Err(CommandError::ConflictingServiceDatabaseActions);
            }
            Ok(Invocation::Service(ServiceInvocation { config, clear_database }))
        }
        Some(Command::Database(database)) => match database.command {
            DatabaseSubcommand::Reinitialize(options) => {
                let KgiConfig { network, node, database, http: _, logging, web: _ } = config;
                Ok(Invocation::DatabaseReinitialize(DatabaseReinitializeInvocation {
                    network,
                    node,
                    database_url: database.url,
                    logging,
                    assume_yes: options.yes,
                }))
            }
        },
    }
}

fn cli_layer(cli: &Cli) -> Result<ConfigLayer, CommandError> {
    Ok(ConfigLayer {
        network: NetworkLayer {
            mainnet: cli.common.mainnet.then_some(true),
            testnet: cli.common.testnet.then_some(true),
            devnet: cli.common.devnet.then_some(true),
            simnet: cli.common.simnet.then_some(true),
            netsuffix: cli.common.netsuffix,
            override_params_file: cli.common.override_params_file.clone(),
        },
        node: NodeLayer { rpc_url: cli.common.node_rpc_url.clone() },
        database: DatabaseLayer {
            url: optional_secret_string("database.url", cli.common.database_url.as_ref())?,
            initialize: cli.service.initialize_db.then_some(true),
            reinitialization_token: optional_secret_string(
                "database.reinitialization-token",
                cli.service.reinitialize_db_token.as_ref(),
            )?,
        },
        http: HttpLayer { listen: cli.service.http_listen.clone() },
        logging: LoggingLayer {
            level: cli.common.log_level.clone(),
            directory: cli.common.log_dir.clone(),
            no_files: cli.common.no_log_files.then_some(true),
        },
        web: WebLayer {
            root: cli.service.web_root.clone(),
            block_explorer_url_template: cli.service.block_explorer_url_template.clone(),
        },
    })
}

fn optional_secret_string(field: &'static str, value: Option<&OsString>) -> Result<Option<String>, CommandError> {
    value.map(|value| value.clone().into_string().map_err(|_| CommandError::InvalidCommandLineUtf8 { field })).transpose()
}

/// Failure while parsing and resolving process entry.
#[derive(Debug, Error)]
pub(crate) enum EntryError {
    /// Clap produced help, version, or a usage error before configuration access.
    #[error(transparent)]
    Cli(#[from] clap::Error),
    /// Parsed arguments could not produce a valid invocation.
    #[error(transparent)]
    Command(#[from] CommandError),
}

/// Failure while converting parsed command-line values into an invocation.
#[derive(Debug, Error)]
pub(crate) enum CommandError {
    /// A protected command-line value was not UTF-8.
    #[error("{field} from the command line is not valid UTF-8")]
    InvalidCommandLineUtf8 { field: &'static str },
    /// A service-only option accompanied an administrative command.
    #[error("{option} is available only for ordinary service startup")]
    ServiceOptionWithAdministrativeCommand { option: &'static str },
    /// Clear intent was combined with another service database action.
    #[error("database initialization, clear, and reinitialization-token actions are mutually exclusive")]
    ConflictingServiceDatabaseActions,
    /// Configuration resolution failed.
    #[error(transparent)]
    Config(#[from] ConfigError),
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, path::Path};

    use clap::error::ErrorKind;
    use kaspa_consensus_core::network::{NetworkId, NetworkType};

    use super::{CommandError, EntryError, Invocation, parse_and_resolve_from};
    use crate::config::Environment;

    const EXECUTABLE: &str = "/opt/kgi/bin/kgi";

    fn parse(arguments: &[&str], environment: &Environment) -> Result<Invocation, EntryError> {
        parse_and_resolve_from(arguments, environment, Path::new(EXECUTABLE))
    }

    fn environment(values: &[(&'static str, &str)]) -> Environment {
        Environment::from_values(values.iter().map(|(name, value)| (*name, OsString::from(value))))
    }

    #[test]
    fn service_is_the_default_command() {
        let invocation = parse(&["kgi", "--database-url", "postgresql://localhost/kgi"], &Environment::default())
            .expect("service command must resolve");

        let Invocation::Service(service) = invocation else {
            panic!("expected service invocation");
        };
        assert!(!service.clear_database);
        assert_eq!(service.config.network.network_id, NetworkId::new(NetworkType::Mainnet));
        assert_eq!(service.config.database.url.expose_url().as_str(), "postgresql://localhost/kgi");
    }

    #[test]
    fn service_cli_surface_populates_the_highest_precedence_layer() {
        let invocation = parse(
            &[
                "kgi",
                "--testnet",
                "--netsuffix=12",
                "--override-params-file=/etc/kgi/params.json",
                "-s",
                "grpc://node.example:16210",
                "--database-url",
                "postgresql://localhost/kgi",
                "--http-listen",
                "0.0.0.0:9080",
                "-d=info,kgi_processing=debug",
                "--no-log-files",
                "--web-root",
                "/srv/kgi/web",
                "--block-explorer-url-template",
                "https://explorer.example/{hash}",
                "--clear-db",
            ],
            &Environment::default(),
        )
        .expect("complete service CLI must resolve");

        let Invocation::Service(service) = invocation else {
            panic!("expected service invocation");
        };
        assert_eq!(service.config.network.network_id, NetworkId::with_suffix(NetworkType::Testnet, 12));
        assert_eq!(service.config.node.rpc_url.as_str(), "grpc://node.example:16210");
        assert_eq!(service.config.http.listen.to_string(), "0.0.0.0:9080");
        assert_eq!(service.config.logging.level, "info,kgi_processing=debug");
        assert!(service.config.logging.directory.is_none());
        assert_eq!(service.config.web.root, Path::new("/srv/kgi/web"));
        assert!(service.clear_database);
    }

    #[test]
    fn administrative_command_accepts_global_inputs_after_subcommands() {
        let invocation = parse(
            &[
                "kgi",
                "database",
                "reinitialize",
                "--testnet",
                "--database-url",
                "postgresql://localhost/kgi",
                "--node-rpc-url",
                "grpc://node.example:16210",
                "--log-level=warn",
                "--yes",
            ],
            &Environment::default(),
        )
        .expect("administrative command must resolve");

        let Invocation::DatabaseReinitialize(command) = invocation else {
            panic!("expected database reinitialize invocation");
        };
        assert_eq!(command.network.network_id, NetworkId::with_suffix(NetworkType::Testnet, 10));
        assert_eq!(command.node.rpc_url.as_str(), "grpc://node.example:16210");
        assert_eq!(command.database_url.expose_url().as_str(), "postgresql://localhost/kgi");
        assert_eq!(command.logging.level, "warn");
        assert!(command.assume_yes);
    }

    #[test]
    fn help_and_version_finish_before_configuration_resolution() {
        for (argument, expected_kind) in [("--help", ErrorKind::DisplayHelp), ("--version", ErrorKind::DisplayVersion)] {
            let error = parse(&["kgi", argument], &Environment::default()).expect_err("display request must short-circuit");
            assert!(matches!(error, EntryError::Cli(ref error) if error.kind() == expected_kind));
        }

        let error = parse(&["kgi", "database", "reinitialize", "--help"], &Environment::default())
            .expect_err("administrative help must short-circuit");
        assert!(matches!(error, EntryError::Cli(ref error) if error.kind() == ErrorKind::DisplayHelp));
    }

    #[test]
    fn service_has_no_run_or_serve_subcommand() {
        for command in ["run", "serve"] {
            let error = parse(&["kgi", command], &Environment::default()).expect_err("unsupported service subcommand must fail");
            assert!(matches!(error, EntryError::Cli(ref error) if error.kind() == ErrorKind::InvalidSubcommand));
        }
    }

    #[test]
    fn yes_is_available_only_to_database_reinitialize() {
        let error = parse(&["kgi", "--yes"], &Environment::default()).expect_err("service must reject --yes");
        assert!(matches!(error, EntryError::Cli(ref error) if error.kind() == ErrorKind::UnknownArgument));
    }

    #[test]
    fn service_only_options_are_rejected_with_administrative_command() {
        let error = parse(
            &["kgi", "--clear-db", "--database-url", "postgresql://localhost/kgi", "database", "reinitialize"],
            &Environment::default(),
        )
        .expect_err("administrative command must reject service-only options");
        assert!(matches!(error, EntryError::Command(CommandError::ServiceOptionWithAdministrativeCommand { option: "--clear-db" })));
    }

    #[test]
    fn clear_is_mutually_exclusive_with_initialization_and_token() {
        let cases = [
            vec!["kgi", "--database-url", "postgresql://localhost/kgi", "--clear-db", "--initialize-db"],
            vec!["kgi", "--database-url", "postgresql://localhost/kgi", "--clear-db", "--reinitialize-db-token", "rotation-2"],
        ];

        for arguments in cases {
            let error = parse(&arguments, &Environment::default()).expect_err("database actions must be exclusive");
            assert!(matches!(error, EntryError::Command(CommandError::ConflictingServiceDatabaseActions)));
        }

        let error = parse(
            &["kgi", "--database-url", "postgresql://localhost/kgi", "--clear-db"],
            &environment(&[("KGI_REINITIALIZE_DB_TOKEN", "rotation-2")]),
        )
        .expect_err("environment token must conflict with clear intent");
        assert!(matches!(error, EntryError::Command(CommandError::ConflictingServiceDatabaseActions)));
    }

    #[test]
    fn protected_values_are_absent_from_command_diagnostics_and_debug() {
        let database_url = "postgresql://alice:hunter2@localhost/kgi?secret=query";
        let token = "rotation-secret";
        let error = parse(
            &["kgi", "--database-url", database_url, "--initialize-db", "--reinitialize-db-token", token],
            &Environment::default(),
        )
        .expect_err("conflicting protected input must fail");
        let diagnostic = format!("{error} {error:?}");
        for secret in [database_url, "alice", "hunter2", "secret=query", token] {
            assert!(!diagnostic.contains(secret));
        }

        let error = parse(
            &[
                "kgi",
                "database",
                "reinitialize",
                "--database-url",
                "postgresql://localhost/kgi",
                "--reinitialize-db-token=administrative-secret",
            ],
            &Environment::default(),
        )
        .expect_err("administrative command must safely reject the service token");
        let diagnostic = format!("{error} {error:?}");
        assert!(!diagnostic.contains("administrative-secret"));
    }
}
