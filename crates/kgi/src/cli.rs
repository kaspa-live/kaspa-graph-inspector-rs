//! Command-line grammar for the KGI process.

use std::{ffi::OsString, path::PathBuf};

use clap::{Args, Parser, Subcommand};

/// Parsed KGI command line.
#[derive(Parser)]
#[command(name = "kgi", version, about = "Kaspa Graph Inspector")]
pub(crate) struct Cli {
    /// Configuration inputs shared by service and administrative commands.
    #[command(flatten)]
    pub(crate) common: CommonOptions,

    /// Inputs accepted only by ordinary service startup.
    #[command(flatten)]
    pub(crate) service: ServiceOptions,

    /// Optional one-shot administrative command.
    #[command(subcommand)]
    pub(crate) command: Option<Command>,
}

/// Configuration inputs shared by service and administrative commands.
#[derive(Args)]
pub(crate) struct CommonOptions {
    /// Path of the TOML configuration file.
    #[arg(short = 'C', long, global = true, value_name = "CONFIG_FILE")]
    pub(crate) config_file: Option<PathBuf>,

    /// Use the main network.
    #[arg(long, global = true)]
    pub(crate) mainnet: bool,

    /// Use the test network.
    #[arg(long, global = true)]
    pub(crate) testnet: bool,

    /// Use the development test network.
    #[arg(long, global = true)]
    pub(crate) devnet: bool,

    /// Use the simulation test network.
    #[arg(long, global = true)]
    pub(crate) simnet: bool,

    /// Testnet network suffix number.
    #[arg(long, global = true, value_name = "netsuffix", require_equals = true)]
    pub(crate) netsuffix: Option<u32>,

    /// Path to a JSON file containing override parameters.
    #[arg(long, global = true, value_name = "OVERRIDE_PARAMS_FILE", require_equals = true)]
    pub(crate) override_params_file: Option<PathBuf>,

    /// Node gRPC server to connect to.
    #[arg(short = 's', long, global = true, value_name = "RPC_URL")]
    pub(crate) node_rpc_url: Option<String>,

    /// PostgreSQL database URL to connect to.
    ///
    /// The expected form is
    /// `postgresql://<username>:<password>@<host>:<port>/<database-name>`.
    #[arg(long, global = true, value_name = "DATABASE_URL")]
    pub(crate) database_url: Option<OsString>,

    /// Logging level for all subsystems {off, error, warn, info, debug, trace}.
    ///
    /// Specify `<subsystem>=<level>,<subsystem2>=<level>,...` to set levels for
    /// individual subsystems.
    #[arg(short = 'd', long, global = true, value_name = "LEVEL", require_equals = true)]
    pub(crate) log_level: Option<String>,

    /// Directory to log output.
    #[arg(long, global = true, value_name = "LOG_DIR")]
    pub(crate) log_dir: Option<PathBuf>,

    /// Disable logging to files.
    #[arg(long, global = true)]
    pub(crate) no_log_files: bool,
}

/// Inputs accepted only by ordinary service startup.
#[derive(Args)]
pub(crate) struct ServiceOptions {
    /// Authorize first initialization of an uninitialized database.
    #[arg(long)]
    pub(crate) initialize_db: bool,

    /// Clear processing data and rebuild from the node current pruning point.
    #[arg(long)]
    pub(crate) clear_db: bool,

    /// Declarative token authorizing one idempotent database reinitialization.
    #[arg(long, value_name = "TOKEN")]
    pub(crate) reinitialize_db_token: Option<OsString>,

    /// Listen for public HTTP requests on this socket address.
    #[arg(long, value_name = "ADDRESS")]
    pub(crate) http_listen: Option<String>,

    /// Serve the browser application from this directory.
    #[arg(long, value_name = "PATH")]
    pub(crate) web_root: Option<PathBuf>,

    /// Configure the browser's external block-explorer URL template.
    #[arg(long, value_name = "TEMPLATE")]
    pub(crate) block_explorer_url_template: Option<String>,
}

impl ServiceOptions {
    pub(crate) fn first_present_option(&self) -> Option<&'static str> {
        if self.initialize_db {
            Some("--initialize-db")
        } else if self.clear_db {
            Some("--clear-db")
        } else if self.reinitialize_db_token.is_some() {
            Some("--reinitialize-db-token")
        } else if self.http_listen.is_some() {
            Some("--http-listen")
        } else if self.web_root.is_some() {
            Some("--web-root")
        } else if self.block_explorer_url_template.is_some() {
            Some("--block-explorer-url-template")
        } else {
            None
        }
    }
}

/// Top-level administrative command.
#[derive(Subcommand)]
pub(crate) enum Command {
    /// Database lifecycle operations.
    Database(DatabaseCommand),
}

/// Database administrative command group.
#[derive(Args)]
pub(crate) struct DatabaseCommand {
    /// Selected database operation.
    #[command(subcommand)]
    pub(crate) command: DatabaseSubcommand,
}

/// One-shot database operation.
#[derive(Subcommand)]
pub(crate) enum DatabaseSubcommand {
    /// Replace a recognized KGI schema and bind an empty schema to the selected network.
    Reinitialize(ReinitializeOptions),
}

/// Administrative reinitialization options.
#[derive(Args)]
pub(crate) struct ReinitializeOptions {
    /// Answer yes to all interactive console questions.
    #[arg(long)]
    pub(crate) yes: bool,

    /// Hidden catcher used to reject the service-only token without echoing
    /// its value in an unknown-argument diagnostic.
    #[arg(long, value_name = "TOKEN", hide = true)]
    pub(crate) reinitialize_db_token: Option<OsString>,
}

#[cfg(test)]
mod tests {
    use clap::{Arg, Command, CommandFactory};

    use super::Cli;

    fn argument<'a>(command: &'a Command, id: &str) -> &'a Arg {
        command.get_arguments().find(|argument| argument.get_id() == id).unwrap()
    }

    #[test]
    fn matching_rusty_kaspa_metadata_is_retained() {
        let command = Cli::command();

        let config_file = argument(&command, "config_file");
        assert_eq!(config_file.get_short(), Some('C'));
        assert_eq!(config_file.get_value_names().unwrap()[0].to_string(), "CONFIG_FILE");
        assert_eq!(config_file.get_help().unwrap().to_string(), "Path of the TOML configuration file");

        for (id, help) in [
            ("testnet", "Use the test network"),
            ("devnet", "Use the development test network"),
            ("simnet", "Use the simulation test network"),
        ] {
            assert_eq!(argument(&command, id).get_help().unwrap().to_string(), help);
        }

        let log_level = argument(&command, "log_level");
        assert_eq!(log_level.get_short(), Some('d'));
        assert_eq!(log_level.get_value_names().unwrap()[0].to_string(), "LEVEL");
        assert!(log_level.is_require_equals_set());
        assert!(log_level.get_help().unwrap().to_string().starts_with("Logging level for all subsystems"));
        assert!(log_level.get_long_help().unwrap().to_string().contains("<subsystem>=<level>"));

        let netsuffix = argument(&command, "netsuffix");
        assert_eq!(netsuffix.get_value_names().unwrap()[0].to_string(), "netsuffix");
        assert!(netsuffix.is_require_equals_set());
        assert_eq!(netsuffix.get_help().unwrap().to_string(), "Testnet network suffix number");

        let override_file = argument(&command, "override_params_file");
        assert!(override_file.is_require_equals_set());
        assert_eq!(override_file.get_help().unwrap().to_string(), "Path to a JSON file containing override parameters");

        let log_dir = argument(&command, "log_dir");
        assert_eq!(log_dir.get_value_names().unwrap()[0].to_string(), "LOG_DIR");
        assert_eq!(log_dir.get_help().unwrap().to_string(), "Directory to log output");
        assert_eq!(argument(&command, "no_log_files").get_help().unwrap().to_string(), "Disable logging to files");
    }

    #[test]
    fn matching_kgi_v1_metadata_is_retained() {
        let mut command = Cli::command();
        command.build();

        let version = argument(&command, "version");
        assert_eq!(version.get_short(), Some('V'));

        let node_rpc_url = argument(&command, "node_rpc_url");
        assert_eq!(node_rpc_url.get_short(), Some('s'));
        assert_eq!(node_rpc_url.get_help().unwrap().to_string(), "Node gRPC server to connect to");

        let database_url = argument(&command, "database_url");
        assert_eq!(database_url.get_value_names().unwrap()[0].to_string(), "DATABASE_URL");
        assert_eq!(database_url.get_help().unwrap().to_string(), "PostgreSQL database URL to connect to");
        assert!(database_url.get_long_help().unwrap().to_string().contains("postgresql://<username>:<password>"));

        let clear_db = argument(&command, "clear_db");
        assert_eq!(clear_db.get_help().unwrap().to_string(), "Clear processing data and rebuild from the node current pruning point");

        let database = command.find_subcommand("database").unwrap();
        let reinitialize = database.find_subcommand("reinitialize").unwrap();
        assert_eq!(argument(reinitialize, "yes").get_help().unwrap().to_string(), "Answer yes to all interactive console questions");
    }
}
