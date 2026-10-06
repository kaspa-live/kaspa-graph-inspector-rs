//! Top-level configuration source loading, resolution, and static validation.

#![allow(dead_code, reason = "wired into process entry by the command parsing increment")]

use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fmt, fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use kaspa_consensus_core::network::{NetworkId, NetworkType};
use kaspa_core::log::LevelFilter;
use kgi_core::config::{
    DatabaseConfig, DatabaseUrl, HttpConfig, KgiConfig, LoggingConfig, NetworkConfig, NodeConfig, ReinitializationToken, WebConfig,
};
use serde::Deserialize;
use thiserror::Error;
use url::Url;

const DEFAULT_TESTNET_SUFFIX: u32 = 10;
const DEFAULT_HTTP_LISTEN: &str = "127.0.0.1:8080";
const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_LOG_DIRECTORY: &str = "./logs";
const WEB_ROOT_FROM_BIN: &str = "share/kgi/web";

const ENV_CONFIG_FILE: &str = "KGI_CONFIG_FILE";
const ENV_MAINNET: &str = "KGI_MAINNET";
const ENV_TESTNET: &str = "KGI_TESTNET";
const ENV_DEVNET: &str = "KGI_DEVNET";
const ENV_SIMNET: &str = "KGI_SIMNET";
const ENV_NETSUFFIX: &str = "KGI_NETSUFFIX";
const ENV_OVERRIDE_PARAMS_FILE: &str = "KGI_OVERRIDE_PARAMS_FILE";
const ENV_NODE_RPC_URL: &str = "KGI_NODE_RPC_URL";
const ENV_DATABASE_URL: &str = "KGI_DATABASE_URL";
const ENV_INITIALIZE_DB: &str = "KGI_INITIALIZE_DB";
const ENV_REINITIALIZE_DB_TOKEN: &str = "KGI_REINITIALIZE_DB_TOKEN";
const ENV_HTTP_LISTEN: &str = "KGI_HTTP_LISTEN";
const ENV_LOG_LEVEL: &str = "KGI_LOG_LEVEL";
const ENV_LOG_DIR: &str = "KGI_LOG_DIR";
const ENV_NO_LOG_FILES: &str = "KGI_NO_LOG_FILES";
const ENV_WEB_ROOT: &str = "KGI_WEB_ROOT";
const ENV_BLOCK_EXPLORER_TEMPLATE: &str = "KGI_BLOCK_EXPLORER_URL_TEMPLATE";

const SUPPORTED_ENVIRONMENT_VARIABLES: [&str; 17] = [
    ENV_CONFIG_FILE,
    ENV_MAINNET,
    ENV_TESTNET,
    ENV_DEVNET,
    ENV_SIMNET,
    ENV_NETSUFFIX,
    ENV_OVERRIDE_PARAMS_FILE,
    ENV_NODE_RPC_URL,
    ENV_DATABASE_URL,
    ENV_INITIALIZE_DB,
    ENV_REINITIALIZE_DB_TOKEN,
    ENV_HTTP_LISTEN,
    ENV_LOG_LEVEL,
    ENV_LOG_DIR,
    ENV_NO_LOG_FILES,
    ENV_WEB_ROOT,
    ENV_BLOCK_EXPLORER_TEMPLATE,
];

/// One configuration source after source-specific syntax has been parsed.
#[derive(Clone, Default)]
pub(crate) struct ConfigLayer {
    pub(crate) network: NetworkLayer,
    pub(crate) node: NodeLayer,
    pub(crate) database: DatabaseLayer,
    pub(crate) http: HttpLayer,
    pub(crate) logging: LoggingLayer,
    pub(crate) web: WebLayer,
}

impl ConfigLayer {
    fn compiled_defaults() -> Self {
        Self {
            network: NetworkLayer { mainnet: Some(true), ..NetworkLayer::default() },
            database: DatabaseLayer { initialize: Some(false), ..DatabaseLayer::default() },
            http: HttpLayer { listen: Some(DEFAULT_HTTP_LISTEN.to_owned()) },
            logging: LoggingLayer {
                level: Some(DEFAULT_LOG_LEVEL.to_owned()),
                directory: Some(PathBuf::from(DEFAULT_LOG_DIRECTORY)),
                no_files: Some(false),
            },
            ..Self::default()
        }
    }
}

/// Network values supplied by one configuration source.
#[derive(Clone, Default)]
pub(crate) struct NetworkLayer {
    pub(crate) mainnet: Option<bool>,
    pub(crate) testnet: Option<bool>,
    pub(crate) devnet: Option<bool>,
    pub(crate) simnet: Option<bool>,
    pub(crate) netsuffix: Option<u32>,
    pub(crate) override_params_file: Option<PathBuf>,
}

/// Node values supplied by one configuration source.
#[derive(Clone, Default)]
pub(crate) struct NodeLayer {
    pub(crate) rpc_url: Option<String>,
}

/// Database values supplied by one configuration source.
#[derive(Clone, Default)]
pub(crate) struct DatabaseLayer {
    pub(crate) url: Option<String>,
    pub(crate) initialize: Option<bool>,
    pub(crate) reinitialization_token: Option<String>,
}

/// HTTP values supplied by one configuration source.
#[derive(Clone, Default)]
pub(crate) struct HttpLayer {
    pub(crate) listen: Option<String>,
}

/// Logging values supplied by one configuration source.
#[derive(Clone, Default)]
pub(crate) struct LoggingLayer {
    pub(crate) level: Option<String>,
    pub(crate) directory: Option<PathBuf>,
    pub(crate) no_files: Option<bool>,
}

/// Web values supplied by one configuration source.
#[derive(Clone, Default)]
pub(crate) struct WebLayer {
    pub(crate) root: Option<PathBuf>,
    pub(crate) block_explorer_url_template: Option<String>,
}

/// Explicit snapshot of supported environment variables.
#[derive(Clone, Default)]
pub(crate) struct Environment {
    values: BTreeMap<&'static str, OsString>,
}

impl Environment {
    /// Captures the supported process environment once.
    pub(crate) fn capture() -> Self {
        let values =
            SUPPORTED_ENVIRONMENT_VARIABLES.into_iter().filter_map(|name| env::var_os(name).map(|value| (name, value))).collect();
        Self { values }
    }

    #[cfg(test)]
    fn from_values(values: impl IntoIterator<Item = (&'static str, OsString)>) -> Self {
        Self { values: values.into_iter().collect() }
    }

    fn config_file(&self) -> Option<PathBuf> {
        self.values.get(ENV_CONFIG_FILE).map(PathBuf::from)
    }

    fn layer(&self) -> Result<ConfigLayer, ConfigError> {
        Ok(ConfigLayer {
            network: NetworkLayer {
                mainnet: self.optional_bool(ENV_MAINNET)?,
                testnet: self.optional_bool(ENV_TESTNET)?,
                devnet: self.optional_bool(ENV_DEVNET)?,
                simnet: self.optional_bool(ENV_SIMNET)?,
                netsuffix: self.optional_parse(ENV_NETSUFFIX)?,
                override_params_file: self.optional_path(ENV_OVERRIDE_PARAMS_FILE),
            },
            node: NodeLayer { rpc_url: self.optional_string(ENV_NODE_RPC_URL)? },
            database: DatabaseLayer {
                url: self.optional_string(ENV_DATABASE_URL)?,
                initialize: self.optional_bool(ENV_INITIALIZE_DB)?,
                reinitialization_token: self.optional_string(ENV_REINITIALIZE_DB_TOKEN)?,
            },
            http: HttpLayer { listen: self.optional_string(ENV_HTTP_LISTEN)? },
            logging: LoggingLayer {
                level: self.optional_string(ENV_LOG_LEVEL)?,
                directory: self.optional_path(ENV_LOG_DIR),
                no_files: self.optional_bool(ENV_NO_LOG_FILES)?,
            },
            web: WebLayer {
                root: self.optional_path(ENV_WEB_ROOT),
                block_explorer_url_template: self.optional_string(ENV_BLOCK_EXPLORER_TEMPLATE)?,
            },
        })
    }

    fn optional_string(&self, name: &'static str) -> Result<Option<String>, ConfigError> {
        self.values
            .get(name)
            .map(|value| {
                value
                    .clone()
                    .into_string()
                    .map_err(|_| ConfigError::InvalidEnvironmentValue { name, reason: "value is not valid UTF-8" })
            })
            .transpose()
    }

    fn optional_path(&self, name: &'static str) -> Option<PathBuf> {
        self.values.get(name).map(PathBuf::from)
    }

    fn optional_bool(&self, name: &'static str) -> Result<Option<bool>, ConfigError> {
        self.optional_parse(name)
    }

    fn optional_parse<T>(&self, name: &'static str) -> Result<Option<T>, ConfigError>
    where
        T: FromStr,
    {
        self.values
            .get(name)
            .map(|value| {
                value
                    .to_str()
                    .and_then(|value| value.parse().ok())
                    .ok_or(ConfigError::InvalidEnvironmentValue { name, reason: "value has invalid syntax" })
            })
            .transpose()
    }
}

/// Inputs needed to resolve one process configuration.
pub(crate) struct ResolveRequest<'a> {
    /// CLI-selected configuration file, which overrides `KGI_CONFIG_FILE`.
    pub(crate) cli_config_file: Option<&'a Path>,
    /// Explicit CLI configuration values.
    pub(crate) cli: &'a ConfigLayer,
    /// Captured process environment.
    pub(crate) environment: &'a Environment,
    /// Real path of the running executable.
    pub(crate) executable_path: &'a Path,
}

/// Loads, resolves, and statically validates the process configuration.
pub(crate) fn resolve(request: ResolveRequest<'_>) -> Result<KgiConfig, ConfigError> {
    let environment = request.environment.layer()?;
    let config_file = request.cli_config_file.map(Path::to_path_buf).or_else(|| request.environment.config_file());
    let file = config_file.as_deref().map(load_toml).transpose()?.unwrap_or_default();
    let defaults = ConfigLayer::compiled_defaults();
    let layers = [
        SourcedLayer { source: ConfigSource::CompiledDefault, values: &defaults },
        SourcedLayer { source: ConfigSource::Toml, values: &file },
        SourcedLayer { source: ConfigSource::Environment, values: &environment },
        SourcedLayer { source: ConfigSource::CommandLine, values: request.cli },
    ];

    validate_network_layers(&layers)?;
    let network_id = resolve_network_id(&layers)?;
    let override_params_file = resolve_value(&layers, |layer| layer.network.override_params_file.clone()).map(|(_, value)| value);

    let rpc_url = match resolve_value(&layers, |layer| layer.node.rpc_url.clone()) {
        Some((source, value)) => parse_url("node.rpc-url", source, &value)?,
        None => default_rpc_url(network_id),
    };

    let (database_source, database_url) =
        resolve_value(&layers, |layer| layer.database.url.clone()).ok_or(ConfigError::MissingDatabaseUrl)?;
    let database_url = DatabaseUrl::new(parse_url("database.url", database_source, &database_url)?);
    let initialize = resolve_value(&layers, |layer| layer.database.initialize).is_some_and(|(_, value)| value);
    let reinitialization_token = resolve_value(&layers, |layer| layer.database.reinitialization_token.clone())
        .map(|(_, value)| ReinitializationToken::new(value));
    if initialize && reinitialization_token.is_some() {
        return Err(ConfigError::ConflictingDatabaseActions);
    }

    let (listen_origin, listen) =
        resolve_value(&layers, |layer| layer.http.listen.clone()).expect("the compiled defaults include an HTTP listen address");
    let listen = listen.parse().map_err(|_| ConfigError::InvalidValue { field: "http.listen", origin: listen_origin })?;

    let (level_origin, level) =
        resolve_value(&layers, |layer| layer.logging.level.clone()).expect("the compiled defaults include a logging level");
    validate_log_filter(&level).map_err(|()| ConfigError::InvalidValue { field: "logging.level", origin: level_origin })?;
    let selected_log_directory =
        resolve_value(&layers, |layer| layer.logging.directory.clone()).expect("the compiled defaults include a log directory");
    let no_files =
        resolve_value(&layers, |layer| layer.logging.no_files).expect("the compiled defaults include the file-logging switch").1;
    if no_files && selected_log_directory.0 != ConfigSource::CompiledDefault {
        return Err(ConfigError::ConflictingLogFileOptions);
    }
    let log_directory = (!no_files).then_some(selected_log_directory.1);

    let web_root = resolve_value(&layers, |layer| layer.web.root.clone())
        .map_or_else(|| derive_web_root(request.executable_path), |(_, value)| Ok(value))?;
    let block_explorer_url_template = resolve_value(&layers, |layer| layer.web.block_explorer_url_template.clone())
        .map(|(origin, value)| {
            validate_block_explorer_template(&value)
                .map(|()| value)
                .map_err(|()| ConfigError::InvalidValue { field: "web.block-explorer-url-template", origin })
        })
        .transpose()?;

    Ok(KgiConfig {
        network: NetworkConfig { network_id, override_params_file },
        node: NodeConfig { rpc_url },
        database: DatabaseConfig { url: database_url, initialize, reinitialization_token },
        http: HttpConfig { listen },
        logging: LoggingConfig { level, directory: log_directory },
        web: WebConfig { root: web_root, block_explorer_url_template },
    })
}

#[derive(Clone, Copy)]
struct SourcedLayer<'a> {
    source: ConfigSource,
    values: &'a ConfigLayer,
}

fn resolve_value<T>(layers: &[SourcedLayer<'_>], get: impl Fn(&ConfigLayer) -> Option<T>) -> Option<(ConfigSource, T)> {
    layers.iter().rev().find_map(|layer| get(layer.values).map(|value| (layer.source, value)))
}

fn validate_network_layers(layers: &[SourcedLayer<'_>]) -> Result<(), ConfigError> {
    for layer in layers {
        let selected =
            [layer.values.network.mainnet, layer.values.network.testnet, layer.values.network.devnet, layer.values.network.simnet]
                .into_iter()
                .filter(|selected| *selected == Some(true))
                .count();
        if selected > 1 {
            return Err(ConfigError::ConflictingNetworkSelectors { origin: layer.source });
        }
    }
    Ok(())
}

fn resolve_network_id(layers: &[SourcedLayer<'_>]) -> Result<NetworkId, ConfigError> {
    let selected_type = layers.iter().rev().find_map(|layer| {
        let network = &layer.values.network;
        [
            (network.mainnet, NetworkType::Mainnet),
            (network.testnet, NetworkType::Testnet),
            (network.devnet, NetworkType::Devnet),
            (network.simnet, NetworkType::Simnet),
        ]
        .into_iter()
        .find_map(|(selected, network_type)| (selected == Some(true)).then_some(network_type))
    });
    let network_type = selected_type.unwrap_or(NetworkType::Mainnet);
    let suffix = resolve_value(layers, |layer| layer.network.netsuffix);

    match (network_type, suffix) {
        (NetworkType::Testnet, Some((_, suffix))) => Ok(NetworkId::with_suffix(NetworkType::Testnet, suffix)),
        (NetworkType::Testnet, None) => Ok(NetworkId::with_suffix(NetworkType::Testnet, DEFAULT_TESTNET_SUFFIX)),
        (_, Some((origin, _))) => Err(ConfigError::InvalidValue { field: "network.netsuffix", origin }),
        (network_type, None) => Ok(NetworkId::new(network_type)),
    }
}

fn default_rpc_url(network_id: NetworkId) -> Url {
    let port = network_id.network_type().default_rpc_port();
    Url::parse(&format!("grpc://127.0.0.1:{port}")).expect("the built-in node RPC URL must be valid")
}

fn parse_url(field: &'static str, origin: ConfigSource, value: &str) -> Result<Url, ConfigError> {
    Url::parse(value).map_err(|_| ConfigError::InvalidValue { field, origin })
}

fn validate_log_filter(filter: &str) -> Result<(), ()> {
    let mut found = false;
    for spec in filter.split(',').map(str::trim) {
        if spec.is_empty() {
            return Err(());
        }
        found = true;
        let mut parts = spec.split('=');
        let first = parts.next().ok_or(())?;
        match (parts.next(), parts.next()) {
            (None, None) => {
                first.parse::<LevelFilter>().map_err(|_| ())?;
            }
            (Some(level), None) if !first.trim().is_empty() && !level.trim().is_empty() => {
                level.trim().parse::<LevelFilter>().map_err(|_| ())?;
            }
            _ => return Err(()),
        }
    }
    found.then_some(()).ok_or(())
}

fn derive_web_root(executable_path: &Path) -> Result<PathBuf, ConfigError> {
    executable_path
        .parent()
        .and_then(Path::parent)
        .map(|root| root.join(WEB_ROOT_FROM_BIN))
        .ok_or_else(|| ConfigError::CannotDeriveWebRoot { executable_path: executable_path.to_path_buf() })
}

fn validate_block_explorer_template(template: &str) -> Result<(), ()> {
    let mut placeholders = template.match_indices("{hash}");
    if placeholders.next().is_none() || placeholders.next().is_some() {
        return Err(());
    }
    let probe = template.replace("{hash}", &"0".repeat(64));
    let url = Url::parse(&probe).map_err(|_| ())?;
    if !matches!(url.scheme(), "http" | "https") || !url.username().is_empty() || url.password().is_some() {
        return Err(());
    }
    Ok(())
}

fn load_toml(path: &Path) -> Result<ConfigLayer, ConfigError> {
    let bytes = fs::read(path).map_err(|_| ConfigError::ConfigFileRead { path: path.to_path_buf() })?;
    let text = std::str::from_utf8(&bytes).map_err(|_| ConfigError::ConfigFileUtf8 { path: path.to_path_buf() })?;
    let file: FileConfig = toml::from_str(text).map_err(|_| ConfigError::ConfigFileToml { path: path.to_path_buf() })?;
    Ok(file.into())
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
struct FileConfig {
    network: FileNetwork,
    node: FileNode,
    database: FileDatabase,
    http: FileHttp,
    logging: FileLogging,
    web: FileWeb,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
struct FileNetwork {
    mainnet: Option<bool>,
    testnet: Option<bool>,
    devnet: Option<bool>,
    simnet: Option<bool>,
    netsuffix: Option<u32>,
    override_params_file: Option<PathBuf>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
struct FileNode {
    rpc_url: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
struct FileDatabase {
    url: Option<String>,
    initialize: Option<bool>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
struct FileHttp {
    listen: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
struct FileLogging {
    level: Option<String>,
    directory: Option<PathBuf>,
    no_files: Option<bool>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
struct FileWeb {
    root: Option<PathBuf>,
    block_explorer_url_template: Option<String>,
}

impl From<FileConfig> for ConfigLayer {
    fn from(file: FileConfig) -> Self {
        Self {
            network: NetworkLayer {
                mainnet: file.network.mainnet,
                testnet: file.network.testnet,
                devnet: file.network.devnet,
                simnet: file.network.simnet,
                netsuffix: file.network.netsuffix,
                override_params_file: file.network.override_params_file,
            },
            node: NodeLayer { rpc_url: file.node.rpc_url },
            database: DatabaseLayer { url: file.database.url, initialize: file.database.initialize, reinitialization_token: None },
            http: HttpLayer { listen: file.http.listen },
            logging: LoggingLayer { level: file.logging.level, directory: file.logging.directory, no_files: file.logging.no_files },
            web: WebLayer { root: file.web.root, block_explorer_url_template: file.web.block_explorer_url_template },
        }
    }
}

/// Configuration source named in a safe diagnostic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConfigSource {
    /// Built into the executable.
    CompiledDefault,
    /// Explicitly selected TOML file.
    Toml,
    /// Process environment.
    Environment,
    /// Explicit command-line option.
    CommandLine,
}

impl fmt::Display for ConfigSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CompiledDefault => "compiled default",
            Self::Toml => "configuration file",
            Self::Environment => "environment",
            Self::CommandLine => "command line",
        })
    }
}

/// Static configuration resolution failure.
#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    /// A selected file could not be read.
    #[error("selected configuration file {path:?} could not be read")]
    ConfigFileRead { path: PathBuf },
    /// A selected file was not UTF-8 text.
    #[error("selected configuration file {path:?} is not valid UTF-8")]
    ConfigFileUtf8 { path: PathBuf },
    /// A selected file failed strict TOML deserialization.
    #[error("selected configuration file {path:?} contains invalid TOML")]
    ConfigFileToml { path: PathBuf },
    /// A supported environment variable had invalid syntax.
    #[error("environment variable {name} is invalid: {reason}")]
    InvalidEnvironmentValue { name: &'static str, reason: &'static str },
    /// One source selected more than one network.
    #[error("{origin} selects more than one network")]
    ConflictingNetworkSelectors { origin: ConfigSource },
    /// The required database URL was absent.
    #[error("database.url is required")]
    MissingDatabaseUrl,
    /// A field supplied by one source was invalid.
    #[error("{field} from {origin} is invalid")]
    InvalidValue { field: &'static str, origin: ConfigSource },
    /// File logging was disabled while an explicit directory remained selected.
    #[error("logging.directory cannot be configured when file logging is disabled")]
    ConflictingLogFileOptions,
    /// More than one mutually exclusive database startup action was selected.
    #[error("database initialization and reinitialization actions are mutually exclusive")]
    ConflictingDatabaseActions,
    /// The standard Web root could not be derived from the executable path.
    #[error("cannot derive the standard Web root from executable path {executable_path:?}")]
    CannotDeriveWebRoot { executable_path: PathBuf },
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        fs,
        net::SocketAddr,
        path::Path,
        sync::atomic::{AtomicU64, Ordering},
    };

    use kaspa_consensus_core::network::{NetworkId, NetworkType};

    use super::{
        ConfigError, ConfigLayer, ConfigSource, DatabaseLayer, Environment, LoggingLayer, NetworkLayer, ResolveRequest, WebLayer,
        resolve,
    };

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn base_cli() -> ConfigLayer {
        ConfigLayer {
            database: DatabaseLayer { url: Some("postgresql://localhost/kgi".to_owned()), ..DatabaseLayer::default() },
            ..ConfigLayer::default()
        }
    }

    fn resolve_test(cli: &ConfigLayer, environment: &Environment) -> Result<kgi_core::config::KgiConfig, ConfigError> {
        resolve(ResolveRequest { cli_config_file: None, cli, environment, executable_path: Path::new("/opt/kgi/bin/kgi") })
    }

    fn environment(values: &[(&'static str, &str)]) -> Environment {
        Environment::from_values(values.iter().map(|(name, value)| (*name, OsString::from(value))))
    }

    fn write_config(contents: &[u8]) -> TestConfigFile {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("kgi-config-{}-{sequence}.toml", std::process::id()));
        fs::write(&path, contents).expect("test configuration file must be writable");
        TestConfigFile(path)
    }

    struct TestConfigFile(std::path::PathBuf);

    impl Drop for TestConfigFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    #[test]
    fn compiled_defaults_are_resolved_after_required_database_url() {
        let config = resolve_test(&base_cli(), &Environment::default()).expect("defaults must resolve");

        assert_eq!(config.network.network_id, NetworkId::new(NetworkType::Mainnet));
        assert_eq!(config.node.rpc_url.as_str(), "grpc://127.0.0.1:16110");
        assert_eq!(config.http.listen, SocketAddr::from(([127, 0, 0, 1], 8080)));
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.directory.as_deref(), Some(Path::new("./logs")));
        assert_eq!(config.web.root, Path::new("/opt/kgi/share/kgi/web"));
        assert!(config.web.block_explorer_url_template.is_none());
    }

    #[test]
    fn every_network_gets_its_derived_rpc_endpoint() {
        let cases = [
            (NetworkLayer { mainnet: Some(true), ..NetworkLayer::default() }, NetworkId::new(NetworkType::Mainnet), 16110),
            (NetworkLayer { testnet: Some(true), ..NetworkLayer::default() }, NetworkId::with_suffix(NetworkType::Testnet, 10), 16210),
            (NetworkLayer { simnet: Some(true), ..NetworkLayer::default() }, NetworkId::new(NetworkType::Simnet), 16510),
            (NetworkLayer { devnet: Some(true), ..NetworkLayer::default() }, NetworkId::new(NetworkType::Devnet), 16610),
        ];

        for (network, expected_id, port) in cases {
            let cli = ConfigLayer { network, ..base_cli() };
            let config = resolve_test(&cli, &Environment::default()).expect("network defaults must resolve");
            assert_eq!(config.network.network_id, expected_id);
            assert_eq!(config.node.rpc_url.as_str(), format!("grpc://127.0.0.1:{port}"));
        }
    }

    #[test]
    fn network_selection_is_atomic_at_the_highest_source() {
        let file = write_config(
            br#"
                [network]
                testnet = true
                netsuffix = 11

                [database]
                url = "postgresql://file/kgi"
            "#,
        );
        let environment =
            environment(&[("KGI_CONFIG_FILE", file.0.to_str().expect("test path must be UTF-8")), ("KGI_MAINNET", "true")]);
        let cli = ConfigLayer { database: DatabaseLayer { url: None, ..DatabaseLayer::default() }, ..ConfigLayer::default() };

        let error = resolve_test(&cli, &environment).expect_err("a lower-source testnet suffix is invalid with selected mainnet");
        assert!(matches!(error, ConfigError::InvalidValue { field: "network.netsuffix", origin: ConfigSource::Toml }));
    }

    #[test]
    fn cli_values_override_environment_and_toml_fields() {
        let file = write_config(
            br#"
                [database]
                url = "postgresql://file/kgi"
                initialize = true
                [http]
                listen = "127.0.0.1:8001"
            "#,
        );
        let environment = environment(&[
            ("KGI_CONFIG_FILE", file.0.to_str().expect("test path must be UTF-8")),
            ("KGI_DATABASE_URL", "postgresql://environment/kgi"),
            ("KGI_HTTP_LISTEN", "127.0.0.1:8002"),
        ]);
        let cli = ConfigLayer {
            database: DatabaseLayer { url: Some("postgresql://command-line/kgi".to_owned()), ..DatabaseLayer::default() },
            http: super::HttpLayer { listen: Some("127.0.0.1:8003".to_owned()) },
            ..ConfigLayer::default()
        };

        let config = resolve_test(&cli, &environment).expect("precedence must resolve");
        assert_eq!(config.database.url.expose_url().host_str(), Some("command-line"));
        assert!(config.database.initialize);
        assert_eq!(config.http.listen, SocketAddr::from(([127, 0, 0, 1], 8003)));
    }

    #[test]
    fn complete_toml_surface_resolves_to_typed_values() {
        let file = write_config(
            br#"
                [network]
                testnet = true
                netsuffix = 12
                override-params-file = "/etc/kgi/consensus.json"

                [node]
                rpc-url = "grpc://node.example:16210"

                [database]
                url = "postgresql://db.example/kgi"
                initialize = true

                [http]
                listen = "0.0.0.0:9080"

                [logging]
                level = "info,kgi_processing=debug"
                directory = "/var/log/kgi"
                no-files = false

                [web]
                root = "/opt/kgi/web"
                block-explorer-url-template = "https://explorer.example/blocks/{hash}"
            "#,
        );
        let cli = ConfigLayer::default();
        let config = resolve(ResolveRequest {
            cli_config_file: Some(&file.0),
            cli: &cli,
            environment: &Environment::default(),
            executable_path: Path::new("/opt/kgi/bin/kgi"),
        })
        .expect("complete TOML surface must resolve");

        assert_eq!(config.network.network_id, NetworkId::with_suffix(NetworkType::Testnet, 12));
        assert_eq!(config.network.override_params_file.as_deref(), Some(Path::new("/etc/kgi/consensus.json")));
        assert_eq!(config.node.rpc_url.as_str(), "grpc://node.example:16210");
        assert_eq!(config.database.url.expose_url().as_str(), "postgresql://db.example/kgi");
        assert!(config.database.initialize);
        assert!(config.database.reinitialization_token.is_none());
        assert_eq!(config.http.listen, SocketAddr::from(([0, 0, 0, 0], 9080)));
        assert_eq!(config.logging.level, "info,kgi_processing=debug");
        assert_eq!(config.logging.directory.as_deref(), Some(Path::new("/var/log/kgi")));
        assert_eq!(config.web.root, Path::new("/opt/kgi/web"));
        assert_eq!(config.web.block_explorer_url_template.as_deref(), Some("https://explorer.example/blocks/{hash}"));
    }

    #[test]
    fn complete_environment_surface_resolves_to_typed_values() {
        let environment = environment(&[
            ("KGI_DEVNET", "true"),
            ("KGI_OVERRIDE_PARAMS_FILE", "/etc/kgi/devnet.json"),
            ("KGI_NODE_RPC_URL", "grpc://node.example:16610"),
            ("KGI_DATABASE_URL", "postgresql://db.example/kgi"),
            ("KGI_INITIALIZE_DB", "false"),
            ("KGI_REINITIALIZE_DB_TOKEN", "rotation-2"),
            ("KGI_HTTP_LISTEN", "0.0.0.0:9080"),
            ("KGI_LOG_LEVEL", "warn,kgi_storage=trace"),
            ("KGI_NO_LOG_FILES", "true"),
            ("KGI_WEB_ROOT", "/srv/kgi/web"),
            ("KGI_BLOCK_EXPLORER_URL_TEMPLATE", "https://explorer.example/{hash}"),
        ]);
        let cli = ConfigLayer::default();
        let config = resolve_test(&cli, &environment).expect("complete environment surface must resolve");

        assert_eq!(config.network.network_id, NetworkId::new(NetworkType::Devnet));
        assert_eq!(config.network.override_params_file.as_deref(), Some(Path::new("/etc/kgi/devnet.json")));
        assert_eq!(config.node.rpc_url.as_str(), "grpc://node.example:16610");
        assert_eq!(config.database.url.expose_url().as_str(), "postgresql://db.example/kgi");
        assert!(!config.database.initialize);
        assert_eq!(config.database.reinitialization_token.as_ref().map(|token| token.expose_secret()), Some("rotation-2"));
        assert_eq!(config.http.listen, SocketAddr::from(([0, 0, 0, 0], 9080)));
        assert_eq!(config.logging.level, "warn,kgi_storage=trace");
        assert!(config.logging.directory.is_none());
        assert_eq!(config.web.root, Path::new("/srv/kgi/web"));
        assert_eq!(config.web.block_explorer_url_template.as_deref(), Some("https://explorer.example/{hash}"));
    }

    #[test]
    fn cli_config_file_selection_overrides_environment_selection() {
        let environment_file = write_config(b"unknown = true");
        let cli_file = write_config(b"[database]\nurl = \"postgresql://cli-file/kgi\"\n");
        let environment = environment(&[("KGI_CONFIG_FILE", environment_file.0.to_str().expect("test path must be UTF-8"))]);
        let cli = ConfigLayer::default();

        let config = resolve(ResolveRequest {
            cli_config_file: Some(&cli_file.0),
            cli: &cli,
            environment: &environment,
            executable_path: Path::new("/opt/kgi/bin/kgi"),
        })
        .expect("CLI-selected file must win");
        assert_eq!(config.database.url.expose_url().host_str(), Some("cli-file"));
    }

    #[test]
    fn strict_toml_rejects_unknown_top_level_and_nested_fields() {
        for contents in [
            b"unknown = true\n[database]\nurl = \"postgresql://localhost/kgi\"\n".as_slice(),
            b"[database]\nurl = \"postgresql://localhost/kgi\"\nunknown = true\n".as_slice(),
        ] {
            let file = write_config(contents);
            let cli = ConfigLayer::default();
            let error = resolve(ResolveRequest {
                cli_config_file: Some(&file.0),
                cli: &cli,
                environment: &Environment::default(),
                executable_path: Path::new("/opt/kgi/bin/kgi"),
            })
            .expect_err("unknown TOML fields must fail");
            assert!(matches!(error, ConfigError::ConfigFileToml { .. }));
        }
    }

    #[test]
    fn selected_file_failures_are_distinct_and_redacted() {
        let missing = std::env::temp_dir().join(format!("missing-kgi-config-{}.toml", std::process::id()));
        let invalid_utf8 = write_config(&[0xff]);
        let secret = "postgresql://alice:hunter2@localhost/kgi?secret=query";
        let malformed = write_config(format!("[database]\nurl = \"{secret}\"\nunknown = [").as_bytes());
        let cli = ConfigLayer::default();

        let errors = [missing.as_path(), invalid_utf8.0.as_path(), malformed.0.as_path()].map(|path| {
            resolve(ResolveRequest {
                cli_config_file: Some(path),
                cli: &cli,
                environment: &Environment::default(),
                executable_path: Path::new("/opt/kgi/bin/kgi"),
            })
            .expect_err("selected file must fail")
        });

        assert!(matches!(errors[0], ConfigError::ConfigFileRead { .. }));
        assert!(matches!(errors[1], ConfigError::ConfigFileUtf8 { .. }));
        assert!(matches!(errors[2], ConfigError::ConfigFileToml { .. }));
        let diagnostic = format!("{} {:?}", errors[2], errors[2]);
        for value in [secret, "alice", "hunter2", "secret=query"] {
            assert!(!diagnostic.contains(value));
        }
    }

    #[test]
    fn invalid_explicit_values_do_not_fall_back() {
        let cases = [
            ("KGI_MAINNET", "yes"),
            ("KGI_NETSUFFIX", "ten"),
            ("KGI_HTTP_LISTEN", "localhost"),
            ("KGI_NODE_RPC_URL", "not a URL"),
            ("KGI_LOG_LEVEL", "verbose"),
        ];

        for (name, value) in cases {
            let error = resolve_test(&base_cli(), &environment(&[(name, value)])).expect_err("invalid environment value must fail");
            assert!(matches!(error, ConfigError::InvalidEnvironmentValue { .. } | ConfigError::InvalidValue { .. }));
        }
    }

    #[test]
    fn database_url_and_token_are_redacted_from_resolution_errors() {
        let invalid_url = "postgresql://alice:hunter2@exa mple/kgi?secret=query";
        let token = "rotate-me-secret";
        let environment = environment(&[("KGI_DATABASE_URL", invalid_url), ("KGI_REINITIALIZE_DB_TOKEN", token)]);
        let cli = ConfigLayer::default();

        let error = resolve_test(&cli, &environment).expect_err("invalid database URL must fail");
        let diagnostic = format!("{error} {error:?}");
        for secret in [invalid_url, "alice", "hunter2", "secret=query", token] {
            assert!(!diagnostic.contains(secret));
        }
    }

    #[test]
    fn initialization_and_reinitialization_token_are_mutually_exclusive() {
        let cli = ConfigLayer {
            database: DatabaseLayer {
                url: Some("postgresql://localhost/kgi".to_owned()),
                initialize: Some(true),
                reinitialization_token: Some("rotation-2".to_owned()),
            },
            ..ConfigLayer::default()
        };

        assert!(matches!(resolve_test(&cli, &Environment::default()), Err(ConfigError::ConflictingDatabaseActions)));
    }

    #[test]
    fn network_selector_and_suffix_rules_are_enforced() {
        let conflicting =
            ConfigLayer { network: NetworkLayer { testnet: Some(true), devnet: Some(true), ..NetworkLayer::default() }, ..base_cli() };
        assert!(matches!(
            resolve_test(&conflicting, &Environment::default()),
            Err(ConfigError::ConflictingNetworkSelectors { origin: ConfigSource::CommandLine })
        ));

        let mainnet_suffix = ConfigLayer { network: NetworkLayer { netsuffix: Some(10), ..NetworkLayer::default() }, ..base_cli() };
        assert!(matches!(
            resolve_test(&mainnet_suffix, &Environment::default()),
            Err(ConfigError::InvalidValue { field: "network.netsuffix", .. })
        ));

        let testnet = ConfigLayer {
            network: NetworkLayer { testnet: Some(true), netsuffix: Some(12), ..NetworkLayer::default() },
            ..base_cli()
        };
        let config = resolve_test(&testnet, &Environment::default()).expect("testnet suffix must resolve");
        assert_eq!(config.network.network_id, NetworkId::with_suffix(NetworkType::Testnet, 12));
    }

    #[test]
    fn log_filter_and_file_output_are_resolved_for_rusty_kaspa_logger() {
        let cli = ConfigLayer {
            logging: LoggingLayer {
                level: Some("info,kgi_processing=debug,kgi_storage=off".to_owned()),
                no_files: Some(true),
                ..LoggingLayer::default()
            },
            ..base_cli()
        };
        let config = resolve_test(&cli, &Environment::default()).expect("valid logger filter must resolve");
        assert_eq!(config.logging.level, "info,kgi_processing=debug,kgi_storage=off");
        assert!(config.logging.directory.is_none());

        for filter in ["", "verbose", "info,", "module=verbose", "=info", "module=info=debug"] {
            let cli =
                ConfigLayer { logging: LoggingLayer { level: Some(filter.to_owned()), ..LoggingLayer::default() }, ..base_cli() };
            assert!(matches!(
                resolve_test(&cli, &Environment::default()),
                Err(ConfigError::InvalidValue { field: "logging.level", .. })
            ));
        }
    }

    #[test]
    fn explicit_log_directory_conflicts_with_disabled_files() {
        let cli = ConfigLayer {
            logging: LoggingLayer { directory: Some("custom-logs".into()), no_files: Some(true), ..LoggingLayer::default() },
            ..base_cli()
        };
        assert!(matches!(resolve_test(&cli, &Environment::default()), Err(ConfigError::ConflictingLogFileOptions)));
    }

    #[test]
    fn web_template_retains_valid_raw_text_and_rejects_invalid_forms() {
        let valid = "https://explorer.example/blocks/{hash}?source=kgi";
        let cli =
            ConfigLayer { web: WebLayer { block_explorer_url_template: Some(valid.to_owned()), ..WebLayer::default() }, ..base_cli() };
        let config = resolve_test(&cli, &Environment::default()).expect("valid explorer template must resolve");
        assert_eq!(config.web.block_explorer_url_template.as_deref(), Some(valid));

        for template in [
            "https://explorer.example/blocks/no-placeholder",
            "https://explorer.example/{hash}/{hash}",
            "https://explorer.example/%7Bhash%7D",
            "ftp://explorer.example/{hash}",
            "https://user@example.com/{hash}",
            "not a url/{hash}",
        ] {
            let cli = ConfigLayer {
                web: WebLayer { block_explorer_url_template: Some(template.to_owned()), ..WebLayer::default() },
                ..base_cli()
            };
            assert!(matches!(
                resolve_test(&cli, &Environment::default()),
                Err(ConfigError::InvalidValue { field: "web.block-explorer-url-template", .. })
            ));
        }
    }

    #[test]
    fn database_url_is_required() {
        assert!(matches!(resolve_test(&ConfigLayer::default(), &Environment::default()), Err(ConfigError::MissingDatabaseUrl)));
    }
}
