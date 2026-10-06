//! Resolved process configuration values.

use std::{fmt, net::SocketAddr, path::PathBuf};

use kaspa_consensus_core::network::NetworkId;
use url::Url;

/// Complete immutable configuration for one KGI process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KgiConfig {
    /// Selected Kaspa network and its optional consensus override file.
    pub network: NetworkConfig,
    /// Node RPC endpoint configuration.
    pub node: NodeConfig,
    /// Database connection and initialization configuration.
    pub database: DatabaseConfig,
    /// Public HTTP listener configuration.
    pub http: HttpConfig,
    /// Process logging configuration.
    pub logging: LoggingConfig,
    /// Browser application configuration.
    pub web: WebConfig,
}

/// Resolved Kaspa network configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkConfig {
    /// Exact network, including the testnet suffix when applicable.
    pub network_id: NetworkId,
    /// Optional consensus parameter override file.
    pub override_params_file: Option<PathBuf>,
}

/// Resolved node connection configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeConfig {
    /// Node gRPC endpoint.
    pub rpc_url: Url,
}

/// Resolved database configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseConfig {
    /// PostgreSQL connection URL.
    pub url: DatabaseUrl,
    /// Whether first initialization of an uninitialized database is authorized.
    pub initialize: bool,
    /// Optional declarative token authorizing one idempotent reinitialization.
    pub reinitialization_token: Option<ReinitializationToken>,
}

/// Resolved public HTTP server configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpConfig {
    /// Socket address on which the public server listens.
    pub listen: SocketAddr,
}

/// Resolved logging configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoggingConfig {
    /// Validated rusty-kaspa logger filter expression.
    pub level: String,
    /// Rotating-log directory, or `None` when file logging is disabled.
    pub directory: Option<PathBuf>,
}

/// Resolved browser application configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebConfig {
    /// Effective root directory containing the built browser application.
    pub root: PathBuf,
    /// Validated block-explorer URL template retained in its original form.
    pub block_explorer_url_template: Option<String>,
}

/// Parsed PostgreSQL URL with redacted formatting.
#[derive(Clone, PartialEq, Eq)]
pub struct DatabaseUrl(Url);

impl DatabaseUrl {
    /// Wraps a parsed PostgreSQL URL.
    #[must_use]
    pub fn new(url: Url) -> Self {
        Self(url)
    }

    /// Exposes the parsed URL to the database connection owner.
    ///
    /// The returned value can contain credentials and must not be formatted in
    /// diagnostics, logs, status values, or panic messages.
    #[must_use]
    pub fn expose_url(&self) -> &Url {
        &self.0
    }

    /// Consumes the wrapper and exposes the parsed URL.
    ///
    /// The returned value can contain credentials and must not be formatted in
    /// diagnostics, logs, status values, or panic messages.
    #[must_use]
    pub fn into_url(self) -> Url {
        self.0
    }
}

impl fmt::Debug for DatabaseUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DatabaseUrl([REDACTED])")
    }
}

/// Declarative database reinitialization token with redacted formatting.
#[derive(Clone, PartialEq, Eq)]
pub struct ReinitializationToken(Box<str>);

impl ReinitializationToken {
    /// Protects a resolved reinitialization token.
    #[must_use]
    pub fn new(token: impl Into<Box<str>>) -> Self {
        Self(token.into())
    }

    /// Exposes the token to the storage-owned reinitialization operation.
    ///
    /// The returned value must not be formatted in diagnostics, logs, status
    /// values, confirmation messages, or panic messages.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ReinitializationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReinitializationToken([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, path::PathBuf};

    use kaspa_consensus_core::network::{NetworkId, NetworkType};

    use super::{
        DatabaseConfig, DatabaseUrl, HttpConfig, KgiConfig, LoggingConfig, NetworkConfig, NodeConfig, ReinitializationToken, WebConfig,
    };

    #[test]
    fn protected_values_expose_their_original_contents() {
        let database_url =
            url::Url::parse("postgresql://alice:hunter2@db.example:5432/kgi?secret=query").expect("test database URL must parse");
        let protected_url = DatabaseUrl::new(database_url.clone());
        let token = ReinitializationToken::new("rotate-me");

        assert_eq!(protected_url.expose_url(), &database_url);
        assert_eq!(protected_url.into_url(), database_url);
        assert_eq!(token.expose_secret(), "rotate-me");
    }

    #[test]
    fn complete_configuration_debug_output_redacts_secrets() {
        let config = KgiConfig {
            network: NetworkConfig { network_id: NetworkId::new(NetworkType::Mainnet), override_params_file: None },
            node: NodeConfig { rpc_url: url::Url::parse("grpc://127.0.0.1:16110").expect("test node URL must parse") },
            database: DatabaseConfig {
                url: DatabaseUrl::new(
                    url::Url::parse("postgresql://alice:hunter2@db.example:5432/kgi?secret=query")
                        .expect("test database URL must parse"),
                ),
                initialize: false,
                reinitialization_token: Some(ReinitializationToken::new("rotate-me")),
            },
            http: HttpConfig { listen: (Ipv4Addr::LOCALHOST, 8080).into() },
            logging: LoggingConfig { level: "info,kgi_processing=debug".to_owned(), directory: Some(PathBuf::from("logs")) },
            web: WebConfig { root: PathBuf::from("web"), block_explorer_url_template: None },
        };

        let output = format!("{config:?}");

        assert!(output.contains("DatabaseUrl([REDACTED])"));
        assert!(output.contains("ReinitializationToken([REDACTED])"));
        for secret in ["alice", "hunter2", "secret=query", "rotate-me"] {
            assert!(!output.contains(secret));
        }
    }
}
