//! Configuration handling for Neon bindings.

use serde::{Deserialize, Serialize};

/// Puffgres configuration.
#[derive(Debug, Serialize, Deserialize)]
pub struct PuffgresConfig {
    pub postgres: PostgresConfig,
    pub turbopuffer: TurbopufferConfig,
    #[serde(default)]
    pub providers: ProvidersConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub connection_string: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TurbopufferConfig {
    pub api_key: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProvidersConfig {
    pub embeddings: Option<EmbeddingProviderConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmbeddingProviderConfig {
    #[serde(rename = "type")]
    pub provider_type: String,
    pub model: String,
    pub api_key: String,
}
