use std::{cmp::max, fs, path::Path};

use kaspa_consensus_core::{
    config::params::{OverrideParams, Params},
    network::{NetworkId, NetworkType},
};
use kgi_model::block::{MAX_BLUE_SCORE, MAX_DAA_SCORE};

use crate::error::{ConsensusParameter, ConsensusParameterReason, ConsensusResolutionError, InvalidConsensusParameters};

/// Local consensus values used by KGI recovery algorithms.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KgiConsensusParams {
    bps: u64,
    mergeset_size_limit: u64,
    anticone_finalization_depth: u64,
    catchup_max_daa_gap: u64,
}

impl KgiConsensusParams {
    /// Resolves and validates the local assumptions for `network_id`.
    pub fn resolve(network_id: NetworkId, override_params_file: Option<&Path>) -> Result<Self, ConsensusResolutionError> {
        let (params, source) = resolve_upstream_params(network_id, override_params_file)?;
        let resolved = Self::from_upstream(&params)?;

        if !network_id.is_mainnet() {
            kaspa_core::warn!(
                "node consensus parameters cannot be verified: network={}, source={}, bps={}, mergeset_size_limit={}, anticone_finalization_depth={}, catchup_max_daa_gap={}",
                network_id,
                source.as_str(),
                resolved.bps,
                resolved.mergeset_size_limit,
                resolved.anticone_finalization_depth,
                resolved.catchup_max_daa_gap,
            );
        }

        Ok(resolved)
    }

    fn from_upstream(params: &Params) -> Result<Self, InvalidConsensusParameters> {
        let raw = &params.blockrate;
        require_minimum(ConsensusParameter::TargetTimePerBlock, raw.target_time_per_block, 1)?;
        require_maximum(ConsensusParameter::TargetTimePerBlock, raw.target_time_per_block, 1_000)?;
        require_minimum(ConsensusParameter::MergeSetSizeLimit, raw.mergeset_size_limit, 2)?;

        let get_blocks_core_budget = checked_add(ConsensusParameter::GetBlocksCoreBudget, raw.mergeset_size_limit, 1)?;
        checked_mul(ConsensusParameter::VspcV2AddedBatchSize, 10, raw.mergeset_size_limit)?;

        let ghostdag_k = u64::from(raw.ghostdag_k);
        let four_merge_k = checked_mul(ConsensusParameter::AnticoneFinalizationDepth, 4, raw.mergeset_size_limit)
            .and_then(|value| checked_mul(ConsensusParameter::AnticoneFinalizationDepth, value, ghostdag_k))?;
        let two_k = checked_mul(ConsensusParameter::AnticoneFinalizationDepth, 2, ghostdag_k)?;
        let anticone_expression = checked_add(ConsensusParameter::AnticoneFinalizationDepth, raw.finality_depth, raw.merge_depth)
            .and_then(|value| checked_add(ConsensusParameter::AnticoneFinalizationDepth, value, four_merge_k))
            .and_then(|value| checked_add(ConsensusParameter::AnticoneFinalizationDepth, value, two_k))
            .and_then(|value| checked_add(ConsensusParameter::AnticoneFinalizationDepth, value, 2))?;

        let bps = params.bps();
        let mergeset_size_limit = params.mergeset_size_limit();
        let anticone_finalization_depth = params.anticone_finalization_depth();
        debug_assert_eq!(anticone_finalization_depth, raw.pruning_depth.min(anticone_expression));

        require_maximum(ConsensusParameter::AnticoneFinalizationDepth, anticone_finalization_depth, MAX_BLUE_SCORE)?;

        let thirty_seconds = checked_mul(ConsensusParameter::CatchupMaxDaaGap, 30, bps)?;
        let catchup_max_daa_gap = max(thirty_seconds, get_blocks_core_budget);
        require_maximum(ConsensusParameter::CatchupMaxDaaGap, catchup_max_daa_gap, MAX_DAA_SCORE)?;

        Ok(Self { bps, mergeset_size_limit, anticone_finalization_depth, catchup_max_daa_gap })
    }

    /// Returns the assumed blocks per second.
    #[must_use]
    pub const fn bps(&self) -> u64 {
        self.bps
    }

    /// Returns the assumed merge-set size limit.
    #[must_use]
    pub const fn mergeset_size_limit(&self) -> u64 {
        self.mergeset_size_limit
    }

    /// Returns the assumed anticone finalization depth.
    #[must_use]
    pub const fn anticone_finalization_depth(&self) -> u64 {
        self.anticone_finalization_depth
    }

    /// Returns KGI's derived maximum Catchup DAA-score gap.
    #[must_use]
    pub const fn catchup_max_daa_gap(&self) -> u64 {
        self.catchup_max_daa_gap
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParameterSource {
    NetworkDefaults,
    TestnetFamilyDefaults,
    OverrideFile,
}

impl ParameterSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NetworkDefaults => "network-defaults",
            Self::TestnetFamilyDefaults => "testnet-family-defaults",
            Self::OverrideFile => "override-file",
        }
    }
}

fn resolve_upstream_params(
    network_id: NetworkId,
    override_params_file: Option<&Path>,
) -> Result<(Params, ParameterSource), ConsensusResolutionError> {
    match network_id.network_type() {
        NetworkType::Mainnet => {
            reject_override(network_id, override_params_file)?;
            Ok((Params::from(network_id), ParameterSource::NetworkDefaults))
        }
        NetworkType::Testnet => {
            reject_override(network_id, override_params_file)?;
            if network_id.suffix() == Some(10) {
                Ok((Params::from(network_id), ParameterSource::NetworkDefaults))
            } else {
                Ok((Params::from(NetworkType::Testnet), ParameterSource::TestnetFamilyDefaults))
            }
        }
        NetworkType::Devnet | NetworkType::Simnet => {
            let defaults = Params::from(network_id);
            let Some(path) = override_params_file else {
                return Ok((defaults, ParameterSource::NetworkDefaults));
            };
            let source = fs::read_to_string(path)
                .map_err(|source| ConsensusResolutionError::ReadOverride { path: path.to_path_buf(), source })?;
            let overrides = serde_json::from_str::<OverrideParams>(&source)
                .map_err(|source| ConsensusResolutionError::ParseOverride { path: path.to_path_buf(), source })?;
            Ok((defaults.override_params(overrides), ParameterSource::OverrideFile))
        }
    }
}

fn reject_override(network_id: NetworkId, override_params_file: Option<&Path>) -> Result<(), ConsensusResolutionError> {
    if override_params_file.is_some() { Err(ConsensusResolutionError::OverrideUnsupported { network_id }) } else { Ok(()) }
}

fn require_minimum(parameter: ConsensusParameter, value: u64, minimum: u64) -> Result<(), InvalidConsensusParameters> {
    if value < minimum {
        Err(InvalidConsensusParameters::new(
            parameter,
            ConsensusParameterReason::BelowMinimum,
            format!("value={value}, minimum={minimum}"),
        ))
    } else {
        Ok(())
    }
}

fn require_maximum(parameter: ConsensusParameter, value: u64, maximum: u64) -> Result<(), InvalidConsensusParameters> {
    if value > maximum {
        Err(InvalidConsensusParameters::new(
            parameter,
            ConsensusParameterReason::AboveMaximum,
            format!("value={value}, maximum={maximum}"),
        ))
    } else {
        Ok(())
    }
}

fn checked_add(parameter: ConsensusParameter, left: u64, right: u64) -> Result<u64, InvalidConsensusParameters> {
    left.checked_add(right).ok_or_else(|| {
        InvalidConsensusParameters::new(
            parameter,
            ConsensusParameterReason::ArithmeticOverflow,
            format!("addition operands={left},{right}"),
        )
    })
}

fn checked_mul(parameter: ConsensusParameter, left: u64, right: u64) -> Result<u64, InvalidConsensusParameters> {
    left.checked_mul(right).ok_or_else(|| {
        InvalidConsensusParameters::new(
            parameter,
            ConsensusParameterReason::ArithmeticOverflow,
            format!("multiplication operands={left},{right}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::Path,
        sync::atomic::{AtomicU64, Ordering},
    };

    use kaspa_consensus_core::{
        config::params::Params,
        network::{NetworkId, NetworkType},
    };
    use kgi_model::block::{MAX_BLUE_SCORE, MAX_DAA_SCORE};

    use super::{KgiConsensusParams, ParameterSource, resolve_upstream_params};
    use crate::error::{ConsensusParameter, ConsensusParameterReason, ConsensusResolutionError};

    static TEMP_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn resolves_mainnet_values_without_override() {
        let params = KgiConsensusParams::resolve(NetworkId::new(NetworkType::Mainnet), None).expect("mainnet parameters");

        assert_eq!(params.bps(), 10);
        assert_eq!(params.mergeset_size_limit(), 248);
        assert_eq!(params.anticone_finalization_depth(), 591_258);
        assert_eq!(params.catchup_max_daa_gap(), 300);
    }

    #[test]
    fn unsupported_testnet_uses_family_defaults_without_panicking() {
        let network_id = NetworkId::with_suffix(NetworkType::Testnet, 99);
        let (params, source) = resolve_upstream_params(network_id, None).expect("fallback");

        assert_eq!(source, ParameterSource::TestnetFamilyDefaults);
        assert_eq!(KgiConsensusParams::from_upstream(&params).expect("valid").bps(), 10);
    }

    #[test]
    fn rejects_override_for_mainnet_and_testnet() {
        let path = Path::new("override.json");
        for network_id in [NetworkId::new(NetworkType::Mainnet), NetworkId::with_suffix(NetworkType::Testnet, 10)] {
            assert!(matches!(
                KgiConsensusParams::resolve(network_id, Some(path)),
                Err(ConsensusResolutionError::OverrideUnsupported { .. })
            ));
        }
    }

    #[test]
    fn applies_devnet_override_file() {
        let path = temp_path();
        let blockrate = {
            let mut blockrate = Params::from(NetworkType::Devnet).blockrate;
            blockrate.target_time_per_block = 20;
            blockrate.mergeset_size_limit = 512;
            blockrate
        };
        let value = serde_json::json!({ "blockrate": blockrate });
        fs::write(&path, serde_json::to_vec(&value).expect("serialize JSON")).expect("write override");

        let params = KgiConsensusParams::resolve(NetworkId::new(NetworkType::Devnet), Some(&path)).expect("override parameters");
        fs::remove_file(path).expect("remove override");

        assert_eq!(params.bps(), 50);
        assert_eq!(params.mergeset_size_limit(), 512);
        assert_eq!(params.catchup_max_daa_gap(), 1_500);
    }

    #[test]
    fn reports_unreadable_and_malformed_override_files() {
        let missing = temp_path();
        assert!(matches!(
            KgiConsensusParams::resolve(NetworkId::new(NetworkType::Simnet), Some(&missing)),
            Err(ConsensusResolutionError::ReadOverride { .. })
        ));

        let malformed = temp_path();
        fs::write(&malformed, b"{not-json").expect("write malformed override");
        assert!(matches!(
            KgiConsensusParams::resolve(NetworkId::new(NetworkType::Simnet), Some(&malformed)),
            Err(ConsensusResolutionError::ParseOverride { .. })
        ));
        fs::remove_file(malformed).expect("remove malformed override");

        let incompatible = temp_path();
        fs::write(&incompatible, br#"{"unknown_parameter": 1}"#).expect("write incompatible override");
        assert!(matches!(
            KgiConsensusParams::resolve(NetworkId::new(NetworkType::Devnet), Some(&incompatible)),
            Err(ConsensusResolutionError::ParseOverride { .. })
        ));
        fs::remove_file(incompatible).expect("remove incompatible override");
    }

    #[test]
    fn validates_raw_bounds_before_derived_methods() {
        let cases = [
            (0, 248, ConsensusParameter::TargetTimePerBlock, ConsensusParameterReason::BelowMinimum),
            (1_001, 248, ConsensusParameter::TargetTimePerBlock, ConsensusParameterReason::AboveMaximum),
            (100, 1, ConsensusParameter::MergeSetSizeLimit, ConsensusParameterReason::BelowMinimum),
        ];

        for (target, limit, parameter, reason) in cases {
            let mut params = Params::from(NetworkType::Devnet);
            params.blockrate.target_time_per_block = target;
            params.blockrate.mergeset_size_limit = limit;
            let error = KgiConsensusParams::from_upstream(&params).expect_err("invalid parameters");
            assert_eq!((error.parameter, error.reason), (parameter, reason));
        }
    }

    #[test]
    fn accepts_target_time_endpoints_and_minimum_merge_set_limit() {
        for target_time_per_block in [1, 1_000] {
            let mut params = Params::from(NetworkType::Devnet);
            params.blockrate.target_time_per_block = target_time_per_block;
            params.blockrate.mergeset_size_limit = 2;

            let resolved = KgiConsensusParams::from_upstream(&params).expect("boundary parameters");
            assert_eq!(resolved.bps(), 1_000 / target_time_per_block);
            assert_eq!(resolved.mergeset_size_limit(), 2);
        }
    }

    #[test]
    fn validates_checked_derived_arithmetic() {
        let mut core_budget = Params::from(NetworkType::Devnet);
        core_budget.blockrate.mergeset_size_limit = u64::MAX;
        assert_invalid(&core_budget, ConsensusParameter::GetBlocksCoreBudget, ConsensusParameterReason::ArithmeticOverflow);

        let mut vspc = Params::from(NetworkType::Devnet);
        vspc.blockrate.mergeset_size_limit = u64::MAX / 10 + 1;
        assert_invalid(&vspc, ConsensusParameter::VspcV2AddedBatchSize, ConsensusParameterReason::ArithmeticOverflow);

        let mut anticone = Params::from(NetworkType::Devnet);
        anticone.blockrate.finality_depth = u64::MAX;
        assert_invalid(&anticone, ConsensusParameter::AnticoneFinalizationDepth, ConsensusParameterReason::ArithmeticOverflow);
    }

    #[test]
    fn rejects_derived_depth_above_domain_maximum() {
        let mut params = Params::from(NetworkType::Devnet);
        params.blockrate.finality_depth = MAX_BLUE_SCORE + 1;
        params.blockrate.merge_depth = 0;
        params.blockrate.mergeset_size_limit = 2;
        params.blockrate.ghostdag_k = 0;
        params.blockrate.pruning_depth = u64::MAX;

        assert_invalid(&params, ConsensusParameter::AnticoneFinalizationDepth, ConsensusParameterReason::AboveMaximum);
    }

    #[test]
    fn accepted_values_fit_domain_ranges() {
        let params = KgiConsensusParams::resolve(NetworkId::new(NetworkType::Simnet), None).expect("simnet parameters");
        assert!(params.anticone_finalization_depth() <= MAX_BLUE_SCORE);
        assert!(params.catchup_max_daa_gap() <= MAX_DAA_SCORE);
    }

    fn assert_invalid(params: &Params, parameter: ConsensusParameter, reason: ConsensusParameterReason) {
        let error = KgiConsensusParams::from_upstream(params).expect_err("invalid parameters");
        assert_eq!((error.parameter, error.reason), (parameter, reason));
    }

    fn temp_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("kgi-node-override-{}-{}.json", std::process::id(), TEMP_ID.fetch_add(1, Ordering::Relaxed)))
    }
}
