use std::collections::HashMap;

use kgi_model::block::BlockHash;

use super::scalar::{BlockHashDto, HashRef};

/// One response-local first-occurrence hash dictionary under construction.
#[derive(Default)]
pub(super) struct HashDictionary {
    hashes: Vec<BlockHashDto>,
    references: HashMap<BlockHash, HashRef>,
}

impl HashDictionary {
    /// Interns one hash and returns its response-local reference.
    pub(super) fn intern(&mut self, hash: BlockHash) -> HashRef {
        if let Some(&reference) = self.references.get(&hash) {
            return reference;
        }

        let reference =
            HashRef::try_from(self.hashes.len()).expect("one encoded graph response contains at most u32::MAX distinct block hashes");
        self.hashes.push(hash.into());
        self.references.insert(hash, reference);
        reference
    }

    /// Finishes the dictionary in first-occurrence order.
    pub(super) fn into_hashes(self) -> Vec<BlockHashDto> {
        self.hashes
    }
}
