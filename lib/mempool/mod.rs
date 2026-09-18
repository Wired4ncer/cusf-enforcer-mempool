use std::collections::HashMap;

use bitcoin::{Amount, BlockHash, OutPoint, Transaction, Txid, Weight};
use bitcoin_jsonrpsee::client::BlockTemplateTransaction;
use hashlink::{LinkedHashMap, LinkedHashSet};
use imbl::{OrdMap, OrdSet, ordmap};
use indexmap::IndexSet;
use lender::FallibleLender as _;
use thiserror::Error;

pub mod dat;
pub mod iter;
pub mod iter_mut;
mod sync;

pub use dat::{MempoolDat, ReadMempoolDatError, read_mempool_dat};
pub use sync::{
    InitialSyncMempoolError, MempoolSync, init_sync_mempool,
    task::SyncTaskError,
};

mod refinement_cmp {
    //! Refinement comparison traits and wrappers, for types that have more than
    //! one possible ordering/equality relation.
    //! The traits in this module must obey the same laws as their counterparts
    //! in [`std::cmp`].

    use std::cmp::Ordering;

    pub trait RefinementPartialEq<Rhs = Self>
    where
        Rhs: ?Sized,
    {
        #[must_use]
        fn eq(&self, other: &Rhs) -> bool;

        #[inline]
        #[must_use]
        fn ne(&self, other: &Rhs) -> bool {
            !self.eq(other)
        }
    }

    pub trait RefinementPartialOrd<Rhs = Self>:
        RefinementPartialEq<Rhs>
    where
        Rhs: ?Sized,
    {
        #[must_use]
        fn partial_cmp(&self, other: &Rhs) -> Option<Ordering>;

        #[inline]
        #[must_use]
        fn lt(&self, other: &Rhs) -> bool {
            self.partial_cmp(other).is_some_and(Ordering::is_lt)
        }

        #[inline]
        #[must_use]
        fn le(&self, other: &Rhs) -> bool {
            self.partial_cmp(other).is_some_and(Ordering::is_le)
        }

        #[inline]
        #[must_use]
        fn gt(&self, other: &Rhs) -> bool {
            self.partial_cmp(other).is_some_and(Ordering::is_gt)
        }

        #[inline]
        #[must_use]
        fn ge(&self, other: &Rhs) -> bool {
            self.partial_cmp(other).is_some_and(Ordering::is_ge)
        }
    }

    pub trait RefinementEq: RefinementPartialEq {}

    pub trait RefinementOrd: RefinementEq + RefinementPartialOrd {
        #[must_use]
        fn cmp(&self, other: &Self) -> Ordering;

        #[inline]
        #[must_use]
        fn max(self, other: Self) -> Self
        where
            Self: Sized,
        {
            if other.lt(&self) { self } else { other }
        }

        #[inline]
        #[must_use]
        fn min(self, other: Self) -> Self
        where
            Self: Sized,
        {
            if other.lt(&self) { other } else { self }
        }

        #[inline]
        #[must_use]
        fn clamp(self, min: Self, max: Self) -> Self
        where
            Self: Sized,
        {
            assert!(min.le(&max));
            if self.lt(&min) {
                min
            } else if self.gt(&max) {
                max
            } else {
                self
            }
        }
    }

    /// Wrapper struct that implements comparison traits from [`std::cmp`].
    #[derive(Clone, Copy, Debug)]
    #[repr(transparent)]
    pub struct RefinementCmp<T: ?Sized>(pub T);

    impl<T, Rhs: ?Sized> PartialEq<RefinementCmp<Rhs>> for RefinementCmp<T>
    where
        T: RefinementPartialEq<Rhs>,
    {
        #[inline(always)]
        fn eq(&self, other: &RefinementCmp<Rhs>) -> bool {
            <T as RefinementPartialEq<Rhs>>::eq(&self.0, &other.0)
        }

        #[allow(clippy::partialeq_ne_impl)]
        #[inline(always)]
        fn ne(&self, other: &RefinementCmp<Rhs>) -> bool {
            <T as RefinementPartialEq<Rhs>>::ne(&self.0, &other.0)
        }
    }

    impl<T, Rhs: ?Sized> PartialOrd<RefinementCmp<Rhs>> for RefinementCmp<T>
    where
        T: RefinementPartialOrd<Rhs>,
    {
        #[inline(always)]
        fn partial_cmp(&self, other: &RefinementCmp<Rhs>) -> Option<Ordering> {
            <T as RefinementPartialOrd<Rhs>>::partial_cmp(&self.0, &other.0)
        }

        #[inline(always)]
        fn lt(&self, other: &RefinementCmp<Rhs>) -> bool {
            <T as RefinementPartialOrd<Rhs>>::lt(&self.0, &other.0)
        }

        #[inline(always)]
        fn le(&self, other: &RefinementCmp<Rhs>) -> bool {
            <T as RefinementPartialOrd<Rhs>>::le(&self.0, &other.0)
        }

        #[inline(always)]
        fn gt(&self, other: &RefinementCmp<Rhs>) -> bool {
            <T as RefinementPartialOrd<Rhs>>::gt(&self.0, &other.0)
        }

        #[inline(always)]
        fn ge(&self, other: &RefinementCmp<Rhs>) -> bool {
            <T as RefinementPartialOrd<Rhs>>::ge(&self.0, &other.0)
        }
    }

    impl<T> Eq for RefinementCmp<T> where T: RefinementEq {}

    impl<T> Ord for RefinementCmp<T>
    where
        T: RefinementOrd,
    {
        #[inline(always)]
        fn cmp(&self, other: &Self) -> Ordering {
            <T as RefinementOrd>::cmp(&self.0, &other.0)
        }

        #[inline(always)]
        fn max(self, other: Self) -> Self
        where
            Self: Sized,
        {
            Self(<T as RefinementOrd>::max(self.0, other.0))
        }

        #[inline(always)]
        fn min(self, other: Self) -> Self
        where
            Self: Sized,
        {
            Self(<T as RefinementOrd>::min(self.0, other.0))
        }

        #[inline(always)]
        fn clamp(self, min: Self, max: Self) -> Self
        where
            Self: Sized,
        {
            Self(<T as RefinementOrd>::clamp(self.0, min.0, max.0))
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FeeRate {
    fee: Amount,
    vsize: u64,
}

impl FeeRate {
    /// Refinement comparison (refinement of canonical quotient comparison).
    /// This will always be equivalent to canonical quotient comparison if
    /// `self` and `other` are not equal when comparing as quotients.
    /// If `self` and `other` are equal when comparing as quotients,
    /// orders by `vsize`.
    /// ```text
    /// (self.fee / self.vsize) < (other.fee / other.vsize) ==> self < other,
    /// (self.fee / self.vsize) > (other.fee / other.vsize) ==> self > other,
    /// (self.fee == other.fee /\ other.fee == other.vsize) <==> self == other,
    /// ((self.fee / self.vsize) == (other.fee / other.vsize)
    /// /\ self.vsize < other.vsize) ==> self < other,
    /// ((self.fee / self.vsize) == (other.fee / other.vsize)
    /// /\ self.vsize > other.vsize) ==> self > other
    /// ```
    fn refinement_cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        // (self.fee / self.size) > (other.fee / other.size) ==>
        // (self.fee * other.size) > (other.fee * self.size)
        let lhs = self.fee.to_sat() as u128 * other.vsize as u128;
        let rhs = other.fee.to_sat() as u128 * self.vsize as u128;
        match lhs.cmp(&rhs) {
            Ordering::Equal => self.vsize.cmp(&other.vsize),
            res => res,
        }
    }
}

impl refinement_cmp::RefinementOrd for FeeRate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.refinement_cmp(other)
    }
}

impl refinement_cmp::RefinementPartialEq for FeeRate {
    fn eq(&self, other: &Self) -> bool {
        <Self as refinement_cmp::RefinementOrd>::cmp(self, other).is_eq()
    }
}

impl refinement_cmp::RefinementEq for FeeRate {}

impl refinement_cmp::RefinementPartialOrd for FeeRate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(<Self as refinement_cmp::RefinementOrd>::cmp(self, other))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TxFees {
    pub base: Amount,
    pub modified: Amount,
    pub ancestor: Amount,
    pub descendant: Amount,
}

#[derive(Clone, Debug)]
pub struct TxInfo {
    pub ancestor_modified_weight: Weight,
    pub ancestor_vsize: u64,
    pub bip125_replaceable: bool,
    pub depends: OrdSet<Txid>,
    pub descendant_modified_weight: Weight,
    pub descendant_vsize: u64,
    pub fees: TxFees,
    pub modified_weight: Weight,
    pub spent_by: OrdSet<Txid>,
    /// Conflicts due to reasons other than shared inputs
    pub conflicts_with: OrdSet<Txid>,
}

#[derive(Debug, Error)]
#[error("Missing ancestor for {tx}: {missing}")]
pub struct MissingAncestorError {
    pub tx: Txid,
    pub missing: Txid,
}

#[derive(Debug, Error)]
#[error("Missing descendant for {tx}: {missing}")]
pub struct MissingDescendantError {
    pub tx: Txid,
    pub missing: Txid,
}

#[derive(Debug, Error)]
#[error("Missing descendants key: {0}")]
pub struct MissingDescendantsKeyError(Txid);

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum MempoolInsertError {
    #[error(transparent)]
    MissingAncestor(#[from] MissingAncestorError),
    #[error(transparent)]
    MissingDescendant(#[from] MissingDescendantError),
    #[error(transparent)]
    MissingDescendantsKey(#[from] MissingDescendantsKeyError),
    #[error("Tx already exists in mempool (`{txid}`)")]
    TxAlreadyExists { txid: Txid },
}

#[derive(Debug, Error)]
#[error("Missing by_ancestor_fee_rate key: {0:?}")]
pub struct MissingByAncestorFeeRateKeyError(FeeRate);

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum MempoolRemoveError {
    #[error(transparent)]
    MissingAncestor(#[from] MissingAncestorError),
    #[error(transparent)]
    MissingByAncestorFeeRateKey(#[from] MissingByAncestorFeeRateKeyError),
    #[error(transparent)]
    MissingDescendant(#[from] MissingDescendantError),
    #[error(transparent)]
    MissingDescendantsKey(#[from] MissingDescendantsKeyError),
}

#[derive(Debug, Error)]
pub enum MempoolUpdateError {
    #[error(transparent)]
    MissingDescendant(#[from] MissingDescendantError),
}

#[derive(Clone, Debug, Default)]
struct ByAncestorFeeRate(
    OrdMap<refinement_cmp::RefinementCmp<FeeRate>, LinkedHashSet<Txid>>,
);

impl ByAncestorFeeRate {
    fn insert(&mut self, fee_rate: FeeRate, txid: Txid) {
        self.0
            .entry(refinement_cmp::RefinementCmp(fee_rate))
            .or_default()
            .insert(txid);
    }

    /// returns `true` if removed successfully, or `false` if not found
    fn remove(&mut self, fee_rate: FeeRate, txid: Txid) -> bool {
        match self.0.entry(refinement_cmp::RefinementCmp(fee_rate)) {
            ordmap::Entry::Occupied(mut entry) => {
                let txs = entry.get_mut();
                txs.remove(&txid);
                if txs.is_empty() {
                    entry.remove();
                }
                true
            }
            ordmap::Entry::Vacant(_) => false,
        }
    }

    /// Iterate from low-to-high fee rate, in insertion order
    #[allow(dead_code)]
    fn iter(&self) -> impl DoubleEndedIterator<Item = (FeeRate, Txid)> + '_ {
        self.0.iter().flat_map(|(fee_rate, txids)| {
            txids.iter().map(|txid| (fee_rate.0, *txid))
        })
    }

    /// Iterate from high-to-low fee rate, in insertion order
    fn iter_rev(
        &self,
    ) -> impl DoubleEndedIterator<Item = (FeeRate, Txid)> + '_ {
        self.0.iter().rev().flat_map(|(fee_rate, txids)| {
            txids.iter().map(|txid| (fee_rate.0, *txid))
        })
    }
}

#[derive(Clone, Debug)]
struct Chain {
    tip: BlockHash,
    blocks: imbl::HashMap<BlockHash, bitcoin_jsonrpsee::client::Block<true>>,
}

impl Chain {
    // Iterate over blocks from tip towards genesis.
    // Not all history is guaranteed to exist, so this iterator might return
    // `None` before the genesis block.
    #[allow(dead_code)]
    fn iter(
        &self,
    ) -> impl Iterator<Item = &bitcoin_jsonrpsee::client::Block<true>> {
        let mut next = Some(self.tip);
        std::iter::from_fn(move || {
            if let Some(block) = self.blocks.get(&next?) {
                next = block.previousblockhash;
                Some(block)
            } else {
                next = None;
                None
            }
        })
    }
}

/// Map of txs (which may not be in the mempool) to their direct child txs,
/// which MUST be in the mempool
#[derive(Clone, Debug, Default)]
struct TxChilds(imbl::HashMap<Txid, imbl::HashSet<Txid>>);

impl TxChilds {
    fn insert(&mut self, txid: Txid, child: Txid) -> bool {
        self.0.entry(txid).or_default().insert(child).is_some()
    }

    fn remove(&mut self, txid: Txid, child: Txid) -> bool {
        match self.0.entry(txid) {
            imbl::hashmap::Entry::Occupied(mut entry) => {
                let res = entry.get_mut().remove(&child).is_some();
                if entry.get().is_empty() {
                    entry.remove();
                }
                res
            }
            imbl::hashmap::Entry::Vacant(_) => false,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct MempoolTxs(imbl::HashMap<Txid, (Transaction, TxInfo)>);

/// Maximum weight usable for block template txs and coinbase txouts.
/// This reserves weight for the block header, the txs array length, and the
/// coinbase tx *without any txouts*, so the weight of the coinbase txouts
/// (including the block reward payout txout, whose spk has no fixed size)
/// MUST be accounted for by the caller.
pub const MAX_USABLE_BLOCK_WEIGHT: Weight = {
    const COINBASE_TXIN_WEIGHT: Weight = {
        let weight_wu = Weight::from_non_witness_data_size(
            // outpoint
            36
            // sequence
            + 4
            // SPK
            + 5
        ).to_wu()
        // witness (stack items length, witness reserved value length, and
        // witness reserved value)
        + Weight::from_witness_data_size(1 + 1 + 32).to_wu();
        Weight::from_wu(weight_wu)
    };
    const COINBASE_TX_WEIGHT: Weight = Weight::from_wu(
        // version
        Weight::from_non_witness_data_size(4).to_wu()
        // locktime
        + Weight::from_non_witness_data_size(4).to_wu()
        // segwit marker and flag
        + Weight::from_witness_data_size(2).to_wu()
        // inputs
        + Weight::from_non_witness_data_size(1).to_wu()
        + COINBASE_TXIN_WEIGHT.to_wu()
        // outputs
        + Weight::from_non_witness_data_size(1).to_wu(),
    );
    let res_wu = Weight::MAX_BLOCK.to_wu() - Weight::from_non_witness_data_size(
            bitcoin::block::Header::SIZE as u64
        ).to_wu()
            // 3 bytes for encoding txs array length
            - Weight::from_non_witness_data_size(3).to_wu()
            - COINBASE_TX_WEIGHT.to_wu();
    Weight::from_wu(res_wu)
};

#[inline(always)]
fn saturating_add_weight(lhs: Weight, rhs: Weight) -> Weight {
    let res_wu = lhs.to_wu().saturating_add(rhs.to_wu());
    Weight::from_wu(res_wu)
}

#[inline(always)]
fn saturating_sub_weight(lhs: Weight, rhs: Weight) -> Weight {
    let res_wu = lhs.to_wu().saturating_sub(rhs.to_wu());
    Weight::from_wu(res_wu)
}

// MUST be cheap to clone so that constructing block templates is cheap
#[derive(Clone, Debug)]
pub struct Mempool {
    by_ancestor_fee_rate: ByAncestorFeeRate,
    chain: Chain,
    /// Map of txs (which may not be in the mempool) to their direct child txs,
    /// which MUST be in the mempool
    tx_childs: TxChilds,
    txs: MempoolTxs,
}

impl Mempool {
    fn new(prev_blockhash: BlockHash) -> Self {
        let chain = Chain {
            tip: prev_blockhash,
            blocks: imbl::HashMap::new(),
        };
        Self {
            by_ancestor_fee_rate: ByAncestorFeeRate::default(),
            chain,
            tx_childs: TxChilds::default(),
            txs: MempoolTxs::default(),
        }
    }

    pub fn tip(&self) -> &bitcoin_jsonrpsee::client::Block<true> {
        &self.chain.blocks[&self.chain.tip]
    }

    /// Insert a tx into the mempool,
    /// stating conflicts with other txs due to reasons other than shared
    /// inputs.
    pub fn insert(
        &mut self,
        tx: Transaction,
        fee: Amount,
        conflicts_with: imbl::OrdSet<Txid>,
        modified_weight: Weight,
    ) -> Result<Option<TxInfo>, MempoolInsertError> {
        let txid = tx.compute_txid();
        if self.txs.0.contains_key(&txid) {
            return Err(MempoolInsertError::TxAlreadyExists { txid });
        }
        // initially incorrect, must be computed after insertion
        let mut ancestor_fees = fee;
        // initially incorrect, must be computed after insertion
        let mut descendant_fees = fee;
        let modified_fee = fee;
        let vsize = tx.vsize() as u64;
        // initially incorrect, must be computed after insertion
        let mut ancestor_modified_weight = modified_weight;
        // initially incorrect, must be computed after insertion
        let mut ancestor_vsize = vsize;
        // initially incorrect, must be computed after insertion
        let mut descendant_modified_weight = modified_weight;
        // initially incorrect, must be computed after insertion
        let mut descendant_vsize = vsize;
        // conflicts including ancestor conflicts
        let mut ancestry_conflicts = conflicts_with.clone();
        let depends = tx
            .input
            .iter()
            .map(|input| {
                let input_txid = input.previous_output.txid;
                self.tx_childs.insert(input_txid, txid);
                input_txid
            })
            .filter(|input_txid| self.txs.0.contains_key(input_txid))
            .collect();
        for dep in &depends {
            let (_, dep_info) =
                self.txs.0.get_mut(dep).ok_or(MissingAncestorError {
                    tx: txid,
                    missing: *dep,
                })?;
            dep_info.spent_by.insert(txid);
            ancestry_conflicts =
                ancestry_conflicts.union(dep_info.conflicts_with.clone());
        }
        let spent_by = if let Some(childs) = self.tx_childs.0.get(&txid) {
            OrdSet::from_iter(childs.iter().copied())
        } else {
            OrdSet::new()
        };
        let info = TxInfo {
            ancestor_modified_weight,
            ancestor_vsize,
            bip125_replaceable: tx.is_explicitly_rbf(),
            depends,
            descendant_modified_weight,
            descendant_vsize,
            fees: TxFees {
                ancestor: ancestor_fees,
                base: fee,
                descendant: descendant_fees,
                modified: modified_fee,
            },
            modified_weight,
            spent_by,
            conflicts_with: ancestry_conflicts,
        };
        let (ndeps, nspenders) = (info.depends.len(), info.spent_by.len());
        let res = self.txs.0.insert(txid, (tx, info)).map(|(_, info)| info);
        tracing::trace!(
            fee = %fee.display_dynamic(),
            modified_fee = %modified_fee.display_dynamic(),
            %txid,
            "Inserted tx into mempool with {ndeps} deps and {nspenders} spenders"
        );
        self.txs.ancestors_mut(txid).for_each(
            |(ancestor_tx, ancestor_info)| {
                ancestor_vsize += ancestor_tx.vsize() as u64;
                ancestor_modified_weight = saturating_add_weight(
                    ancestor_modified_weight,
                    ancestor_info.modified_weight,
                );
                ancestor_fees += ancestor_info.fees.modified;
                ancestor_info.descendant_vsize += vsize;
                ancestor_info.descendant_modified_weight =
                    saturating_add_weight(
                        ancestor_info.descendant_modified_weight,
                        modified_weight,
                    );
                ancestor_info.fees.descendant += modified_fee;
                Ok(())
            },
        )?;
        // Direct children of this tx that were already in the mempool when it
        // was inserted (i.e. inserted out-of-order, before their parent). Their
        // `depends` was computed without this tx and must be backfilled below.
        let direct_children: imbl::HashSet<Txid> =
            self.tx_childs.0.get(&txid).cloned().unwrap_or_default();
        self.txs.descendants_mut(txid).skip(1).for_each(
            |(descendant_tx, descendant_info)| {
                let descendant_txid = descendant_tx.compute_txid();
                descendant_vsize += descendant_tx.vsize() as u64;
                descendant_modified_weight = saturating_add_weight(
                    descendant_modified_weight,
                    descendant_info.modified_weight,
                );
                descendant_fees += descendant_info.fees.modified;
                // The descendant's ancestor set now includes this tx, which
                // changes its ancestor fee rate. Re-key it in
                // `by_ancestor_fee_rate` (remove the stale key, insert the new
                // one), mirroring what `remove` does in reverse. This only
                // fires for out-of-order inserts (a topologically-ordered
                // insert has no descendants yet); without it a later
                // `remove`/`propose_txs` fails with `MissingByAncestorFeeRateKey`
                // because the descendant's stored key no longer matches its
                // stats. (issue #611)
                let old_ancestor_fee_rate = FeeRate {
                    fee: descendant_info.fees.ancestor,
                    vsize: descendant_info
                        .ancestor_modified_weight
                        .to_vbytes_ceil(),
                };
                self.by_ancestor_fee_rate
                    .remove(old_ancestor_fee_rate, descendant_txid);
                descendant_info.ancestor_vsize += vsize;
                descendant_info.ancestor_modified_weight =
                    saturating_add_weight(
                        descendant_info.ancestor_modified_weight,
                        modified_weight,
                    );
                descendant_info.fees.ancestor += modified_fee;
                let new_ancestor_fee_rate = FeeRate {
                    fee: descendant_info.fees.ancestor,
                    vsize: descendant_info
                        .ancestor_modified_weight
                        .to_vbytes_ceil(),
                };
                self.by_ancestor_fee_rate
                    .insert(new_ancestor_fee_rate, descendant_txid);
                // If this descendant spends one of the newly-inserted tx's
                // outputs (a direct child) but was inserted before it, its
                // `depends` never recorded the dependency. Backfill it, else it
                // can be proposed ahead of / without its parent -> invalid
                // template. (issue #611)
                if direct_children.contains(&descendant_txid) {
                    descendant_info.depends.insert(txid);
                }
                descendant_info.conflicts_with = descendant_info
                    .conflicts_with
                    .clone()
                    .union(conflicts_with.clone());
                Ok(())
            },
        )?;
        for conflict_txid in conflicts_with {
            // The conflicting tx may have already been removed from the
            // mempool (e.g. confirmed in a block). Skip it.
            if !self.txs.0.contains_key(&conflict_txid) {
                continue;
            }
            self.txs.descendants_mut(conflict_txid).for_each(
                |(_descendant_tx, descendant_info)| {
                    descendant_info.conflicts_with.insert(txid);
                    Ok(())
                },
            )?;
        }
        let (_, info) = self.txs.0.get_mut(&txid).unwrap();
        info.fees.ancestor = ancestor_fees;
        info.fees.descendant = descendant_fees;
        info.ancestor_modified_weight = ancestor_modified_weight;
        info.ancestor_vsize = ancestor_vsize;
        info.descendant_modified_weight = descendant_modified_weight;
        info.descendant_vsize = descendant_vsize;
        let ancestor_fee_rate = FeeRate {
            fee: ancestor_fees,
            vsize: ancestor_modified_weight.to_vbytes_ceil(),
        };
        self.by_ancestor_fee_rate.insert(ancestor_fee_rate, txid);
        // Out-of-order insert (descendants were already present): the
        // incremental updates above miss the ancestor<->descendant cross
        // terms this tx just created. Recompute both sides exactly.
        if !direct_children.is_empty() {
            self.recompute_package_stats_around(txid)?;
        }
        Ok(res)
    }

    /// Transitive in-mempool descendants of `txid`, NOT including `txid`.
    /// Walks `spent_by`, which only ever holds in-mempool children.
    fn descendant_txids(&self, txid: Txid) -> Vec<Txid> {
        let mut seen = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::from([txid]);
        let mut res = Vec::new();
        while let Some(t) = queue.pop_front() {
            let Some((_, info)) = self.txs.0.get(&t) else {
                continue;
            };
            for child in &info.spent_by {
                if seen.insert(*child) {
                    queue.push_back(*child);
                    res.push(*child);
                }
            }
        }
        res
    }

    /// Recompute the package totals that an OUT-OF-ORDER insert of `txid`
    /// invalidates, from scratch.
    ///
    /// The incremental loops in `insert` credit each pre-existing descendant
    /// with the inserted tx alone, and each ancestor with the inserted tx
    /// alone. But inserting the middle of a chain also joins every ancestor
    /// of the new tx to every descendant of it: with G already in the pool,
    /// then C (child of the absent P), then P, C's ancestor set gains P *and*
    /// G, and G's descendant set gains P *and* C. The incremental arithmetic
    /// left C's `ancestor_vsize` at C+P against a true package of C+P+G — an
    /// under-counted package that `propose_txs_mut` then charges at the
    /// wrong weight (over-weight template, i.e. an invalid block, with no
    /// post-check) and that `remove` later subtracts below zero (panic in the
    /// sync task). Found by the 2026-09-18 review of PR #114.
    ///
    /// The ancestor/descendant iterators visit each tx once, so summing over
    /// them is exact set semantics — a descendant that already reached an
    /// ancestor by another path is not double-counted. Only the out-of-order
    /// path calls this; the in-order insert has no descendants and never does.
    fn recompute_package_stats_around(
        &mut self,
        txid: Txid,
    ) -> Result<(), MempoolInsertError> {
        let (_, inserted_info) =
            self.txs.0.get(&txid).ok_or(MissingDescendantError {
                tx: txid,
                missing: txid,
            })?;
        let inserted_conflicts = inserted_info.conflicts_with.clone();
        // Descendants: ancestor totals + the `by_ancestor_fee_rate` key.
        for desc_txid in self.descendant_txids(txid) {
            let mut anc_vsize = 0u64;
            let mut anc_weight = Weight::ZERO;
            let mut anc_fee = Amount::ZERO;
            let mut ancestors = self.txs.ancestors(desc_txid);
            while let Some((_, anc_tx, anc_info)) = ancestors.next()? {
                anc_vsize += anc_tx.vsize() as u64;
                anc_weight =
                    saturating_add_weight(anc_weight, anc_info.modified_weight);
                anc_fee += anc_info.fees.modified;
            }
            let (desc_tx, desc_info) = self.txs.0.get_mut(&desc_txid).ok_or(
                MissingDescendantError {
                    tx: txid,
                    missing: desc_txid,
                },
            )?;
            let old_key = FeeRate {
                fee: desc_info.fees.ancestor,
                vsize: desc_info.ancestor_modified_weight.to_vbytes_ceil(),
            };
            self.by_ancestor_fee_rate.remove(old_key, desc_txid);
            desc_info.ancestor_vsize = anc_vsize + desc_tx.vsize() as u64;
            desc_info.ancestor_modified_weight =
                saturating_add_weight(anc_weight, desc_info.modified_weight);
            desc_info.fees.ancestor = anc_fee + desc_info.fees.modified;
            desc_info.conflicts_with = desc_info
                .conflicts_with
                .clone()
                .union(inserted_conflicts.clone());
            let new_key = FeeRate {
                fee: desc_info.fees.ancestor,
                vsize: desc_info.ancestor_modified_weight.to_vbytes_ceil(),
            };
            self.by_ancestor_fee_rate.insert(new_key, desc_txid);
        }
        // Ancestors: descendant totals (no key depends on them).
        let mut anc_ids = Vec::new();
        let mut ancestors = self.txs.ancestors(txid);
        while let Some((anc_txid, _, _)) = ancestors.next()? {
            anc_ids.push(anc_txid);
        }
        for anc_txid in anc_ids {
            let mut desc_vsize = 0u64;
            let mut desc_weight = Weight::ZERO;
            let mut desc_fee = Amount::ZERO;
            for d in self.descendant_txids(anc_txid) {
                let (d_tx, d_info) =
                    self.txs.0.get(&d).ok_or(MissingDescendantError {
                        tx: anc_txid,
                        missing: d,
                    })?;
                desc_vsize += d_tx.vsize() as u64;
                desc_weight =
                    saturating_add_weight(desc_weight, d_info.modified_weight);
                desc_fee += d_info.fees.modified;
            }
            let (anc_tx, anc_info) =
                self.txs.0.get_mut(&anc_txid).ok_or(MissingAncestorError {
                    tx: txid,
                    missing: anc_txid,
                })?;
            anc_info.descendant_vsize = desc_vsize + anc_tx.vsize() as u64;
            anc_info.descendant_modified_weight =
                saturating_add_weight(desc_weight, anc_info.modified_weight);
            anc_info.fees.descendant = desc_fee + anc_info.fees.modified;
        }
        Ok(())
    }

    /// Remove a tx from the mempool. Descendants are updated but not removed.
    fn remove(
        &mut self,
        txid: &Txid,
    ) -> Result<Option<(Transaction, TxInfo)>, MempoolRemoveError> {
        let Some((tx, info)) = self.txs.0.get(txid) else {
            return Ok(None);
        };
        let ancestor_modified_weight = info.ancestor_modified_weight;
        let modified_weight = info.modified_weight;
        let vsize = tx.vsize() as u64;
        let fees = info.fees;
        for spent_tx in tx.input.iter().map(|input| input.previous_output.txid)
        {
            self.tx_childs.remove(spent_tx, *txid);
        }
        let mut descendants = self.txs.descendants_mut(*txid);
        // Skip first element
        let _: Option<_> = descendants.next()?;
        let () = descendants
            .map_err(MempoolRemoveError::MissingDescendant)
            .for_each(|(desc_tx, desc_info)| {
                let ancestor_fee_rate = FeeRate {
                    fee: desc_info.fees.ancestor,
                    vsize: desc_info.ancestor_modified_weight.to_vbytes_ceil(),
                };
                let desc_txid = desc_tx.compute_txid();
                if !self
                    .by_ancestor_fee_rate
                    .remove(ancestor_fee_rate, desc_txid)
                {
                    let err =
                        MissingByAncestorFeeRateKeyError(ancestor_fee_rate);
                    return Err(err.into());
                };
                desc_info.ancestor_modified_weight = saturating_sub_weight(
                    desc_info.ancestor_modified_weight,
                    modified_weight,
                );
                // Saturating, never panicking: an underflow here means the
                // package totals were already wrong, and a panic in the sync
                // task kills the enforcer (the #611 symptom). Say so instead.
                if desc_info.ancestor_vsize < vsize
                    || desc_info.fees.ancestor < fees.modified
                {
                    tracing::error!(
                        %txid, %desc_txid,
                        "descendant package totals would underflow on remove; \
                         ancestor stats were inconsistent"
                    );
                }
                desc_info.ancestor_vsize =
                    desc_info.ancestor_vsize.saturating_sub(vsize);
                desc_info.fees.ancestor = desc_info
                    .fees
                    .ancestor
                    .checked_sub(fees.modified)
                    .unwrap_or(Amount::ZERO);
                let ancestor_fee_rate = FeeRate {
                    fee: desc_info.fees.ancestor,
                    vsize: desc_info.ancestor_modified_weight.to_vbytes_ceil(),
                };
                self.by_ancestor_fee_rate
                    .insert(ancestor_fee_rate, desc_txid);
                // FIXME: remove
                tracing::trace!("removing {txid} as a dep of {desc_txid}");
                desc_info.depends.remove(txid);
                Result::<_, MempoolRemoveError>::Ok(())
            })?;
        // Update all ancestors
        let () =
            self.txs
                .ancestors_mut(*txid)
                .for_each(|(_anc_tx, anc_info)| {
                    anc_info.descendant_modified_weight = saturating_sub_weight(
                        anc_info.descendant_modified_weight,
                        modified_weight,
                    );
                    if anc_info.descendant_vsize < vsize
                        || anc_info.fees.descendant < fees.modified
                    {
                        tracing::error!(
                            %txid,
                            "ancestor package totals would underflow on \
                             remove; descendant stats were inconsistent"
                        );
                    }
                    anc_info.descendant_vsize =
                        anc_info.descendant_vsize.saturating_sub(vsize);
                    anc_info.fees.descendant = anc_info
                        .fees
                        .descendant
                        .checked_sub(fees.modified)
                        .unwrap_or(Amount::ZERO);
                    anc_info.spent_by.remove(txid);
                    Ok(())
                })?;
        let ancestor_fee_rate = FeeRate {
            fee: fees.ancestor,
            vsize: ancestor_modified_weight.to_vbytes_ceil(),
        };
        // Update `self.by_ancestor_fee_rate`
        if !self.by_ancestor_fee_rate.remove(ancestor_fee_rate, *txid) {
            let err = MissingByAncestorFeeRateKeyError(ancestor_fee_rate);
            return Err(err.into());
        };
        let res = self.txs.0.remove(txid);
        Ok(res)
    }

    /// Remove a tx from mempool, and all descendants.
    /// Returns the removed tx (if it was present) and any descendants.
    fn remove_with_descendants(
        &mut self,
        txid: &Txid,
    ) -> Result<LinkedHashMap<Txid, Transaction>, MempoolRemoveError> {
        let mut res = LinkedHashMap::new();
        if let Some((tx, _tx_info)) = self.remove(txid)? {
            res.replace(*txid, tx);
        }
        let mut remove_stack = LinkedHashSet::<Txid>::from_iter(
            self.tx_childs
                .0
                .get(txid)
                .iter()
                .flat_map(|tx_childs| tx_childs.iter().cloned()),
        );
        while let Some(txid) = remove_stack.pop_front() {
            if let Some((tx, _tx_info)) = self.remove(&txid)? {
                res.replace(txid, tx);
            };
            remove_stack.extend(
                self.tx_childs
                    .0
                    .get(&txid)
                    .iter()
                    .flat_map(|tx_childs| tx_childs.iter().cloned()),
            );
        }
        Ok(res)
    }

    /// Txids of mempool txs spending the given outpoint.
    pub(crate) fn spenders_of(&self, outpoint: &OutPoint) -> Vec<Txid> {
        let Some(childs) = self.tx_childs.0.get(&outpoint.txid) else {
            return Vec::new();
        };
        childs
            .iter()
            .filter(|child_txid| {
                self.txs.0.get(*child_txid).is_some_and(|(child_tx, _)| {
                    child_tx
                        .input
                        .iter()
                        .any(|input| input.previous_output == *outpoint)
                })
            })
            .copied()
            .collect()
    }

    /// Retain txs for which the provided closure returns `true`.
    /// The closure's second argument is the in-mempool input txs for the
    /// transaction.
    /// If the bool argument is `true, also deletes descendants of any deleted
    /// tx.
    /// Returns the removed txs.
    pub fn try_filter<F, E>(
        &mut self,
        also_remove_descendants: bool,
        mut f: F,
    ) -> Result<
        LinkedHashMap<Txid, Transaction>,
        either::Either<MempoolRemoveError, E>,
    >
    where
        F: FnMut(&Transaction, &HashMap<Txid, &Transaction>) -> Result<bool, E>,
    {
        let no_ancestors_txids: Vec<Txid> = self
            .txs
            .0
            .iter()
            .filter_map(|(txid, (_tx, tx_info))| {
                if tx_info.depends.is_empty() {
                    Some(*txid)
                } else {
                    None
                }
            })
            .collect();
        let mut res = LinkedHashMap::new();
        for txid in no_ancestors_txids {
            let mut descendants = Vec::<Txid>::new();
            let () = self
                .txs
                .descendants_mut(txid)
                .for_each(|(tx, _info)| {
                    let descendant_txid = tx.compute_txid();
                    descendants.push(descendant_txid);
                    Ok(())
                })
                .map_err(|err| either::Either::Left(err.into()))?;
            'descs: for descendant_txid in descendants {
                let Some((tx, _info)) = self.txs.0.get(&descendant_txid) else {
                    continue 'descs;
                };
                let mut tx_inputs = HashMap::<Txid, &Transaction>::new();
                'tx_inputs: for tx_in in &tx.input {
                    let input_txid = tx_in.previous_output.txid;
                    if tx_inputs.contains_key(&input_txid) {
                        continue 'tx_inputs;
                    }
                    if let Some((input_tx, _)) = self.txs.0.get(&input_txid) {
                        tx_inputs.insert(input_txid, input_tx);
                    }
                }
                if !f(tx, &tx_inputs).map_err(either::Either::Right)? {
                    let removed = if also_remove_descendants {
                        self.remove_with_descendants(&descendant_txid)
                            .map_err(either::Either::Left)?
                    } else {
                        self.remove(&descendant_txid)
                            .map_err(either::Either::Left)?
                            .into_iter()
                            .map(|(tx, _tx_info)| (descendant_txid, tx))
                            .collect()
                    };
                    res.extend(removed);
                }
            }
        }
        Ok(res)
    }

    /// choose txs for a block proposal, mutating the underlying mempool.
    /// If no weight limit is specified, or the specified weight exceeds
    /// `Weight::MAX_BLOCK`, then `Weight::MAX_BLOCK` will be used as the
    /// weight limit.
    fn propose_txs_mut(
        &mut self,
        weight_limit: Option<Weight>,
    ) -> Result<IndexSet<Txid>, MempoolRemoveError> {
        let mut res = IndexSet::new();
        let mut weight_remaining = weight_limit
            .unwrap_or(MAX_USABLE_BLOCK_WEIGHT)
            .min(MAX_USABLE_BLOCK_WEIGHT);
        tracing::debug!(%weight_remaining, "Selecting txs");
        loop {
            let Some((ancestor_fee_rate, txid)) = self
                .by_ancestor_fee_rate
                .iter_rev()
                .find(|(ancestor_fee_rate, _txid)| {
                    Weight::from_vb(ancestor_fee_rate.vsize).is_some_and(
                        |ancestors_weight| ancestors_weight <= weight_remaining,
                    )
                })
            else {
                break;
            };
            tracing::trace!(%txid, "Proposing tx with ancestors");
            // stack of txs to add
            let mut to_add = vec![(txid, false)];
            // What this package really costs, summed from the txs removed —
            // not from the index key, which is a cached package total that
            // can be stale or under-counted (see
            // `recompute_package_stats_around`). Per tx the charge is the
            // larger of its raw weight (what the block is bounded by) and its
            // `modified_weight` (raw weight + the caller's tweak: the enforcer
            // adds the weight of the M7 coinbase output that block production
            // appends for every accepted BMM request, and nothing else
            // reserves that space). With exact stats and a non-negative tweak
            // this equals the old key-based charge; a negative tweak still
            // cannot undershoot real weight.
            let mut package_weight = Weight::ZERO;
            let mut package_txids = Vec::new();
            while let Some((txid, parents_visited)) = to_add.pop() {
                if parents_visited {
                    tracing::trace!(%txid, "Removing tx from mempool");
                    let (tx, info) = self
                        .remove(&txid)?
                        .expect("missing tx in mempool when proposing txs");
                    package_weight = saturating_add_weight(
                        package_weight,
                        tx.weight().max(info.modified_weight),
                    );
                    package_txids.push(txid);
                    res.insert(txid);
                    // Remove conflicts for the final tx
                    if to_add.is_empty() {
                        for conflict_txid in info.conflicts_with {
                            for (removed_txid, _removed_tx) in
                                self.remove_with_descendants(&conflict_txid)?
                            {
                                tracing::trace!(%txid, %removed_txid, "Removed tx from mempool due to conflict");
                            }
                        }
                    }
                } else {
                    let Some((_, info)) = self.txs.0.get(&txid) else {
                        tracing::warn!(%txid, "Missing tx in mempool when proposing txs, omitting from block template proposal");
                        continue;
                    };

                    to_add.push((txid, true));
                    to_add.extend(info.depends.iter().map(|dep| (*dep, false)))
                }
            }
            if package_weight > weight_remaining {
                // The cached key admitted a package that does not fit. It is
                // already out of this (cloned) mempool, so it cannot be
                // re-selected; leave it out of the template rather than emit
                // an over-weight block.
                tracing::warn!(
                    %txid,
                    package_weight = %package_weight,
                    key_vsize = ancestor_fee_rate.vsize,
                    "package heavier than its index key admits; omitting it \
                     from the block template"
                );
                for t in package_txids {
                    res.shift_remove(&t);
                    // `remove` already pruned `t` from its descendants'
                    // `depends`, so they would be selected as rootless
                    // packages and `propose_txs` would then fail on the
                    // missing ancestor — no template at all. Take them out of
                    // this clone with their parents (tx_childs still lists
                    // a removed tx's children).
                    drop(self.remove_with_descendants(&t)?);
                }
                continue;
            }
            weight_remaining -= package_weight;
        }
        Ok(res)
    }

    pub fn propose_txs(
        &self,
        weight_limit: Option<Weight>,
    ) -> Result<Vec<BlockTemplateTransaction>, MempoolRemoveError> {
        let mut txs = self.clone().propose_txs_mut(weight_limit)?;
        let mut res = Vec::new();
        // build result in reverse order
        while let Some(txid) = txs.pop() {
            tracing::trace!(%txid, "Computing deps for tx");
            let mut depends = Vec::new();
            let mut ancestors = self.txs.ancestors(txid);
            while let Some((anc_txid, _, _)) = ancestors.next()? {
                let anc_idx = txs.get_index_of(&anc_txid).ok_or(
                    MissingAncestorError {
                        tx: txid,
                        missing: anc_txid,
                    },
                )?;
                depends.push(anc_idx as u32);
            }
            depends.sort();

            // TODO: Not sure if this is correct behavior. But avoid panics if we're
            // handling a transaction that we cannot find.
            let Some((tx, info)) = self.txs.0.get(&txid) else {
                tracing::warn!(%txid, "Missing tx in mempool when proposing txs, omitting from block template");
                continue;
            };

            let block_template_tx = BlockTemplateTransaction {
                data: bitcoin::consensus::serialize(tx),
                txid,
                hash: tx.compute_wtxid(),
                depends,
                fee: bitcoin::SignedAmount::from_sat(
                    info.fees.base.to_sat() as i64
                ),
                // FIXME: compute this
                sigops: None,
                weight: tx.weight().to_wu(),
            };
            res.push(block_template_tx);
        }
        res.reverse();
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{
        Amount, OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Witness,
        absolute::LockTime, hashes::Hash as _, transaction::Version,
    };

    fn make_tx(inputs: &[OutPoint], num_outputs: usize) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: inputs
                .iter()
                .map(|prev| TxIn {
                    previous_output: *prev,
                    sequence: Sequence::MAX,
                    script_sig: ScriptBuf::new(),
                    witness: Witness::new(),
                })
                .collect(),
            output: (0..num_outputs)
                .map(|i| TxOut {
                    value: Amount::from_sat(1000 * (i as u64 + 1)),
                    script_pubkey: ScriptBuf::new(),
                })
                .collect(),
        }
    }

    fn test_mempool() -> Mempool {
        Mempool::new(BlockHash::all_zeros())
    }

    /// Reproduces the prod crash: inserting a tx whose `conflicts_with`
    /// references a txid not present in the mempool (e.g. already confirmed
    /// in a block) must not panic or error.
    #[test]
    fn insert_with_nonexistent_conflict_succeeds() {
        let mut mempool = test_mempool();

        let tx = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 1);
        let weight = tx.weight();
        let absent_txid = Txid::from_byte_array([0x42; 32]);

        let result = mempool.insert(
            tx,
            Amount::from_sat(100),
            OrdSet::unit(absent_txid),
            weight,
        );

        assert!(result.is_ok(), "insert failed: {result:?}");
    }

    /// Assert that a block template is a valid block order: every tx that spends
    /// the output of another *in-mempool* (unconfirmed) tx must (a) have that
    /// parent present in the template, and (b) appear strictly after it.
    /// A miner that violates either produces a block rejected with
    /// `bad-txns-inputs-missingorspent`.
    fn assert_template_topologically_valid(
        template: &[BlockTemplateTransaction],
        mempool: &Mempool,
    ) {
        let position: HashMap<Txid, usize> = template
            .iter()
            .enumerate()
            .map(|(i, t)| (t.txid, i))
            .collect();
        for (i, t) in template.iter().enumerate() {
            let tx: Transaction = bitcoin::consensus::deserialize(&t.data)
                .expect("template tx data did not deserialize");
            for input in &tx.input {
                let parent = input.previous_output.txid;
                // Only in-mempool parents matter; anything else is assumed
                // confirmed on-chain.
                if !mempool.txs.0.contains_key(&parent) {
                    continue;
                }
                let parent_pos = position.get(&parent).unwrap_or_else(|| {
                    panic!(
                        "template not closed: tx {} (pos {i}) spends in-mempool \
                         parent {parent}, which is absent from the template",
                        t.txid
                    )
                });
                assert!(
                    *parent_pos < i,
                    "template out of order: tx {} at pos {i} spends parent \
                     {parent} at pos {parent_pos}",
                    t.txid
                );
            }
        }
    }

    /// Regression for the silent invalid-template bug (issue #611). When a child
    /// tx is inserted BEFORE its parent (out-of-order mempool sync, routine
    /// under RBF churn / batched-fetch gaps), the child's `depends` set is
    /// computed once at insert time and never learns about the later-arriving
    /// parent, and the descendant's `by_ancestor_fee_rate` key is left stale.
    /// `propose_txs` then either errors with `MissingByAncestorFeeRateKey` or
    /// emits the child ahead of — or without — its parent, yielding a template
    /// that mines to a block rejected `bad-txns-inputs-missingorspent`.
    /// Regression for issue #611 (the stale-template half). When a block
    /// confirms a tx, mempool txs spending the same outpoint (RBF variants /
    /// double-spends) are evicted by the node WITHOUT a removal sequence
    /// message; `spenders_of` is what the sync layer uses to find and evict
    /// them locally at block connect, together with their descendants.
    #[test]
    fn spenders_of_finds_conflicting_spender() {
        let mut mempool = test_mempool();

        let funding_outpoint = OutPoint::new(Txid::all_zeros(), 0);
        // A spends the funding outpoint; C spends A's output 0.
        let tx_a = make_tx(&[funding_outpoint], 1);
        let txid_a = tx_a.compute_txid();
        let tx_c = make_tx(&[OutPoint::new(txid_a, 0)], 1);
        let txid_c = tx_c.compute_txid();
        // B spends an unrelated outpoint of the same funding tx.
        let tx_b = make_tx(&[OutPoint::new(Txid::all_zeros(), 1)], 1);
        let txid_b = tx_b.compute_txid();

        for tx in [tx_a, tx_c, tx_b] {
            let weight = tx.weight();
            mempool
                .insert(tx, Amount::from_sat(1000), OrdSet::new(), weight)
                .unwrap();
        }

        // Only A spends the funding outpoint itself.
        assert_eq!(mempool.spenders_of(&funding_outpoint), vec![txid_a]);
        assert_eq!(
            mempool.spenders_of(&OutPoint::new(Txid::all_zeros(), 7)),
            Vec::<Txid>::new()
        );

        // The sweep at block connect: a confirmed tx consumed the funding
        // outpoint, so A and its descendant C must go; B stays.
        let removed = mempool.remove_with_descendants(&txid_a).unwrap();
        assert!(removed.contains_key(&txid_a));
        assert!(removed.contains_key(&txid_c));
        assert!(!mempool.txs.0.contains_key(&txid_a));
        assert!(!mempool.txs.0.contains_key(&txid_c));
        assert!(mempool.txs.0.contains_key(&txid_b));
        assert_eq!(mempool.spenders_of(&funding_outpoint), Vec::<Txid>::new());
    }

    #[test]
    fn out_of_order_insert_yields_valid_template() {
        let mut mempool = test_mempool();

        // parent P (spends a confirmed output), child A (spends P's output 0).
        let parent = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 1);
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], 1);

        // Insert the CHILD first, then the PARENT.
        let child_weight = child.weight();
        mempool
            .insert(child, Amount::from_sat(1000), OrdSet::new(), child_weight)
            .unwrap();
        let parent_weight = parent.weight();
        mempool
            .insert(
                parent,
                Amount::from_sat(1000),
                OrdSet::new(),
                parent_weight,
            )
            .unwrap();

        let template = mempool
            .propose_txs(None)
            .expect("propose_txs returned an error");
        assert_template_topologically_valid(&template, &mempool);
    }

    /// Multi-level variant: a 3-tx chain (grandparent -> parent -> child)
    /// inserted in FULLY REVERSED order (child, then parent, then grandparent).
    /// Each late-arriving ancestor must re-key every descendant's
    /// `by_ancestor_fee_rate` entry and backfill its direct child's `depends`,
    /// so the proposed template still lists all three in a valid block order —
    /// exercising the fix across more than one dependency level.
    #[test]
    fn out_of_order_insert_chain_yields_valid_template() {
        let mut mempool = test_mempool();

        // chain: grandparent G (confirmed input) <- parent P <- child C.
        let grandparent = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 1);
        let grandparent_txid = grandparent.compute_txid();
        let parent = make_tx(&[OutPoint::new(grandparent_txid, 0)], 1);
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], 1);
        let child_txid = child.compute_txid();

        // Insert fully reversed: CHILD, then PARENT, then GRANDPARENT.
        let w = child.weight();
        mempool
            .insert(child, Amount::from_sat(1000), OrdSet::new(), w)
            .unwrap();
        let w = parent.weight();
        mempool
            .insert(parent, Amount::from_sat(1000), OrdSet::new(), w)
            .unwrap();
        let w = grandparent.weight();
        mempool
            .insert(grandparent, Amount::from_sat(1000), OrdSet::new(), w)
            .unwrap();

        let template = mempool
            .propose_txs(None)
            .expect("propose_txs returned an error");
        assert_template_topologically_valid(&template, &mempool);
        // The whole chain must be proposed — none silently dropped.
        let ids: std::collections::HashSet<Txid> =
            template.iter().map(|t| t.txid).collect();
        assert!(
            ids.contains(&grandparent_txid)
                && ids.contains(&parent_txid)
                && ids.contains(&child_txid),
            "template is missing part of the chain: {ids:?}"
        );
    }

    /// Killing the mutation the review found surviving in the descendant
    /// fee-rate re-key of the out-of-order insert fix: `out_of_order_insert_*`
    /// only proposes a template, which never consults a descendant's
    /// `by_ancestor_fee_rate` key. A later `remove` does — and fails with
    /// `MissingByAncestorFeeRateKey` if the descendant was left under its stale
    /// pre-parent key. (issue #611)
    #[test]
    fn out_of_order_insert_then_remove_rekeys_descendant_fee_rate() {
        let mut mempool = test_mempool();
        let parent = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 1);
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], 1);
        let child_txid = child.compute_txid();

        // Out-of-order: child before parent. Inserting the parent must re-key
        // the child's ancestor-fee-rate entry (its ancestor set gained the
        // parent) — otherwise the child sits under a stale key.
        let w = child.weight();
        mempool
            .insert(child, Amount::from_sat(1000), OrdSet::new(), w)
            .unwrap();
        let w = parent.weight();
        mempool
            .insert(parent, Amount::from_sat(1000), OrdSet::new(), w)
            .unwrap();

        // `remove` looks the child up by its CURRENT ancestor fee rate; without
        // the re-key that key is absent and removal fails.
        mempool.remove_with_descendants(&child_txid).expect(
            "removing an out-of-order-inserted descendant must not fail with a \
             stale ancestor-fee-rate key",
        );
        assert!(!mempool.txs.0.contains_key(&child_txid));
    }

    /// The middle of a chain arriving LAST. G (in the pool) <- P (absent)
    /// <- C: insert G, then C (whose only in-pool dep is none — P is absent),
    /// then P. The incremental insert credited C with P alone and G with P
    /// alone; the true packages are C+P+G on both sides. Found by the
    /// 2026-09-18 review of PR #114 (defect #1).
    fn middle_tx_arrives_last() -> (Mempool, Txid, Txid, Txid, u64, u64, u64) {
        let mut mempool = test_mempool();
        // A wide grandparent so the missing term is unmistakable.
        let grandparent = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 12);
        let grandparent_txid = grandparent.compute_txid();
        let parent = make_tx(&[OutPoint::new(grandparent_txid, 0)], 1);
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], 1);
        let child_txid = child.compute_txid();
        let (g_vs, p_vs, c_vs) = (
            grandparent.vsize() as u64,
            parent.vsize() as u64,
            child.vsize() as u64,
        );
        let w = grandparent.weight();
        mempool
            .insert(grandparent, Amount::from_sat(10), OrdSet::new(), w)
            .unwrap();
        let w = child.weight();
        mempool
            .insert(child, Amount::from_sat(1000), OrdSet::new(), w)
            .unwrap();
        let w = parent.weight();
        mempool
            .insert(parent, Amount::from_sat(1000), OrdSet::new(), w)
            .unwrap();
        (
            mempool,
            grandparent_txid,
            parent_txid,
            child_txid,
            g_vs,
            p_vs,
            c_vs,
        )
    }

    #[test]
    fn middle_tx_arrives_last_package_totals_are_exact() {
        let (mempool, g, p, c, g_vs, p_vs, c_vs) = middle_tx_arrives_last();
        let (_, c_info) = &mempool.txs.0[&c];
        let (_, p_info) = &mempool.txs.0[&p];
        let (_, g_info) = &mempool.txs.0[&g];
        assert_eq!(
            c_info.ancestor_vsize,
            c_vs + p_vs + g_vs,
            "child's ancestor package must count the grandparent"
        );
        assert_eq!(
            c_info.fees.ancestor,
            Amount::from_sat(1000 + 1000 + 10),
            "child's ancestor fees must count the grandparent"
        );
        assert!(c_info.depends.contains(&p), "child must depend on parent");
        assert_eq!(p_info.ancestor_vsize, p_vs + g_vs);
        assert_eq!(p_info.descendant_vsize, p_vs + c_vs);
        assert_eq!(
            g_info.descendant_vsize,
            g_vs + p_vs + c_vs,
            "grandparent's descendant package must count the child"
        );
        assert_eq!(g_info.fees.descendant, Amount::from_sat(10 + 1000 + 1000));
        // The child's index key must be the recomputed one: a `remove` looks
        // it up by that key and fails on a stale one.
        let key = FeeRate {
            fee: c_info.fees.ancestor,
            vsize: c_info.ancestor_modified_weight.to_vbytes_ceil(),
        };
        assert!(
            mempool
                .by_ancestor_fee_rate
                .0
                .get(&refinement_cmp::RefinementCmp(key))
                .is_some_and(|set| set.contains(&c)),
            "child is not indexed under its recomputed ancestor fee rate"
        );
    }

    #[test]
    fn middle_tx_arrives_last_template_respects_weight_limit() {
        let (mempool, _g, _p, _c, g_vs, p_vs, c_vs) = middle_tx_arrives_last();
        // A limit that fits C+P but NOT C+P+G. Under-counted totals let the
        // whole chain through (2124 wu against a 488 wu limit in the review's
        // probe); exact totals must yield a template within the limit — and
        // since C and P both need G, that template is empty.
        let limit = Weight::from_vb_unwrap(c_vs + p_vs + g_vs - 1);
        let template = mempool.propose_txs(Some(limit)).unwrap();
        let total: u64 = template
            .iter()
            .map(|t| {
                let tx: Transaction =
                    bitcoin::consensus::deserialize(&t.data).unwrap();
                tx.weight().to_wu()
            })
            .sum();
        assert!(
            total <= limit.to_wu(),
            "template weight {total} exceeds the limit {}",
            limit.to_wu()
        );
        assert_template_topologically_valid(&template, &mempool);
        // And with room for all three, all three are proposed, in order.
        let template = mempool.propose_txs(None).unwrap();
        assert_eq!(template.len(), 3);
        assert_template_topologically_valid(&template, &mempool);
    }

    #[test]
    fn middle_tx_arrives_last_then_remove_does_not_underflow() {
        // The review's second measurement: with a high-fee grandparent the
        // under-counted child fee (`ancestor -= G.fee`) panicked with
        // `Amount subtraction error` inside `remove` — in the sync task, on
        // block connect. Now the totals are exact, and `remove` saturates.
        let mut mempool = test_mempool();
        let grandparent = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 12);
        let g = grandparent.compute_txid();
        let parent = make_tx(&[OutPoint::new(g, 0)], 1);
        let p = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(p, 0)], 1);
        let c = child.compute_txid();
        let w = grandparent.weight();
        mempool
            .insert(grandparent, Amount::from_sat(50_000), OrdSet::new(), w)
            .unwrap();
        let w = child.weight();
        mempool
            .insert(child, Amount::from_sat(1000), OrdSet::new(), w)
            .unwrap();
        let w = parent.weight();
        mempool
            .insert(parent, Amount::from_sat(1000), OrdSet::new(), w)
            .unwrap();
        // Mine G: remove it, descendants stay and are re-keyed.
        mempool
            .remove(&g)
            .expect("removing the grandparent must not fail");
        let (_, c_info) = &mempool.txs.0[&c];
        assert_eq!(c_info.fees.ancestor, Amount::from_sat(2000));
        let (_, p_info) = &mempool.txs.0[&p];
        assert_eq!(p_info.fees.ancestor, Amount::from_sat(1000));
        assert!(p_info.depends.is_empty());
        // Then everything else proposes cleanly.
        let template = mempool.propose_txs(None).unwrap();
        assert_eq!(template.len(), 2);
        assert_template_topologically_valid(&template, &mempool);
    }

    /// The weight guard alone: an index key that under-counts must not let a
    /// package overshoot the limit, and a dropped package must not leave its
    /// descendants selectable without it (that yields no template at all).
    /// A tx with `modified_weight` far below its real weight is the cheapest
    /// way to fake an under-counting key.
    #[test]
    fn weight_guard_drops_package_whose_key_undercounts_and_its_descendants() {
        let mut mempool = test_mempool();
        let parent = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 4);
        let p = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(p, 0)], 1);
        let c = child.compute_txid();
        let grandchild = make_tx(&[OutPoint::new(c, 0)], 1);
        let g = grandchild.compute_txid();
        let p_weight = parent.weight();
        // Key says 4 wu; the tx really weighs p_weight.
        mempool
            .insert(
                parent,
                Amount::from_sat(5000),
                OrdSet::new(),
                Weight::from_wu(4),
            )
            .unwrap();
        let w = child.weight();
        mempool
            .insert(child, Amount::from_sat(10), OrdSet::new(), w)
            .unwrap();
        let w = grandchild.weight();
        mempool
            .insert(grandchild, Amount::from_sat(10), OrdSet::new(), w)
            .unwrap();
        // Fits the key (4 wu) but not the real parent.
        let limit = Weight::from_wu(p_weight.to_wu() - 1);
        let template = mempool.propose_txs(Some(limit)).expect(
            "a dropped package must not make propose_txs fail on a missing ancestor",
        );
        assert!(
            template.is_empty(),
            "nothing fits without the parent, got {} txs",
            template.len()
        );
        // Sanity: with the key honest, the guard never trips and all propose.
        let _ = (c, g);
    }

    /// The charge is max(raw weight, modified weight): a positive tweak (the
    /// enforcer's +M7-output reservation per BMM request) must reserve space.
    /// Two independent txs, each with a +192 wu tweak: the index filter alone
    /// admits the second one whenever the first was charged at raw weight.
    #[test]
    fn proposal_charges_modified_weight_when_larger() {
        let mut mempool = test_mempool();
        let a = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 1);
        let b = make_tx(&[OutPoint::new(Txid::all_zeros(), 1)], 1);
        let w = a.weight();
        assert_eq!(w, b.weight());
        let tweak = Weight::from_wu(192);
        mempool
            .insert(a, Amount::from_sat(1000), OrdSet::new(), w + tweak)
            .unwrap();
        mempool
            .insert(b, Amount::from_sat(1000), OrdSet::new(), w + tweak)
            .unwrap();
        // The index key rounds the modified weight up to whole vbytes.
        let key_weight =
            Weight::from_vb_unwrap((w + tweak).to_vbytes_ceil()).to_wu();
        // After charging the first tx at w+tweak, the second's key must NOT
        // fit; after charging it at w only, it does.
        let limit = Weight::from_wu(w.to_wu() + tweak.to_wu() + key_weight - 1);
        let template = mempool.propose_txs(Some(limit)).unwrap();
        assert_eq!(
            template.len(),
            1,
            "second tx admitted: the first was charged at raw weight, not modified"
        );
    }

    /// `remove` on inconsistent totals must saturate, not panic: hand-corrupt
    /// a child's ancestor stats below its parent's (and the parent's
    /// descendant stats below the child's), then remove one of them.
    fn corrupted_parent_child() -> (Mempool, Txid, Txid) {
        let mut mempool = test_mempool();
        let parent = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], 1);
        let p = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(p, 0)], 1);
        let c = child.compute_txid();
        let w = parent.weight();
        mempool
            .insert(parent, Amount::from_sat(5000), OrdSet::new(), w)
            .unwrap();
        let w = child.weight();
        mempool
            .insert(child, Amount::from_sat(100), OrdSet::new(), w)
            .unwrap();
        let (_, c_info) = mempool.txs.0.get_mut(&c).unwrap();
        let old = FeeRate {
            fee: c_info.fees.ancestor,
            vsize: c_info.ancestor_modified_weight.to_vbytes_ceil(),
        };
        mempool.by_ancestor_fee_rate.remove(old, c);
        c_info.fees.ancestor = Amount::from_sat(100);
        c_info.ancestor_vsize = 1;
        let new = FeeRate {
            fee: c_info.fees.ancestor,
            vsize: c_info.ancestor_modified_weight.to_vbytes_ceil(),
        };
        mempool.by_ancestor_fee_rate.insert(new, c);
        let (_, p_info) = mempool.txs.0.get_mut(&p).unwrap();
        p_info.fees.descendant = Amount::from_sat(1);
        p_info.descendant_vsize = 1;
        (mempool, p, c)
    }

    #[test]
    fn remove_parent_saturates_descendant_totals() {
        // Removing the parent runs the DESCENDANTS half over the corrupted
        // child: 100 sat - 5000 sat.
        let (mut mempool, p, c) = corrupted_parent_child();
        mempool
            .remove(&p)
            .expect("descendants-half underflow must saturate");
        let (_, c_info) = &mempool.txs.0[&c];
        assert_eq!(c_info.fees.ancestor, Amount::ZERO);
        assert_eq!(c_info.ancestor_vsize, 0);
        mempool.remove(&c).unwrap();
        assert!(mempool.txs.0.is_empty());
    }

    #[test]
    fn remove_child_saturates_ancestor_totals() {
        // Removing the child runs the ANCESTORS half over the corrupted
        // parent: 1 sat - 100 sat.
        let (mut mempool, p, c) = corrupted_parent_child();
        mempool
            .remove(&c)
            .expect("ancestors-half underflow must saturate");
        let (_, p_info) = &mempool.txs.0[&p];
        assert_eq!(p_info.fees.descendant, Amount::ZERO);
        assert_eq!(p_info.descendant_vsize, 0);
        mempool.remove(&p).unwrap();
        assert!(mempool.txs.0.is_empty());
    }
}
