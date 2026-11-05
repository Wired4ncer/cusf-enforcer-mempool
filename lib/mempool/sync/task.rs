//! Initial mempool sync
//!
//! During initial mempool sync, we maintain both a filtered and unfiltered
//! mempool.
//! This allows us to correctly mirror the node's mempool, whilst also
//! constructing a mempool that enforces a CUSF enforcer's validation rules.

use core::future::Future;
use std::{
    borrow::BorrowMut,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    path::Path,
    sync::Arc,
};

use bitcoin::{Amount, BlockHash, OutPoint, Transaction, TxIn, Txid, Weight};
use bitcoin_jsonrpsee::{
    bitcoin::hashes::Hash as _,
    client::{BoolWitness, GetRawMempoolClient as _, RawMempoolWithSequence},
    jsonrpsee::core::ClientError as JsonRpcError,
};
use educe::Educe;
use futures::{
    FutureExt as _, StreamExt as _,
    future::{BoxFuture, FusedFuture},
};
use hashlink::LinkedHashSet;
use thiserror::Error;
use tokio::sync::{RwLock as TokioRwLock, watch};
use tracing::Instrument;

use crate::{
    cusf_enforcer::{
        self, ConnectBlockAction, CusfEnforcer, DisconnectBlockAction,
    },
    mempool::{
        Mempool, MempoolInsertError, MempoolRemoveError, MempoolUpdateError,
        sync::{
            ApplySyncActionResult, ApplySyncActionTimeoutError,
            BatchedResponseItem, CombinedStream, CombinedStreamItem,
            RequestError, RequestItem, RequestQueue, ResponseItem, SyncAction,
            SyncActionQueue, abandoned_pool::AbandonedPool,
            apply_sync_action_timeout, batched_request,
        },
    },
    zmq::{
        BlockHashEvent, BlockHashMessage, SequenceMessage, SequenceStreamError,
        TxHashEvent, TxHashMessage,
    },
};

#[derive(Debug)]
struct UnfilteredMempool {
    tip: BlockHash,
    txs: HashSet<Txid>,
}

#[derive(Educe)]
#[educe(Debug(bound(cusf_enforcer::Error<Enforcer>: std::fmt::Debug)))]
#[derive(Error)]
pub enum SyncTaskError<Enforcer>
where
    Enforcer: CusfEnforcer,
{
    #[error(transparent)]
    ApplySyncActionTimeout(#[from] ApplySyncActionTimeoutError),
    #[error("Combined stream ended unexpectedly")]
    CombinedStreamEnded,
    #[error(
        "disconnected block tip ({}) does not match unfiltered mempool ({})",
        .disconnected_tip,
        .unfiltered_mempool_tip,
    )]
    DisconnectTipMismatch {
        disconnected_tip: BlockHash,
        unfiltered_mempool_tip: BlockHash,
    },
    #[error(transparent)]
    CusfEnforcer(#[from] cusf_enforcer::Error<Enforcer>),
    #[error("Failed to decode block: `{block_hash}`")]
    DecodeBlock {
        block_hash: BlockHash,
        source: bitcoin::consensus::encode::Error,
    },
    #[error("Fee overflow")]
    FeeOverflow,
    #[error("Missing first message with mempool sequence: {0}")]
    FirstMempoolSequence(u64),
    #[error(transparent)]
    InitialSyncEnforcer(#[from] cusf_enforcer::InitialSyncError<Enforcer>),
    #[error("RPC error")]
    JsonRpc(#[from] JsonRpcError),
    #[error(transparent)]
    MempoolInsert(#[from] MempoolInsertError),
    #[error(transparent)]
    MempoolRemove(#[from] MempoolRemoveError),
    #[error(transparent)]
    MempoolUpdate(#[from] MempoolUpdateError),
    #[error(transparent)]
    Request(#[from] RequestError),
    #[error("Sequence stream error")]
    SequenceStream(#[from] SequenceStreamError),
    #[error("Sequence stream ended unexpectedly")]
    SequenceStreamEnded,
    // TODO - this is not really an error...
    #[error("Sync was stopped")]
    Shutdown,
    #[error(
        "unexpected tip ({}) in unfiltered mempool, expected ({})",
        .tip,
        .expected,
    )]
    UnexpectedTipUnfilteredMempool { tip: BlockHash, expected: BlockHash },
    #[error("missing tx ({0}) in unfiltered mempool")]
    UnfilteredMempoolMissingTx(Txid),
    #[error("Value in overflow")]
    ValueInOverflow,
    #[error("Value out overflow")]
    ValueOutOverflow,
}

struct MempoolSyncInner<Enforcer> {
    abandoned_pool: AbandonedPool,
    enforcer: Enforcer,
    mempool: Mempool,
    /// Publishes `mempool.chain.tip` whenever it changes, so observers (e.g.
    /// the GBT server's BIP22 long polling) can wait on tip changes without
    /// polling. Receivers come from [`MempoolSync::subscribe_tip`].
    tip_watch: watch::Sender<BlockHash>,
    unfiltered_mempool: UnfilteredMempool,
    /// How many times each rejected tx has been reconsidered after a block
    /// connect (see [`reconsider_rejected_txs`]). Cleared when the tx is
    /// admitted or leaves the node's mempool.
    reconsider_attempts: HashMap<Txid, u8>,
}

#[derive(Debug)]
pub struct SyncState {
    action_queue: SyncActionQueue,
    blocks_needed: LinkedHashSet<BlockHash>,
    /// Drop messages with lower mempool sequences.
    /// Set to None after encountering this mempool sequence ID.
    /// Return an error if higher sequence is encountered.
    first_mempool_sequence: Option<u64>,
    /// Blocks rejected by the CUSF enforcer
    rejected_blocks: HashSet<BlockHash>,
    /// Txs rejected by the CUSF enforcer
    rejected_txs: HashSet<Txid>,
    request_queue: RequestQueue,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: HashMap<Txid, Transaction>,
    txs_needed: LinkedHashSet<Txid>,
    unavailable_txs: HashSet<Txid>,
    /// Fees Core computed: `getrawmempool verbose` at startup, then
    /// `getmempoolentry` per arriving tx. Forgotten when the tx is mined or
    /// leaves the mempool.
    known_fees: HashMap<Txid, Amount>,
    /// Txids Core reported in the mempool. Used to tell a parent that is
    /// itself in the mempool -- which carries dependency semantics and must
    /// still be resolved -- from a confirmed one, which does not.
    mempool_txids: HashSet<Txid>,
}

/// Sync state, mutably borrowed while applying an action from the queue
struct SyncStateBorrowedMut<'a> {
    blocks_needed: &'a mut LinkedHashSet<BlockHash>,
    /// Blocks rejected by the CUSF enforcer
    rejected_blocks: &'a mut HashSet<BlockHash>,
    /// Txs rejected by the CUSF enforcer
    rejected_txs: &'a mut HashSet<Txid>,
    request_queue: &'a RequestQueue,
    /// Txs not needed in mempool, but requested in order to determine fees
    tx_cache: &'a mut HashMap<Txid, Transaction>,
    txs_needed: &'a mut LinkedHashSet<Txid>,
    unavailable_txs: &'a mut HashSet<Txid>,
    known_fees: &'a mut HashMap<Txid, Amount>,
    /// Pruned as members confirm or are removed, so a stale entry cannot
    /// classify a since-confirmed tx as still-unconfirmed (issue #611).
    mempool_txids: &'a mut HashSet<Txid>,
}

pub struct MempoolSyncing<Enforcer> {
    inner: MempoolSyncInner<Enforcer>,
    sync_state: SyncState,
}

struct MempoolSyncingBorrowed<'a, Enforcer> {
    inner: &'a MempoolSyncInner<Enforcer>,
    sync_state: &'a SyncState,
}

impl<Enforcer> MempoolSyncingBorrowed<'_, Enforcer> {
    /// A tx in the abandoned pool does not hold this up. Nothing is in flight
    /// for it: it waits on a message from the node (its parent back, or its
    /// own removal) that may never come, and it stays out of the mempool, and
    /// so out of templates, meanwhile. The same pool carries on after sync.
    fn is_synced(&self) -> bool {
        self.sync_state.blocks_needed.is_empty()
            && self.sync_state.txs_needed.is_empty()
            && self.sync_state.action_queue.is_empty()
            && self.inner.mempool.chain.tip == self.inner.unfiltered_mempool.tip
    }
}

// TODO: move txs_needed / request_queue handling out
/// Returns the txs to drive again now that the tip moved, parents before
/// children: those parked on a tx this block mined, then the rejected ones to
/// reconsider (see [`reconsider_rejected_txs`]). The caller queues an
/// `InsertTx` for each, in that order, at the front of the action queue.
async fn connect_block<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    block: &bitcoin_jsonrpsee::client::Block<true>,
) -> Result<Vec<Txid>, SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    let prev_blockhash =
        block.previousblockhash.unwrap_or_else(BlockHash::all_zeros);
    if prev_blockhash != inner.unfiltered_mempool.tip {
        if block.hash == inner.unfiltered_mempool.tip {
            // Ignore, block has already been applied
            return Ok(Vec::new());
        }
        // A subscriber that joins while bitcoind is still flushing its ZMQ
        // notification queue receives connect events for blocks that the
        // initial-sync tip snapshot already covers.
        if block.confirmations > 0
            && let Some(tip_block) = inner
                .mempool
                .chain
                .blocks
                .get(&inner.unfiltered_mempool.tip)
            && block.height < tip_block.height
        {
            tracing::debug!(
                block_hash = %block.hash,
                block_height = block.height,
                tip = %inner.unfiltered_mempool.tip,
                "Ignoring stale block connect below current tip"
            );
            return Ok(Vec::new());
        }
        // A disconnect for a rejected block is deliberately ignored,
        // but that skip also bypasses `handle_disconnected_block`, the
        // only place that rolls back `unfiltered_mempool.tip` -- leaving
        // it stuck at the rejected block. If the new block's parent is
        // our filtered tip, we're in exactly that state: the filtered view
        // is already correct, so avoid failing
        if prev_blockhash == inner.mempool.chain.tip {
            tracing::warn!(
                block_hash = %block.hash,
                stale_unfiltered_tip = %inner.unfiltered_mempool.tip,
                %prev_blockhash,
                "unfiltered mempool tip was stale (a disconnect for a \
                 rejected block skipped the tip rollback), resyncing to \
                 the filtered chain tip",
            );
            inner.unfiltered_mempool.tip = prev_blockhash;
        } else {
            return Err(SyncTaskError::UnexpectedTipUnfilteredMempool {
                tip: inner.unfiltered_mempool.tip,
                expected: prev_blockhash,
            });
        }
    }
    for tx_info in &block.tx {
        inner.unfiltered_mempool.txs.remove(&tx_info.txid);
    }
    inner.unfiltered_mempool.tip = block.hash;
    if prev_blockhash != inner.mempool.chain.tip {
        tracing::debug!(
            block_hash = %block.hash,
            mempool_tip = %inner.mempool.chain.tip,
            %prev_blockhash,
            "Rejecting block due to rejected parent"
        );
        sync_state.rejected_blocks.insert(block.hash);
        sync_state
            .request_queue
            .push_front(RequestItem::RejectBlock(block.hash));
        return Ok(Vec::new());
    }
    let block_decoded =
        block.try_into().map_err(|err| SyncTaskError::DecodeBlock {
            block_hash: block.hash,
            source: err,
        })?;
    match inner
        .enforcer
        .borrow_mut()
        .connect_block(&block_decoded)
        .await
        .map_err(cusf_enforcer::Error::ConnectBlock)?
    {
        ConnectBlockAction::Accept { remove_mempool_txs } => {
            let mined_txids: HashSet<Txid> = block_decoded
                .txdata
                .iter()
                .map(Transaction::compute_txid)
                .collect();
            // Parked txs whose parent this block just made available.
            let mut restored = Vec::new();
            for tx in block_decoded.txdata {
                let txid = tx.compute_txid();
                let _removed: Option<_> = inner.mempool.remove(&txid)?;
                sync_state.txs_needed.remove(&txid);
                sync_state.known_fees.remove(&txid);
                // Its fetch may have lost the race with this block after the
                // block's response cleared this (`handle_resp_block`). Left
                // set, every later child would be parked on a confirmed coin.
                sync_state.unavailable_txs.remove(&txid);
                // Mined while parked: no removal message will ever come for it.
                inner.abandoned_pool.remove(&txid);
                for (restored_txid, restored_tx) in
                    inner.abandoned_pool.restore_descendant_txs(&txid)
                {
                    sync_state.tx_cache.insert(restored_txid, restored_tx);
                    if !mined_txids.contains(&restored_txid) {
                        inner.unfiltered_mempool.txs.remove(&restored_txid);
                        restored.push(restored_txid);
                    }
                }
                // A tx this block mined is valid by the rules that accepted
                // the block, whatever `accept_tx` said about it earlier (an
                // M8 that reached us before the block it names is rejected as
                // expired, then mined by the next block). Left in
                // `rejected_txs`, every later spend of its outputs is refused
                // as "rejected parent" before the enforcer is even asked —
                // for a bidder that funds each bid from the previous bid's
                // change, that is every bid, forever. (ecpool beta,
                // 2026-09-19: no M8 in our template after 969,715.)
                sync_state.rejected_txs.remove(&txid);
                inner.reconsider_attempts.remove(&txid);
                // The initial-sync snapshot must forget confirmed members, or
                // a later child of this tx resolves it as a still-unconfirmed
                // mempool parent and re-admits the mined tx from the tx cache
                // into block templates (issue #611, observed live).
                sync_state.mempool_txids.remove(&txid);
                sync_state
                    .request_queue
                    .remove(&RequestItem::Tx(txid, true));
                sync_state.request_queue.remove(&RequestItem::Fee(txid));
                // Evict mempool txs that conflict with this confirmed tx:
                // they spend an outpoint it just consumed. The node evicts
                // them at block connect WITHOUT emitting a removal sequence
                // message, so without this sweep they linger in the enforced
                // mempool and every template they land in mines to
                // `bad-txns-inputs-missingorspent` (issue #611, the
                // stale-template half). Their descendants go with them — the
                // output they spend no longer exists. Txs mined in this same
                // block are skipped: they were removed above, and their
                // still-unconfirmed descendants remain valid.
                for input in &tx.input {
                    for spender in
                        inner.mempool.spenders_of(&input.previous_output)
                    {
                        if mined_txids.contains(&spender) {
                            continue;
                        }
                        for (removed_txid, _removed_tx) in
                            inner.mempool.remove_with_descendants(&spender)?
                        {
                            tracing::debug!(
                                %removed_txid,
                                conflicts_with = %txid,
                                block_hash = %block.hash,
                                "removed tx conflicting with confirmed tx",
                            );
                            inner.unfiltered_mempool.txs.remove(&removed_txid);
                            inner.reconsider_attempts.remove(&removed_txid);
                        }
                    }
                }
                sync_state.tx_cache.insert(txid, tx);
            }
            // What THIS block's connect evicts by the enforcer's own verdict
            // (e.g. the losing bids for the parent block) is stale for this
            // tip and must not be reconsidered in the same breath.
            let mut evicted_now = HashSet::new();
            for txid in remove_mempool_txs {
                let removed = inner.mempool.remove_with_descendants(&txid)?;
                // Descendants carry the rejection too, as on the `accept_tx`
                // path.
                let rejected: LinkedHashSet<Txid> = std::iter::once(txid)
                    .chain(removed.into_iter().map(|(txid, _tx)| txid))
                    .collect();
                for txid in rejected {
                    // Don't remove TXs mined by this very block.
                    // `prioritisetransaction` works even for a tx absent from
                    // the mempool, so deprioritizing a confirmed tx would
                    // silently poison it if a later reorg returned it to the
                    // mempool.
                    if mined_txids.contains(&txid) {
                        continue;
                    }
                    tracing::trace!(
                        %txid,
                        block_hash = %block.hash,
                        "deprioritizing tx removed by connected block",
                    );
                    sync_state.rejected_txs.insert(txid);
                    evicted_now.insert(txid);
                    sync_state
                        .request_queue
                        .push_front(RequestItem::RejectTx(txid));
                }
            }
            inner.mempool.chain.tip = block.hash;
            let _prev: BlockHash = inner.tip_watch.send_replace(block.hash);
            let reconsider =
                reconsider_rejected_txs(inner, sync_state, &evicted_now);
            tracing::debug!(
                block_hash = %block.hash,
                count = reconsider.len(),
                "reconsidering rejected txs after block connect"
            );
            restored.retain(|txid| !reconsider.contains(txid));
            restored.extend(reconsider);
            Ok(restored)
        }
        ConnectBlockAction::Reject => {
            sync_state.rejected_blocks.insert(block.hash);
            sync_state
                .request_queue
                .push_front(RequestItem::RejectBlock(block.hash));
            Ok(Vec::new())
        }
    }
}

/// The enforcer's verdict on a tx can depend on the chain tip: an M8 naming a
/// block the enforcer has not connected yet is `BmmRequestExpired`, and the
/// very same tx is valid the moment that block connects. A verdict is given
/// once, at arrival, so a tx that the node still holds but that we rejected
/// against an older tip would never be asked about again — it stays out of
/// every template we build and, once mined by someone else, seeds the
/// rejected-parent cascade (see `connect_block`).
///
/// After each accepted block, forget the verdict on every rejected tx the
/// node still holds, reverse its node-side deprioritisation, and hand it back
/// to the caller to be re-driven through `try_add_tx_from_caches`, parents
/// before children so a re-admitted parent is in the enforced mempool before
/// its child resolves against it (never as a rootless orphan, issue #611). A
/// tx whose input is a rejected tx the node no longer holds cannot be valid
/// and is left rejected. A tx the enforcer rejects again is simply rejected
/// again (the queue cancels the pending unreject, so the node's delta is
/// untouched); after `MAX_RECONSIDER_ATTEMPTS` re-rejections it is left
/// alone until the node drops it — an M8 can only become valid when the exact
/// block it names connects, which is the very next connect in the incident
/// case, so two attempts cover it and a spammer cannot make every block cost
/// one `accept_tx` per junk tx. `excluded` is what this same connect just
/// evicted (stale for this tip by the enforcer's own verdict). A rejected tx
/// whose bytes are not in `tx_cache` (a descendant pulled out of the
/// abandoned pool) cannot be re-driven and stays rejected until mined — the
/// same limit as before.
fn reconsider_rejected_txs<Enforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    excluded: &HashSet<Txid>,
) -> Vec<Txid> {
    const MAX_RECONSIDER_ATTEMPTS: u8 = 2;
    let mut remaining: Vec<Txid> = sync_state
        .rejected_txs
        .iter()
        .copied()
        .filter(|txid| {
            !excluded.contains(txid)
                && inner.unfiltered_mempool.txs.contains(txid)
                && sync_state.tx_cache.contains_key(txid)
                && inner.reconsider_attempts.get(txid).copied().unwrap_or(0)
                    < MAX_RECONSIDER_ATTEMPTS
        })
        .collect();
    // Deterministic order for the topological pass below.
    remaining.sort_unstable();
    let mut ordered = Vec::new();
    loop {
        let (ready, blocked): (Vec<Txid>, Vec<Txid>) =
            remaining.into_iter().partition(|txid| {
                sync_state.tx_cache[txid].input.iter().all(|input| {
                    !sync_state
                        .rejected_txs
                        .contains(&input.previous_output.txid)
                })
            });
        if ready.is_empty() {
            break;
        }
        for txid in ready {
            sync_state.rejected_txs.remove(&txid);
            // `try_add_tx_from_caches` short-circuits on a tx already recorded
            // in the unfiltered mirror; the re-drive puts it back.
            inner.unfiltered_mempool.txs.remove(&txid);
            sync_state
                .request_queue
                .push_front(RequestItem::UnrejectTx(txid));
            *inner.reconsider_attempts.entry(txid).or_insert(0) += 1;
            tracing::trace!(%txid, "reconsidering rejected tx after block connect");
            ordered.push(txid);
        }
        remaining = blocked;
    }
    ordered
}

/// Returns `false` without applying the disconnect if the parent block must
/// first be fetched into `chain.blocks`. The caller should retry once the
/// parent block response has been handled.
async fn handle_disconnected_block<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    blocks_needed: &mut LinkedHashSet<BlockHash>,
    rejected_txs: &mut HashSet<Txid>,
    request_queue: &RequestQueue,
    block: &bitcoin_jsonrpsee::client::Block<true>,
) -> Result<bool, SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    let prev_blockhash =
        block.previousblockhash.unwrap_or_else(BlockHash::all_zeros);
    if inner.unfiltered_mempool.tip != block.hash {
        return Err(SyncTaskError::DisconnectTipMismatch {
            disconnected_tip: block.hash,
            unfiltered_mempool_tip: inner.unfiltered_mempool.tip,
        });
    }
    if inner.mempool.chain.tip == block.hash {
        // Rolling the tip back requires the parent block to be present in
        // `chain.blocks` because `Mempool::tip` indexes into it. The parent is
        // absent if it predates the initial sync, which fetches only the
        // sync-time tip itself.
        if block.previousblockhash.is_some()
            && !inner.mempool.chain.blocks.contains_key(&prev_blockhash)
        {
            if !blocks_needed.contains(&prev_blockhash) {
                tracing::debug!(
                    block_hash = %block.hash,
                    %prev_blockhash,
                    "Requesting parent block before disconnect"
                );
                blocks_needed.replace(prev_blockhash);
                request_queue.push_front(RequestItem::Block(prev_blockhash));
            }
            return Ok(false);
        }
        let DisconnectBlockAction { remove_mempool_txs } = inner
            .enforcer
            .borrow_mut()
            .disconnect_block(block.hash)
            .await
            .map_err(cusf_enforcer::Error::DisconnectBlock)?;
        for txid in remove_mempool_txs {
            let removed = inner.mempool.remove_with_descendants(&txid)?;
            let rejected: LinkedHashSet<Txid> = std::iter::once(txid)
                .chain(removed.into_iter().map(|(txid, _tx)| txid))
                .collect();
            for txid in rejected {
                tracing::trace!(
                    %txid,
                    block_hash = %block.hash,
                    "deprioritizing tx removed by disconnected block",
                );
                rejected_txs.insert(txid);
                request_queue.push_front(RequestItem::RejectTx(txid));
            }
        }
        inner.mempool.chain.tip = prev_blockhash;
        let _prev: BlockHash = inner.tip_watch.send_replace(prev_blockhash);
    }
    inner.unfiltered_mempool.tip = prev_blockhash;
    Ok(true)
}

fn handle_block_hash_msg<Enforcer>(
    inner: &MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    block_hash_msg: BlockHashMessage,
) {
    let BlockHashMessage {
        block_hash,
        event: _,
        ..
    } = block_hash_msg;
    if !inner.mempool.chain.blocks.contains_key(&block_hash) {
        tracing::trace!(%block_hash, "Adding block to req queue");
        sync_state.blocks_needed.replace(block_hash);
        sync_state
            .request_queue
            .push_back(RequestItem::Block(block_hash));
    }
}

fn handle_tx_hash_msg<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    tx_hash_msg: TxHashMessage,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    let TxHashMessage {
        txid,
        event,
        mempool_seq,
        zmq_seq: _,
    } = tx_hash_msg;
    if let Some(first_mempool_seq) = sync_state.first_mempool_sequence {
        match mempool_seq.cmp(&first_mempool_seq) {
            Ordering::Less => {
                // Ignore
                return Ok(());
            }
            Ordering::Equal => {
                sync_state.first_mempool_sequence = None;
            }
            Ordering::Greater => {
                // TODO: ignore for now
                /*
                return Err(SyncTaskError::FirstMempoolSequence(
                    first_mempool_seq,
                ));
                */
                tracing::warn!(
                    %mempool_seq,
                    "Expected first mempool seq to be <= {first_mempool_seq}"
                );
                sync_state.first_mempool_sequence = None;
            }
        }
    }
    match event {
        TxHashEvent::Added => {
            if sync_state.tx_cache.contains_key(&txid) {
                // The bytes are here, the fee may not be (a reorg hands back
                // every tx of the block at once: queued here, they batch). The
                // node has it again, so `unavailable` is out of date, and
                // left set it would end this very insert as "left the mempool".
                if sync_state.unavailable_txs.remove(&txid) {
                    for (restored_txid, restored_tx) in inner
                        .abandoned_pool
                        .restore_descendant_txs(&txid)
                        .into_iter()
                        .rev()
                    {
                        inner.unfiltered_mempool.txs.remove(&restored_txid);
                        sync_state.tx_cache.insert(restored_txid, restored_tx);
                        sync_state
                            .action_queue
                            .push_front(SyncAction::InsertTx(restored_txid));
                    }
                }
                if !sync_state.known_fees.contains_key(&txid) {
                    sync_state.request_queue.push_back(RequestItem::Fee(txid));
                }
                return Ok(());
            }
            sync_state.txs_needed.replace(txid);
            if sync_state.unavailable_txs.remove(&txid) {
                // Push to the front of the request queue, so that previously
                // abandoned txs can be added to the mempool
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(txid, true));
                let restored_txs =
                    inner.abandoned_pool.restore_descendant_txs(&txid);
                for (restored_txid, restored_tx) in
                    restored_txs.into_iter().rev()
                {
                    // The abandoned tx was recorded in the unfiltered mempool
                    // when it was parked; left there, the re-insert below is
                    // silently swallowed by the already-present short-circuit
                    // in `try_add_tx_from_caches` and the tx is never
                    // admitted. (issue #611)
                    inner.unfiltered_mempool.txs.remove(&restored_txid);
                    sync_state.tx_cache.insert(restored_txid, restored_tx);
                    // Push to the front of the action queue, so that
                    // previously abandoned txs can be added into the mempool
                    sync_state
                        .action_queue
                        .push_front(SyncAction::InsertTx(restored_txid));
                }
            } else {
                sync_state
                    .request_queue
                    .push_back(RequestItem::Tx(txid, true));
            }
            tracing::trace!(%txid, "Added tx to req queue");
        }
        TxHashEvent::Removed => {
            tracing::trace!(%txid, "Removed tx from req queue");
            sync_state.txs_needed.remove(&txid);
            sync_state.unavailable_txs.insert(txid);
            sync_state
                .request_queue
                .remove(&RequestItem::Tx(txid, true));
            sync_state.request_queue.remove(&RequestItem::Fee(txid));
        }
    }
    Ok(())
}

fn handle_seq_message<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    seq_msg: SequenceMessage,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    match seq_msg {
        SequenceMessage::BlockHash(block_hash_msg) => {
            let () = handle_block_hash_msg(inner, sync_state, block_hash_msg);
        }
        SequenceMessage::TxHash(tx_hash_msg) => {
            let () = handle_tx_hash_msg(inner, sync_state, tx_hash_msg)?;
        }
    }
    sync_state
        .action_queue
        .push_back(SyncAction::SequenceMessage(seq_msg));
    Ok(())
}

async fn handle_resp_block<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    resp_block: bitcoin_jsonrpsee::client::Block<true>,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    sync_state.blocks_needed.remove(&resp_block.hash);
    let block_txids: HashSet<Txid> =
        resp_block.tx.iter().map(|tx_info| tx_info.txid).collect();
    for tx_info in resp_block.tx.iter().rev() {
        sync_state.unavailable_txs.remove(&tx_info.txid);
        for (restored_txid, restored_tx) in inner
            .abandoned_pool
            .restore_descendant_txs(&tx_info.txid)
            .into_iter()
            .rev()
        {
            sync_state.tx_cache.insert(restored_txid, restored_tx);
            // A parked descendant that is itself confirmed by this very block
            // (parent and child mined together) must NOT be re-inserted into
            // the mempool: it would put an already-mined tx into every block
            // template — mining it is `bad-txns-inputs-missingorspent`
            // (issue #611, the stale-template half, observed live as tx
            // bfea81ef… restored-and-proposed right after the block that
            // mined it). Caching it above is enough — that is all a confirmed
            // tx is needed for.
            if block_txids.contains(&restored_txid) {
                continue;
            }
            // Parked txs stay recorded in the unfiltered mempool; drop that
            // record or the re-insert below is silently swallowed by the
            // already-present short-circuit in `try_add_tx_from_caches` and
            // the restored tx is never admitted. (issue #611)
            inner.unfiltered_mempool.txs.remove(&restored_txid);
            sync_state
                .action_queue
                .push_front(SyncAction::InsertTx(restored_txid));
        }
    }
    match sync_state
        .action_queue
        .front()
        .as_ref()
        .map(|front| &front.action)
    {
        Some(SyncAction::SequenceMessage(SequenceMessage::BlockHash(
            BlockHashMessage {
                block_hash,
                event: BlockHashEvent::Connected,
                ..
            },
        ))) if *block_hash == resp_block.hash => {
            let reconsider = {
                let sync_state = SyncStateBorrowedMut {
                    blocks_needed: &mut sync_state.blocks_needed,
                    rejected_blocks: &mut sync_state.rejected_blocks,
                    rejected_txs: &mut sync_state.rejected_txs,
                    request_queue: &sync_state.request_queue,
                    tx_cache: &mut sync_state.tx_cache,
                    txs_needed: &mut sync_state.txs_needed,
                    unavailable_txs: &mut sync_state.unavailable_txs,
                    known_fees: &mut sync_state.known_fees,
                    mempool_txids: &mut sync_state.mempool_txids,
                };
                connect_block(inner, sync_state, &resp_block).await?
            };
            sync_state.action_queue.pop_front();
            for txid in reconsider.into_iter().rev() {
                sync_state
                    .action_queue
                    .push_front(SyncAction::InsertTx(txid));
            }
        }
        Some(SyncAction::SequenceMessage(SequenceMessage::BlockHash(
            BlockHashMessage {
                block_hash,
                event: BlockHashEvent::Disconnected,
                ..
            },
        ))) if *block_hash == resp_block.hash
            && inner.mempool.chain.tip == resp_block.hash =>
        {
            if handle_disconnected_block(
                inner,
                &mut sync_state.blocks_needed,
                &mut sync_state.rejected_txs,
                &sync_state.request_queue,
                &resp_block,
            )
            .await?
            {
                sync_state.action_queue.pop_front();
            }
        }
        Some(_) | None => (),
    }
    inner
        .mempool
        .chain
        .blocks
        .insert(resp_block.hash, resp_block);
    Ok(())
}

/// Whether an arriving mempool tx's parent has to be fetched.
///
/// A parent is wanted for one thing: when it is itself in the mempool, the
/// dependency state that decides whether this tx must be rejected or abandoned
/// with it. The fee comes from the node, so a confirmed parent has nothing to
/// offer, and without `txindex` the node could not return one anyway.
///
/// `unfiltered_txs` is the live mirror of the node's mempool. The
/// `mempool_txids` snapshot is frozen at initial sync, so on its own it
/// classifies every parent the node accepted after startup as confirmed and
/// skips the fetch; the child's resolution then discovers the parent is a
/// mempool tx and requests it a round trip later. Consulting both sets (as
/// `try_get_parent_txs_from_caches` does) requests it now.
fn needs_parent_fetch(
    sync_state: &SyncState,
    unfiltered_txs: &HashSet<Txid>,
    input_txid: &Txid,
) -> bool {
    if sync_state.tx_cache.contains_key(input_txid) {
        return false;
    }
    sync_state.mempool_txids.contains(input_txid)
        || unfiltered_txs.contains(input_txid)
}

fn handle_resp_tx(sync_state: &mut SyncState, tx: Transaction) {
    let txid = tx.compute_txid();
    sync_state.txs_needed.remove(&txid);
    sync_state.tx_cache.insert(txid, tx);
}

/// First rejected parent, if any were rejected; otherwise
/// Required parent txs, if any must be requested; otherwise
/// Unavailable parent txs, if any are not available; otherwise
/// Abandoned parent txs, if any are abandoned; otherwise nothing stands in the
/// way
enum ParentTxsResult {
    /// First rejected parent tx, by vin
    Rejected(Txid),
    /// Required parent txs, sorted by vin
    Required(LinkedHashSet<Txid>),
    /// Unavailable parent txs, sorted by vin
    Unavailable(LinkedHashSet<Txid>),
    /// Abandoned parent txs, sorted by vin
    Abandoned(LinkedHashSet<Txid>),
    /// Every parent in the mempool is in the enforced mempool. A confirmed
    /// parent is neither fetched nor looked at.
    Available,
}

/// Everything consulted when resolving a tx's parents
#[derive(Clone, Copy)]
struct ParentCaches<'a> {
    abandoned_pool: &'a AbandonedPool,
    mempool: &'a Mempool,
    rejected_txs: &'a HashSet<Txid>,
    tx_cache: &'a HashMap<Txid, Transaction>,
    unavailable_txs: &'a HashSet<Txid>,
    mempool_txids: &'a HashSet<Txid>,
    /// Live mirror of the node's mempool (`unfiltered_mempool.txs`).
    /// `mempool_txids` is a snapshot frozen at initial sync, so it knows
    /// nothing about txs the node accepted afterwards; a parent is known to be
    /// an unconfirmed mempool tx if it appears in EITHER set.
    unfiltered_txs: &'a HashSet<Txid>,
}

fn try_get_parent_txs_from_caches(
    caches: ParentCaches<'_>,
    vin: &[TxIn],
) -> ParentTxsResult {
    let ParentCaches {
        abandoned_pool,
        mempool,
        rejected_txs,
        tx_cache,
        unavailable_txs,
        mempool_txids,
        unfiltered_txs,
    } = caches;
    // Whether a parent is known to be an unconfirmed tx in the node's mempool.
    // `mempool_txids` covers the initial-sync snapshot; `unfiltered_txs` is the
    // live mirror maintained from the sequence stream and covers everything the
    // node accepted after that snapshot. Consulting only the frozen snapshot
    // classified every post-startup mempool parent as "confirmed", which let
    // the tx cache stand in for it below — admitting its child as a rootless
    // orphan (issue #611, the live recurrence).
    let is_node_mempool_tx = |input_txid: &Txid| {
        mempool_txids.contains(input_txid)
            || unfiltered_txs.contains(input_txid)
    };
    let mut abandoned_input_txs = LinkedHashSet::new();
    let mut input_txs_needed = LinkedHashSet::new();
    let mut unavailable_input_txs = LinkedHashSet::new();
    for input in vin {
        let OutPoint {
            txid: input_txid,
            vout: _,
        } = input.previous_output;
        if rejected_txs.contains(&input_txid) {
            return ParentTxsResult::Rejected(input_txid);
        } else if tx_cache.contains_key(&input_txid)
            && (!is_node_mempool_tx(&input_txid)
                || mempool.txs.0.contains_key(&input_txid))
        {
            // A cache hit says nothing about an unconfirmed *mempool* parent,
            // which must be present in the enforced mempool before its child is
            // admitted: standing in for that presence would insert the child as
            // a rootless orphan and the block template it lands in is
            // closure-broken (`bad-txns-inputs-missingorspent`). When such a
            // parent is only cached, fall through so the child is deferred
            // (`Required`) until the parent is actually inserted. (issue #611)
        } else if mempool.txs.0.contains_key(&input_txid) {
        } else if abandoned_pool.contains(&input_txid) {
            abandoned_input_txs.replace(input_txid);
        } else if unavailable_txs.contains(&input_txid) {
            unavailable_input_txs.replace(input_txid);
        } else if !is_node_mempool_tx(&input_txid) {
            // Confirmed parent. The action queue is strictly FIFO, so the
            // mirror is the node's mempool as of this tx's announcement: an
            // unconfirmed parent would be in it by now.
        } else {
            input_txs_needed.replace(input_txid);
        }
    }
    if !input_txs_needed.is_empty() {
        ParentTxsResult::Required(input_txs_needed)
    } else if !unavailable_input_txs.is_empty() {
        ParentTxsResult::Unavailable(unavailable_input_txs)
    } else if !abandoned_input_txs.is_empty() {
        ParentTxsResult::Abandoned(abandoned_input_txs)
    } else {
        ParentTxsResult::Available
    }
}

// Returns `Success` if the tx was added to the mempool or abandoned pool,
// already exists in the unfiltered mempool, was already marked unavailable, or
// was rejected. `Success` may carry front-of-queue inserts: parents that are
// already fetched but whose own insert is still queued *behind* this tx, which
// must be applied first (followed by this tx again).
// Returns `Pending` if the tx, its parent txs or its fee must be fetched first.
fn try_add_tx_from_caches<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    txid: Txid,
) -> Result<ApplySyncActionResult, SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    if inner.unfiltered_mempool.txs.contains(&txid) {
        return Ok(ApplySyncActionResult::from(true));
    }
    let Some(tx) = sync_state.tx_cache.get(&txid) else {
        if sync_state.unavailable_txs.contains(&txid) {
            inner.unfiltered_mempool.txs.insert(txid);
            return Ok(ApplySyncActionResult::from(true));
        } else {
            sync_state
                .request_queue
                .push_front(RequestItem::Tx(txid, true));
            return Ok(ApplySyncActionResult::Pending);
        }
    };
    let known_fee = sync_state.known_fees.get(&txid).copied();
    let caches = ParentCaches {
        abandoned_pool: &inner.abandoned_pool,
        mempool: &inner.mempool,
        rejected_txs: sync_state.rejected_txs,
        tx_cache: sync_state.tx_cache,
        unavailable_txs: sync_state.unavailable_txs,
        mempool_txids: &*sync_state.mempool_txids,
        unfiltered_txs: &inner.unfiltered_mempool.txs,
    };
    match try_get_parent_txs_from_caches(caches, &tx.input) {
        ParentTxsResult::Rejected(rejected_parent) => {
            // Reject tx
            tracing::trace!(
                %txid,
                %rejected_parent,
                "rejecting tx: rejected parent",
            );
            sync_state.rejected_txs.insert(txid);
            sync_state
                .request_queue
                .push_front(RequestItem::RejectTx(txid));
            inner.unfiltered_mempool.txs.insert(txid);
            Ok(ApplySyncActionResult::from(true))
        }
        ParentTxsResult::Unavailable(unavailable_parents) => {
            let tx = sync_state
                .tx_cache
                .remove(&txid)
                .expect("tx should be present in tx cache");
            // TX is abandoned, add to abandoned pool
            inner
                .abandoned_pool
                .insert(tx, unavailable_parents.into_iter().collect());
            tracing::trace!(%txid, "added tx to abandoned pool");
            inner.unfiltered_mempool.txs.insert(txid);
            Ok(ApplySyncActionResult::from(true))
        }
        ParentTxsResult::Required(required_parents) => {
            // A required parent that is already in the tx cache needs no
            // fetch: it reached `Required` because it is a known node-mempool
            // tx whose own insert has not applied yet. Since the action queue
            // is strictly FIFO, that insert can be queued BEHIND this tx (an
            // out-of-order initial snapshot, or a child announced right after
            // its parent) — returning `Pending` would then deadlock the queue
            // head until the apply timeout kills the task. Instead, requeue:
            // apply the parents' inserts first, then retry this tx.
            let (queued_parents, fetch_parents): (Vec<Txid>, Vec<Txid>) =
                required_parents.into_iter().partition(|input_txid| {
                    sync_state.tx_cache.contains_key(input_txid)
                });
            if fetch_parents.is_empty() {
                let mut push_txs_action_queue_front = queued_parents;
                for parent_txid in &push_txs_action_queue_front {
                    // A parent can linger in the unfiltered mempool without
                    // any terminal state (it reached `Required`, so it is in
                    // none of them) — e.g. a tx restored from the abandoned
                    // pool that was never scrubbed from the unfiltered set.
                    // Left in place, its requeued insert is swallowed by the
                    // already-present short-circuit above and this tx requeues
                    // the same parent forever. Scrub it so the insert makes
                    // progress. (issue #611)
                    inner.unfiltered_mempool.txs.remove(parent_txid);
                }
                push_txs_action_queue_front.push(txid);
                return Ok(ApplySyncActionResult::Success {
                    push_txs_action_queue_front,
                });
            }
            for input_txid in fetch_parents.into_iter().rev() {
                sync_state.txs_needed.replace(input_txid);
                sync_state.txs_needed.to_front(&input_txid);
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
            Ok(ApplySyncActionResult::Pending)
        }
        ParentTxsResult::Abandoned(_abandoned_parents) => {
            let tx = sync_state
                .tx_cache
                .remove(&txid)
                .expect("tx should be present in tx cache");
            inner.abandoned_pool.insert(tx, HashSet::new());
            tracing::trace!(%txid, "added tx to abandoned pool");
            inner.unfiltered_mempool.txs.insert(txid);
            Ok(ApplySyncActionResult::from(true))
        }
        ParentTxsResult::Available => {
            let Some(fee_delta) = known_fee else {
                if sync_state.unavailable_txs.contains(&txid) {
                    // Left the node's mempool before we learned its fee. It
                    // is never admitted without one. Recorded like any other
                    // unavailable tx, for its `Removed` or its block to clear.
                    tracing::trace!(%txid, "tx left the mempool, not admitted");
                    inner.unfiltered_mempool.txs.insert(txid);
                    return Ok(ApplySyncActionResult::from(true));
                }
                sync_state.request_queue.push_front(RequestItem::Fee(txid));
                return Ok(ApplySyncActionResult::Pending);
            };
            match inner
                .enforcer
                .borrow_mut()
                .accept_tx(tx)
                .map_err(cusf_enforcer::Error::AcceptTx)?
            {
                cusf_enforcer::TxAcceptAction::Accept {
                    conflicts_with,
                    weight_tweak,
                } => {
                    let modified_weight_wu =
                        tx.weight().to_wu().saturating_add_signed(weight_tweak);
                    let modified_weight = Weight::from_wu(modified_weight_wu);
                    match inner.mempool.insert(
                        tx.clone(),
                        fee_delta,
                        conflicts_with.into(),
                        modified_weight,
                    ) {
                        Ok(_) => (),
                        // Mirroring someone else's mempool means the same tx
                        // can be announced to us twice. It happens whenever a
                        // block we rejected is invalidated: we never connected
                        // it, so we never removed its transactions, and now
                        // bitcoind hands them back. We still have it, so there
                        // is nothing to insert.
                        Err(MempoolInsertError::TxAlreadyExists { .. }) => {
                            tracing::trace!(
                                %txid,
                                "tx already in mempool, ignoring re-announcement",
                            );
                        }
                        Err(err) => return Err(err.into()),
                    }
                    inner.unfiltered_mempool.txs.insert(txid);
                    inner.reconsider_attempts.remove(&txid);
                    tracing::trace!(%txid, "added tx to mempool");
                    Ok(ApplySyncActionResult::from(true))
                }
                cusf_enforcer::TxAcceptAction::Reject => {
                    tracing::trace!(%txid, "rejecting tx");
                    // remove all descendants
                    let rejected_mempool_txs =
                        inner.mempool.remove_with_descendants(&txid)?;
                    let mut rejected_txs = HashSet::new();
                    for rejected_tx in std::iter::once(txid).chain(
                        rejected_mempool_txs.into_iter().map(|(txid, _)| txid),
                    ) {
                        rejected_txs.extend(
                            inner
                                .abandoned_pool
                                .remove_descendant_txs(&rejected_tx)
                                .into_iter()
                                .map(|(txid, _)| txid),
                        );
                        rejected_txs.insert(rejected_tx);
                    }
                    for rejected_tx in rejected_txs {
                        sync_state.rejected_txs.insert(rejected_tx);
                        sync_state
                            .request_queue
                            .push_front(RequestItem::RejectTx(rejected_tx));
                    }
                    inner.unfiltered_mempool.txs.insert(txid);
                    Ok(ApplySyncActionResult::from(true))
                }
            }
        }
    }
}

async fn try_apply_seq_message<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: SyncStateBorrowedMut<'_>,
    seq_msg: &SequenceMessage,
) -> Result<ApplySyncActionResult, SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    match seq_msg {
        SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Disconnected,
            ..
        }) => {
            if inner.mempool.chain.tip != *block_hash {
                if sync_state.rejected_blocks.contains(block_hash) {
                    // We never connected this block, so there is nothing to
                    // disconnect. Sync actions apply in order, so any connect for
                    // the same block has already been handled by the time this one
                    // reaches the head of the queue.
                    //
                    // Waiting instead would stall the queue permanently. Nothing
                    // requests the block, the tip cannot become it, and the action
                    // stays at the head until the apply timeout kills the task.
                    tracing::debug!(
                        %block_hash,
                        tip = %inner.mempool.chain.tip,
                        "ignoring disconnect for a block that was never connected",
                    );
                    return Ok(ApplySyncActionResult::from(true));
                } else {
                    return Ok(ApplySyncActionResult::Pending);
                }
            }
            let Some(block) =
                inner.mempool.chain.blocks.get(block_hash).cloned()
            else {
                return Ok(ApplySyncActionResult::Pending);
            };
            let applied = handle_disconnected_block(
                inner,
                sync_state.blocks_needed,
                sync_state.rejected_txs,
                sync_state.request_queue,
                &block,
            )
            .await?;
            Ok(ApplySyncActionResult::from(applied))
        }
        SequenceMessage::TxHash(TxHashMessage {
            txid,
            event: TxHashEvent::Added,
            mempool_seq: _,
            zmq_seq: _,
        }) => {
            // Do NOT clear `unavailable_txs` here. The stale-verdict clearing
            // that issue #611 needs — "the node re-announced this tx, so an
            // earlier `unavailable` verdict is stale" — already happens exactly
            // once, at *arrival*, in `handle_tx_hash_msg` (which also queues
            // the fresh fetch and restores abandoned descendants).
            //
            // This apply path is different: the same queued `Added` action is
            // re-applied on every drive while its fetch is pending
            // (`try_add_tx_from_caches` returns `Pending` without popping it).
            // Clearing the mark here re-ran on each of those re-applications,
            // which removed the only terminal state a genuinely-gone tx has:
            // when the fetch lost a race with a removal (an ordinary RBF while
            // `getrawtransaction` was in flight), `handle_resp` marked the tx
            // unavailable, the re-drive re-applied this action, this line
            // un-marked it, the tx was re-fetched, the fetch failed the same
            // way, and round it went — a silent livelock that admitted nothing
            // and flooded the node with `getrawtransaction`. Leaving the mark
            // intact lets the re-application terminate on the `unavailable`
            // branch of `try_add_tx_from_caches` instead.
            try_add_tx_from_caches(inner, sync_state, *txid)
        }
        SequenceMessage::TxHash(TxHashMessage {
            txid,
            event: TxHashEvent::Removed,
            mempool_seq: _,
            zmq_seq: _,
        }) => {
            if inner.unfiltered_mempool.txs.remove(txid) {
                inner.mempool.remove(txid)?;
                inner.abandoned_pool.remove(txid);
                inner.reconsider_attempts.remove(txid);
                sync_state.mempool_txids.remove(txid);
                sync_state.known_fees.remove(txid);
                Ok(ApplySyncActionResult::from(true))
            } else {
                Err(SyncTaskError::UnfilteredMempoolMissingTx(*txid))
            }
        }
        SequenceMessage::BlockHash(BlockHashMessage {
            block_hash,
            event: BlockHashEvent::Connected,
            ..
        }) => {
            let Some(block) = inner.mempool.chain.blocks.get(block_hash) else {
                tracing::debug!(
                    %block_hash,
                    "waiting for block to connect"
                );
                return Ok(ApplySyncActionResult::Pending);
            };
            let reconsider =
                connect_block(inner, sync_state, &block.clone()).await?;
            Ok(ApplySyncActionResult::Success {
                push_txs_action_queue_front: reconsider,
            })
        }
    }
}

// returns `true` if an item was applied successfully
async fn try_apply_next_sync_action<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
) -> Result<bool, SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    let Some(next_sync_action) = sync_state.action_queue.front() else {
        tracing::trace!("no sync actions to apply in queue");
        return Ok(false);
    };
    let res = match next_sync_action.action {
        SyncAction::InsertTx(txid) => {
            let sync_state = SyncStateBorrowedMut {
                blocks_needed: &mut sync_state.blocks_needed,
                rejected_blocks: &mut sync_state.rejected_blocks,
                rejected_txs: &mut sync_state.rejected_txs,
                request_queue: &sync_state.request_queue,
                tx_cache: &mut sync_state.tx_cache,
                txs_needed: &mut sync_state.txs_needed,
                unavailable_txs: &mut sync_state.unavailable_txs,
                known_fees: &mut sync_state.known_fees,
                mempool_txids: &mut sync_state.mempool_txids,
            };
            try_add_tx_from_caches(inner, sync_state, txid)?
        }
        SyncAction::SequenceMessage(seq_msg) => {
            let sync_state = SyncStateBorrowedMut {
                blocks_needed: &mut sync_state.blocks_needed,
                rejected_blocks: &mut sync_state.rejected_blocks,
                rejected_txs: &mut sync_state.rejected_txs,
                request_queue: &sync_state.request_queue,
                tx_cache: &mut sync_state.tx_cache,
                txs_needed: &mut sync_state.txs_needed,
                unavailable_txs: &mut sync_state.unavailable_txs,
                known_fees: &mut sync_state.known_fees,
                mempool_txids: &mut sync_state.mempool_txids,
            };
            try_apply_seq_message(inner, sync_state, &seq_msg).await?
        }
    };
    match res {
        ApplySyncActionResult::Success {
            push_txs_action_queue_front,
        } => {
            sync_state.action_queue.pop_front();
            for tx in push_txs_action_queue_front.into_iter().rev() {
                sync_state.action_queue.push_front(SyncAction::InsertTx(tx));
            }
            Ok(true)
        }
        ApplySyncActionResult::Pending => Ok(false),
    }
}

async fn handle_resp<Enforcer, BorrowedEnforcer>(
    inner: &mut MempoolSyncInner<Enforcer>,
    sync_state: &mut SyncState,
    resp: BatchedResponseItem,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
{
    match resp {
        BatchedResponseItem::BatchTx {
            fetched,
            unavailable,
        } => {
            // Lost the race with a `Removed`, see `tx_fetch_result`.
            for txid in unavailable {
                tracing::debug!(
                    %txid,
                    "dropping batched fetch for a tx that left the mempool"
                );
                sync_state.txs_needed.remove(&txid);
                sync_state.unavailable_txs.insert(txid);
            }
            let mut input_txs_needed = LinkedHashSet::new();
            for (tx, in_mempool) in fetched {
                if in_mempool {
                    // Asked for here, not when each tx reaches the head of the
                    // queue, so a burst costs one call per batch.
                    let txid = tx.compute_txid();
                    if !sync_state.known_fees.contains_key(&txid)
                        && !sync_state.unavailable_txs.contains(&txid)
                    {
                        sync_state
                            .request_queue
                            .push_front(RequestItem::Fee(txid));
                    }
                    for input_txid in
                        tx.input.iter().map(|input| input.previous_output.txid)
                    {
                        if !needs_parent_fetch(
                            sync_state,
                            &inner.unfiltered_mempool.txs,
                            &input_txid,
                        ) {
                            continue;
                        }
                        input_txs_needed.replace(input_txid);
                    }
                }
                let () = handle_resp_tx(sync_state, tx);
            }
            sync_state
                .txs_needed
                .extend(input_txs_needed.iter().copied());
            for input_txid in input_txs_needed.into_iter().rev() {
                sync_state
                    .request_queue
                    .push_front(RequestItem::Tx(input_txid, false))
            }
        }
        BatchedResponseItem::BatchFee { fees, gone } => {
            for (txid, fee) in fees {
                // A `Removed` overtook the answer: keep it out, nothing would
                // ever forget it.
                if !sync_state.unavailable_txs.contains(&txid) {
                    sync_state.known_fees.insert(txid, fee);
                }
            }
            for txid in gone {
                tracing::debug!(
                    %txid,
                    "dropping fee fetch for a tx that left the mempool"
                );
                sync_state.txs_needed.remove(&txid);
                sync_state
                    .request_queue
                    .remove(&RequestItem::Tx(txid, true));
                sync_state.unavailable_txs.insert(txid);
            }
        }
        BatchedResponseItem::Single(ResponseItem::Block(block)) => {
            tracing::debug!(%block.hash, "Handling block response");
            let () = handle_resp_block(inner, sync_state, *block).await?;
        }
        BatchedResponseItem::BatchRejectTx
        | BatchedResponseItem::BatchUnrejectTx
        | BatchedResponseItem::Single(ResponseItem::RejectBlock)
        | BatchedResponseItem::Single(ResponseItem::RejectTx)
        | BatchedResponseItem::Single(ResponseItem::UnrejectTx) => {}
    }
    while try_apply_next_sync_action(inner, sync_state).await? {}
    Ok(())
}

trait ContinueLoop<const EXIT_AFTER_SYNC: bool> {
    async fn continue_loop<Enforcer>(
        inner: &TokioRwLock<MempoolSyncInner<Enforcer>>,
        sync_state: &SyncState,
    ) -> bool;
}

struct ContinueLoopImpl;

impl ContinueLoop<true> for ContinueLoopImpl {
    async fn continue_loop<Enforcer>(
        inner: &TokioRwLock<MempoolSyncInner<Enforcer>>,
        sync_state: &SyncState,
    ) -> bool {
        let inner_read = inner.read().await;
        !(MempoolSyncingBorrowed {
            inner: &inner_read,
            sync_state,
        }
        .is_synced())
    }
}

impl ContinueLoop<false> for ContinueLoopImpl {
    async fn continue_loop<Enforcer>(
        _inner: &TokioRwLock<MempoolSyncInner<Enforcer>>,
        _sync_state: &SyncState,
    ) -> bool {
        true
    }
}

/// If `EXIT_AFTER_SYNC` is `true`, then returns once syncing is complete.
/// Otherwise, continues until the shutdown signal is encountered.
async fn task_inner<
    const EXIT_AFTER_SYNC: bool,
    Enforcer,
    BorrowedEnforcer,
    ShutdownSignal,
>(
    inner: &TokioRwLock<MempoolSyncInner<Enforcer>>,
    sync_state: &mut SyncState,
    combined_stream: &mut CombinedStream<
        'static,
        'static,
        futures::future::Fuse<BoxFuture<'static, ()>>,
        ShutdownSignal,
    >,
) -> Result<(), SyncTaskError<BorrowedEnforcer>>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
    ShutdownSignal: FusedFuture<Output = ()> + Send + Unpin,
    ContinueLoopImpl: ContinueLoop<EXIT_AFTER_SYNC>,
{
    while <ContinueLoopImpl as ContinueLoop<EXIT_AFTER_SYNC>>::continue_loop(
        inner, sync_state,
    )
    .await
    {
        let msg = combined_stream
            .next()
            .await
            .ok_or(SyncTaskError::CombinedStreamEnded)?;
        let msg_kind = match &msg {
            CombinedStreamItem::ZmqSeq(_) => "sequence",
            CombinedStreamItem::Response(_) => "response",
            CombinedStreamItem::ApplySyncActionTimeout => {
                "apply seq action timeout"
            }
            CombinedStreamItem::Shutdown => "shutdown",
        };

        let span = tracing::debug_span!(
            "handle_stream_msg",
            sequence_id = ulid::Ulid::generate().to_string(), // ULIDs are clickable, short and sorts naturally by time
        );

        tracing::debug!(parent: &span, "Handling {msg_kind} stream message");

        match msg {
            CombinedStreamItem::ZmqSeq(seq_msg) => {
                let seq_msg = seq_msg?;
                let mut fut = async || {
                    let mut inner_write = inner.write().await;
                    let () = handle_seq_message(
                        &mut inner_write,
                        sync_state,
                        seq_msg,
                    )?;
                    while try_apply_next_sync_action(
                        &mut inner_write,
                        sync_state,
                    )
                    .await?
                    {}
                    Ok::<_, SyncTaskError<_>>(())
                };
                let () = fut().instrument(span).await?;
                combined_stream.apply_sync_action_timeout =
                    apply_sync_action_timeout(&sync_state.action_queue).fuse();
            }
            CombinedStreamItem::Response(resp) => {
                // Losing the race to fetch a tx that has since left the
                // mempool is not an error here. It comes back as a member of
                // `BatchTx::unavailable`.
                let resp = resp?;
                {
                    let mut inner_write = inner.write().await;
                    let () = handle_resp(&mut inner_write, sync_state, resp)
                        .instrument(span)
                        .await?;
                }
                combined_stream.apply_sync_action_timeout =
                    apply_sync_action_timeout(&sync_state.action_queue).fuse();
            }
            CombinedStreamItem::ApplySyncActionTimeout => {
                let err = ApplySyncActionTimeoutError {
                    action: sync_state
                        .action_queue
                        .front()
                        .as_ref()
                        .map(|head| head.action),
                };
                return Err(err.into());
            }
            CombinedStreamItem::Shutdown => {
                tracing::info!(parent: &span, "shutdown signal received, aborting");
                return Err(SyncTaskError::Shutdown);
            }
        }
    }
    Ok(())
}

pub struct MempoolSynced<Enforcer, ShutdownSignal>
where
    ShutdownSignal: Future,
{
    // Uses a dummy value for the enforcer argument.
    // The caller must provide an owned enforcer when creating a `MempoolSync`
    // from this.
    inner: MempoolSyncing<()>,
    combined_stream: CombinedStream<
        'static,
        'static,
        futures::future::Fuse<BoxFuture<'static, ()>>,
        futures::future::Shared<ShutdownSignal>,
    >,
    _marker: PhantomData<Enforcer>,
}

/// Seed the tx cache from Core's `mempool.dat`, best-effort.
fn seed_tx_cache_from_dat(
    path: &Path,
    mempool_txids: &[Txid],
    tx_cache: &mut HashMap<Txid, Transaction>,
) {
    let start = std::time::Instant::now();
    let mut dat = match crate::mempool::dat::read_mempool_dat(path) {
        Ok(dat) => dat,
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                "could not read mempool dump, syncing entirely over RPC: {err:#}"
            );
            return;
        }
    };
    if let Some(at) = dat.truncated_at {
        tracing::debug!(
            "mempool dump was only readable up to entry {at} of {}",
            dat.declared
        );
    }
    for txid in mempool_txids {
        if let Some(tx) = dat.txs.remove(txid) {
            tx_cache.insert(*txid, tx);
        }
    }
    tracing::info!(
        "seeded {} of {} mempool txs from {} in {:?}, {} left for RPC",
        tx_cache.len(),
        mempool_txids.len(),
        path.display(),
        start.elapsed(),
        mempool_txids.len() - tx_cache.len(),
    );
}

pub async fn init_sync_mempool<
    Enforcer,
    BorrowedEnforcer,
    RpcClient,
    ShutdownSignal,
>(
    mut enforcer: Enforcer,
    rpc_client: RpcClient,
    zmq_addr_sequence: &str,
    // Optional path to the node's `mempool.dat`. When set, it is used as a
    // fast path for the initial sync.
    mempool_dat_path: Option<&Path>,
    // Would it be better to return a Some/None, indicating sync stoppage?
    shutdown_signal: ShutdownSignal,
) -> Result<
    MempoolSynced<BorrowedEnforcer, ShutdownSignal>,
    SyncTaskError<BorrowedEnforcer>,
>
where
    Enforcer: BorrowMut<BorrowedEnforcer>,
    BorrowedEnforcer: CusfEnforcer,
    RpcClient: bitcoin_jsonrpsee::client::MainClient + Send + Sync + 'static,
    ShutdownSignal: FusedFuture<Output = ()> + Send + Unpin,
{
    let shutdown_signal = shutdown_signal.shared();
    let (best_block_hash, sequence_stream) = cusf_enforcer::initial_sync(
        enforcer.borrow_mut(),
        &rpc_client,
        zmq_addr_sequence,
        shutdown_signal.clone(),
    )
    .await?;
    let RawMempoolWithSequence {
        txids,
        mempool_sequence,
    } = rpc_client
        .get_raw_mempool(BoolWitness::<false>, BoolWitness::<true>)
        .await?;

    // Ask Core for the fees it has already computed. Without this, the sync
    // fetches every tx's parents purely to sum the values of the outputs being
    // spent, which on a mainnet mempool measured as ~95% of the sync's wall clock.
    //
    // Best-effort: Core refuses `verbose` together with `mempool_sequence`, so
    // this is a second call and its snapshot can differ from the one above. A
    // tx missing from it gets its fee from `getmempoolentry`, like every tx
    // that arrives later.
    let known_fees: HashMap<Txid, Amount> = {
        let started = std::time::Instant::now();
        match rpc_client
            .get_raw_mempool(BoolWitness::<true>, BoolWitness::<false>)
            .await
        {
            Ok(verbose) => {
                let fees: HashMap<Txid, Amount> = verbose
                    .entries
                    .into_iter()
                    .map(|(txid, info)| (txid, info.fees.base))
                    .collect();
                tracing::debug!(
                    "took fees for {} of {} mempool txs from the node in {:?}",
                    fees.len(),
                    txids.len(),
                    started.elapsed(),
                );
                fees
            }
            Err(err) => {
                tracing::warn!(
                    "could not read mempool fees from the node, \
                     asking for them tx by tx instead: {err}"
                );
                HashMap::new()
            }
        }
    };
    let mempool_txids: HashSet<Txid> = txids.iter().copied().collect();
    let (tip_watch, _) = watch::channel(best_block_hash);
    let inner = MempoolSyncInner {
        abandoned_pool: AbandonedPool::default(),
        reconsider_attempts: HashMap::new(),
        enforcer,
        mempool: Mempool::new(best_block_hash),
        tip_watch,
        unfiltered_mempool: UnfilteredMempool {
            tip: best_block_hash,
            txs: HashSet::new(),
        },
    };
    let inner = TokioRwLock::new(inner);
    let mut sync_state = {
        let request_queue = RequestQueue::default();
        request_queue.push_back(RequestItem::Block(best_block_hash));
        let mut tx_cache = HashMap::new();
        if let Some(path) = mempool_dat_path {
            seed_tx_cache_from_dat(path, &txids, &mut tx_cache);
        }
        for txid in &txids {
            if !tx_cache.contains_key(txid) {
                request_queue.push_back(RequestItem::Tx(*txid, true));
            }
        }
        let txs_needed = LinkedHashSet::from_iter(
            txids
                .iter()
                .copied()
                .filter(|txid| !tx_cache.contains_key(txid)),
        );
        let action_queue = SyncActionQueue::from_iter(
            txids.iter().cloned().map(SyncAction::InsertTx),
        );
        SyncState {
            action_queue,
            blocks_needed: LinkedHashSet::from_iter([best_block_hash]),
            first_mempool_sequence: Some(mempool_sequence + 1),
            rejected_blocks: HashSet::new(),
            rejected_txs: HashSet::new(),
            request_queue,
            tx_cache,
            txs_needed,
            unavailable_txs: HashSet::new(),
            known_fees,
            mempool_txids,
        }
    };

    let response_stream = sync_state
        .request_queue
        .clone()
        .then({
            let rpc_client = Arc::new(rpc_client);
            move |request| {
                let rpc_client = rpc_client.clone();
                async move {
                    batched_request::<RpcClient>(&rpc_client, request).await
                }
            }
        })
        .boxed();

    let mut combined_stream = CombinedStream::new(
        sequence_stream,
        response_stream,
        apply_sync_action_timeout(&sync_state.action_queue).fuse(),
        shutdown_signal,
    );
    let () = task_inner::<true, _, _, _>(
        &inner,
        &mut sync_state,
        &mut combined_stream,
    )
    .await?;

    let res = {
        let inner = {
            let abandoned = inner.read().await.abandoned_pool.len();
            if abandoned > 0 {
                tracing::warn!(
                    abandoned,
                    "mempool sync done with txs parked on a parent the node \
                     could not supply; they stay out of block templates"
                );
            }
            let MempoolSyncInner {
                abandoned_pool,
                enforcer: _,
                mempool,
                tip_watch,
                unfiltered_mempool,
                reconsider_attempts,
            } = inner.into_inner();
            MempoolSyncing {
                inner: MempoolSyncInner {
                    abandoned_pool,
                    enforcer: (),
                    mempool,
                    tip_watch,
                    unfiltered_mempool,
                    reconsider_attempts,
                },
                sync_state,
            }
        };
        MempoolSynced {
            inner,
            combined_stream,
            _marker: PhantomData,
        }
    };
    Ok(res)
}

pub struct MempoolSync<Enforcer> {
    inner: std::sync::Weak<TokioRwLock<MempoolSyncInner<Enforcer>>>,
    _task: tokio_util::task::AbortOnDropHandle<()>,
    /// Observes `mempool.chain.tip`, see [`Self::subscribe_tip`].
    tip_rx: watch::Receiver<BlockHash>,
}

impl<Enforcer> MempoolSync<Enforcer> {
    pub fn subscribe_tip(&self) -> watch::Receiver<BlockHash> {
        self.tip_rx.clone()
    }
}

impl<Enforcer> MempoolSync<Enforcer>
where
    Enforcer: CusfEnforcer + Send + Sync + 'static,
{
    pub fn new<ShutdownSignal, ErrHandler, ErrHandlerFut>(
        enforcer: Enforcer,
        mempool_synced: MempoolSynced<Enforcer, ShutdownSignal>,
        err_handler: ErrHandler,
    ) -> Self
    where
        ShutdownSignal: Future<Output = ()> + Send + Sync + 'static,
        ErrHandler:
            FnOnce(SyncTaskError<Enforcer>) -> ErrHandlerFut + Send + 'static,
        ErrHandlerFut: Future<Output = ()> + Send,
    {
        let MempoolSynced {
            inner,
            mut combined_stream,
            _marker,
        } = mempool_synced;
        let (inner, tip_rx, mut sync_state) = {
            let MempoolSyncing {
                inner:
                    MempoolSyncInner {
                        abandoned_pool,
                        enforcer: (),
                        mempool,
                        tip_watch,
                        unfiltered_mempool,
                        reconsider_attempts,
                    },
                sync_state,
            } = inner;
            let tip_rx = tip_watch.subscribe();
            let inner = MempoolSyncInner {
                abandoned_pool,
                enforcer,
                mempool,
                tip_watch,
                unfiltered_mempool,
                reconsider_attempts,
            };
            (inner, tip_rx, sync_state)
        };
        let inner = Arc::new(TokioRwLock::new(inner));
        let inner_weak = Arc::downgrade(&inner);
        let task = tokio::task::spawn(async move {
            match task_inner::<false, _, _, _>(
                &inner,
                &mut sync_state,
                &mut combined_stream,
            )
            .await
            {
                Ok(_) => {}
                Err(err) => err_handler(err).await,
            }
        });
        Self {
            inner: inner_weak,
            _task: tokio_util::task::AbortOnDropHandle::new(task),
            tip_rx,
        }
    }

    /// A `MempoolSync` over `mempool` with no sync task, for tests outside
    /// this module.
    #[cfg(test)]
    pub(crate) fn for_test(enforcer: Enforcer, mempool: Mempool) -> Self {
        let tip = mempool.chain.tip;
        let (tip_watch, tip_rx) = watch::channel(tip);
        let inner = Arc::new(TokioRwLock::new(MempoolSyncInner {
            abandoned_pool: AbandonedPool::default(),
            enforcer,
            mempool,
            tip_watch,
            unfiltered_mempool: UnfilteredMempool {
                tip,
                txs: HashSet::new(),
            },
            reconsider_attempts: HashMap::new(),
        }));
        let inner_weak = Arc::downgrade(&inner);
        // Stands in for the sync task, which owns the mempool.
        let task = tokio::task::spawn(async move {
            let _inner = inner;
            std::future::pending::<()>().await
        });
        Self {
            inner: inner_weak,
            _task: tokio_util::task::AbortOnDropHandle::new(task),
            tip_rx,
        }
    }

    /// Run `f` under the write lock the sync task holds while it accepts and
    /// inserts txs, for tests outside this module.
    #[cfg(test)]
    pub(crate) async fn with_mut_for_test<Output>(
        &self,
        f: impl FnOnce(&mut Mempool) -> Output,
    ) -> Option<Output> {
        let inner = self.inner.upgrade()?;
        let mut inner_write = inner.write().await;
        Some(f(&mut inner_write.mempool))
    }

    /// Apply a function over the mempool and enforcer.
    /// Returns `None` if the mempool is unavailable due to an error.
    pub async fn with<F, Output>(&self, f: F) -> Option<Output>
    where
        F: for<'a> FnOnce(&'a Mempool, &'a Enforcer) -> BoxFuture<'a, Output>,
    {
        let inner = self.inner.upgrade()?;
        let inner_read = inner.read().await;
        let res = f(&inner_read.mempool, &inner_read.enforcer).await;
        Some(res)
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::{
        Amount, BlockHash, OutPoint, ScriptBuf, Sequence, Transaction, TxIn,
        TxOut, Txid, Witness, absolute::LockTime, hashes::Hash as _,
        transaction::Version,
    };
    use futures::{FutureExt as _, StreamExt as _};
    use hashlink::LinkedHashSet;

    use super::*;
    use crate::{cusf_enforcer::DefaultEnforcer, mempool::Mempool};

    fn make_tx(inputs: &[OutPoint], output_values: &[u64]) -> Transaction {
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
            output: output_values
                .iter()
                .map(|value| TxOut {
                    value: Amount::from_sat(*value),
                    script_pubkey: ScriptBuf::new(),
                })
                .collect(),
        }
    }

    /// Regression test for the initial-sync crash reported in
    /// LayerTwo-Labs/bip300301_enforcer#406.
    ///
    /// During initial sync the RPC mempool snapshot and the live ZMQ
    /// `sequence` stream are reconciled together, so the same txid can be
    /// queued for insertion twice (e.g. once as the snapshot `InsertTx` and
    /// once as a buffered `Added` sequence message).
    ///
    /// Verifies the unfiltered mempool invariant (filtered ⊆ unfiltered).
    #[test]
    fn add_tx_from_caches_is_idempotent() {
        let genesis = BlockHash::all_zeros();
        let (tip_watch, _) = watch::channel(genesis);
        let mut inner = MempoolSyncInner {
            abandoned_pool: AbandonedPool::default(),
            reconsider_attempts: HashMap::new(),
            enforcer: DefaultEnforcer,
            mempool: Mempool::new(genesis),
            tip_watch,
            unfiltered_mempool: UnfilteredMempool {
                tip: genesis,
                txs: HashSet::new(),
            },
        };

        // A parent tx supplying a spendable output, available only via the tx
        // cache (it is not itself a mempool tx).
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let parent_txid = parent.compute_txid();
        // The tx under test, spending the parent's output.
        let tx = make_tx(&[OutPoint::new(parent_txid, 0)], &[90_000]);
        let txid = tx.compute_txid();

        let mut tx_cache = HashMap::new();
        tx_cache.insert(parent_txid, parent);
        tx_cache.insert(txid, tx);

        let mut blocks_needed = LinkedHashSet::new();
        let mut rejected_blocks = HashSet::new();
        let mut rejected_txs = HashSet::new();
        let request_queue = RequestQueue::default();
        let mut txs_needed = LinkedHashSet::new();
        let mut unavailable_txs = HashSet::new();
        let mut known_fees = HashMap::from([(txid, Amount::from_sat(10_000))]);

        // First insertion adds the tx to the mempool.
        {
            let sync_state = SyncStateBorrowedMut {
                blocks_needed: &mut blocks_needed,
                rejected_blocks: &mut rejected_blocks,
                rejected_txs: &mut rejected_txs,
                request_queue: &request_queue,
                tx_cache: &mut tx_cache,
                txs_needed: &mut txs_needed,
                unavailable_txs: &mut unavailable_txs,
                known_fees: &mut known_fees,
                mempool_txids: &mut HashSet::new(),
            };
            let added = try_add_tx_from_caches::<_, DefaultEnforcer>(
                &mut inner, sync_state, txid,
            )
            .expect("first add should succeed");
            assert!(
                matches!(added, ApplySyncActionResult::Success { .. }),
                "tx should be added to the mempool"
            );
        }
        assert!(
            inner.mempool.txs.0.contains_key(&txid),
            "tx should be present in the mempool after the first add"
        );
        assert!(
            inner.unfiltered_mempool.txs.contains(&txid),
            "tx should be present in the unfiltered mempool after the first add"
        );

        // Re-applying the same txid must be an idempotent no-op success, not a
        // fatal `TxAlreadyExists`.
        {
            let sync_state = SyncStateBorrowedMut {
                blocks_needed: &mut blocks_needed,
                rejected_blocks: &mut rejected_blocks,
                rejected_txs: &mut rejected_txs,
                request_queue: &request_queue,
                tx_cache: &mut tx_cache,
                txs_needed: &mut txs_needed,
                unavailable_txs: &mut unavailable_txs,
                known_fees: &mut known_fees,
                mempool_txids: &mut HashSet::new(),
            };
            let added = try_add_tx_from_caches::<_, DefaultEnforcer>(
                &mut inner, sync_state, txid,
            )
            .expect(
                "re-adding an already-present tx must not error \
                 (idempotent reconciliation)",
            );
            assert!(
                matches!(added, ApplySyncActionResult::Success { .. }),
                "re-add should report success"
            );
        }
        assert!(
            inner.unfiltered_mempool.txs.contains(&txid),
            "tx should still be present in the unfiltered mempool after the second add"
        );
    }

    /// Builds a `SyncStateBorrowedMut` and applies `try_add_tx_from_caches`
    /// the way the action-queue loop does: front-pushed inserts are applied
    /// first (in order), then any remaining requeued txs. Panics on `Pending`
    /// (all txs a driven test uses are already in the tx cache).
    fn drive_add(
        inner: &mut MempoolSyncInner<DefaultEnforcer>,
        tx_cache: &mut HashMap<Txid, Transaction>,
        mempool_txids: &mut HashSet<Txid>,
        txid: Txid,
    ) {
        let request_queue = RequestQueue::default();
        let mut queue = std::collections::VecDeque::from([txid]);
        let mut steps = 0;
        while let Some(next) = queue.pop_front() {
            steps += 1;
            assert!(steps <= 64, "requeue loop failed to converge");
            let mut blocks_needed = LinkedHashSet::new();
            let mut rejected_blocks = HashSet::new();
            let mut rejected_txs = HashSet::new();
            let mut txs_needed = LinkedHashSet::new();
            let mut unavailable_txs = HashSet::new();
            let mut known_fees: HashMap<Txid, Amount> =
                tx_cache.keys().map(|txid| (*txid, Amount::ONE_SAT)).collect();
            let sync_state = SyncStateBorrowedMut {
                blocks_needed: &mut blocks_needed,
                rejected_blocks: &mut rejected_blocks,
                rejected_txs: &mut rejected_txs,
                request_queue: &request_queue,
                tx_cache: &mut *tx_cache,
                txs_needed: &mut txs_needed,
                unavailable_txs: &mut unavailable_txs,
                known_fees: &mut known_fees,
                mempool_txids: &mut *mempool_txids,
            };
            match try_add_tx_from_caches::<_, DefaultEnforcer>(
                inner, sync_state, next,
            )
            .expect("add must not error")
            {
                ApplySyncActionResult::Success {
                    push_txs_action_queue_front,
                } => {
                    for tx in push_txs_action_queue_front.into_iter().rev() {
                        queue.push_front(tx);
                    }
                }
                ApplySyncActionResult::Pending => {
                    panic!("unexpected Pending for a fully-cached tx {next}")
                }
            }
        }
    }

    /// Regression for issue #611 (the silent closure-broken-template half). A
    /// child must not be admitted to the enforced mempool ahead of an
    /// *unconfirmed mempool* parent that is present only in the tx cache. The
    /// tx cache exists to read a confirmed parent's spent-output value; letting
    /// a cache hit stand in for an unconfirmed mempool parent's *presence*
    /// inserts the child as a rootless orphan, so the block template it lands
    /// in is closure-broken and mines to `bad-txns-inputs-missingorspent`.
    ///
    /// Also covers the strictly-FIFO deadlock: since the parent's own insert
    /// is queued behind the child (an out-of-order initial snapshot), simply
    /// deferring the child would block the queue head forever. The child's add
    /// must instead requeue the parent's insert in front of its own, and
    /// driving that requeue must admit BOTH, parent first.
    #[test]
    fn child_not_admitted_ahead_of_unconfirmed_mempool_parent() {
        let genesis = BlockHash::all_zeros();
        let (tip_watch, _) = watch::channel(genesis);
        let mut inner = MempoolSyncInner {
            abandoned_pool: AbandonedPool::default(),
            reconsider_attempts: HashMap::new(),
            enforcer: DefaultEnforcer,
            mempool: Mempool::new(genesis),
            tip_watch,
            unfiltered_mempool: UnfilteredMempool {
                tip: genesis,
                txs: HashSet::new(),
            },
        };

        // An UNCONFIRMED MEMPOOL parent, currently only in the tx cache (e.g.
        // fetched as a dependency) and NOT yet inserted into the enforced
        // mempool. Its child spends its output.
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let parent_txid = parent.compute_txid();
        let tx = make_tx(&[OutPoint::new(parent_txid, 0)], &[90_000]);
        let txid = tx.compute_txid();

        let mut tx_cache = HashMap::new();
        tx_cache.insert(parent_txid, parent);
        tx_cache.insert(txid, tx);
        // The parent's own (confirmed) funding tx, fetched for its output
        // value. Not a node-mempool tx, so the cache may stand in for it.
        tx_cache.insert(Txid::all_zeros(), make_tx(&[], &[200_000]));

        // Both are unconfirmed node-mempool txs, from the initial snapshot.
        let mut mempool_txids: HashSet<Txid> =
            [parent_txid, txid].into_iter().collect();

        drive_add(&mut inner, &mut tx_cache, &mut mempool_txids, txid);

        // The closure invariant (never child-without-parent) plus liveness:
        // the requeue must have admitted both, parent first.
        assert!(
            inner.mempool.txs.0.contains_key(&parent_txid),
            "parent should have been admitted via the front-of-queue requeue; \
             admitting the child without it is a closure-broken template"
        );
        assert!(
            inner.mempool.txs.0.contains_key(&txid),
            "child should have been admitted after its parent"
        );
    }

    /// Regression for issue #611, the live recurrence. `mempool_txids` is a
    /// snapshot frozen at initial sync, so a parent the node accepted *after*
    /// startup is not in it; classifying such a parent as "confirmed" let the
    /// tx cache stand in for it and its child was admitted as a rootless
    /// orphan. The live node-mempool mirror (`unfiltered_mempool.txs`) must be
    /// consulted too.
    ///
    /// Modelled here in the state the field failure was observed in: the
    /// parent was marked unavailable (its fetch raced a removal), the node
    /// re-accepted it (so it is in the unfiltered mempool but not the enforced
    /// one), and the child's dependency fetch has placed it in the tx cache.
    #[test]
    fn late_arriving_parent_not_masked_by_tx_cache() {
        let genesis = BlockHash::all_zeros();
        let (tip_watch, _) = watch::channel(genesis);
        let mut inner = MempoolSyncInner {
            abandoned_pool: AbandonedPool::default(),
            reconsider_attempts: HashMap::new(),
            enforcer: DefaultEnforcer,
            mempool: Mempool::new(genesis),
            tip_watch,
            unfiltered_mempool: UnfilteredMempool {
                tip: genesis,
                txs: HashSet::new(),
            },
        };

        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let parent_txid = parent.compute_txid();
        let tx = make_tx(&[OutPoint::new(parent_txid, 0)], &[90_000]);
        let txid = tx.compute_txid();

        // Post-snapshot world: the frozen snapshot knows neither tx. The
        // parent is in the node's mempool (unfiltered mirror) but NOT the
        // enforced mempool, and is marked unavailable from the earlier race.
        let mut mempool_txids: HashSet<Txid> = HashSet::new();
        inner.unfiltered_mempool.txs.insert(parent_txid);

        let mut tx_cache = HashMap::new();
        tx_cache.insert(parent_txid, parent);
        tx_cache.insert(txid, tx);

        let mut blocks_needed = LinkedHashSet::new();
        let mut rejected_blocks = HashSet::new();
        let mut rejected_txs = HashSet::new();
        let request_queue = RequestQueue::default();
        let mut txs_needed = LinkedHashSet::new();
        let mut unavailable_txs = HashSet::from([parent_txid]);

        let sync_state = SyncStateBorrowedMut {
            blocks_needed: &mut blocks_needed,
            rejected_blocks: &mut rejected_blocks,
            rejected_txs: &mut rejected_txs,
            request_queue: &request_queue,
            tx_cache: &mut tx_cache,
            txs_needed: &mut txs_needed,
            unavailable_txs: &mut unavailable_txs,
            known_fees: &mut HashMap::new(),
            mempool_txids: &mut mempool_txids,
        };
        let _res = try_add_tx_from_caches::<_, DefaultEnforcer>(
            &mut inner, sync_state, txid,
        )
        .expect("add must not error");

        // The child must not be admitted while its (unconfirmed, re-accepted)
        // parent is absent from the enforced mempool; it parks in the
        // abandoned pool until the parent is admitted.
        assert!(
            !inner.mempool.txs.0.contains_key(&txid),
            "child was admitted to the enforced mempool although its \
             unconfirmed node-mempool parent is absent from it — a \
             closure-broken template (live #611 recurrence)"
        );
        assert!(
            inner.abandoned_pool.contains(&txid),
            "child should be parked in the abandoned pool until its parent \
             is admitted"
        );
    }

    /// A parent can linger in the unfiltered mempool with no terminal state
    /// and no enforced-mempool entry — the state a tx restored from the
    /// abandoned pool was left in when the restore forgot to scrub the
    /// unfiltered set (its queued re-insert was then swallowed by the
    /// already-present short-circuit). A child resolving such a "ghost"
    /// parent must scrub it and requeue its insert so both are admitted —
    /// not requeue the same swallowed insert forever. (issue #611)
    #[test]
    fn ghost_unfiltered_parent_is_scrubbed_and_admitted() {
        let genesis = BlockHash::all_zeros();
        let (tip_watch, _) = watch::channel(genesis);
        let mut inner = MempoolSyncInner {
            abandoned_pool: AbandonedPool::default(),
            reconsider_attempts: HashMap::new(),
            enforcer: DefaultEnforcer,
            mempool: Mempool::new(genesis),
            tip_watch,
            unfiltered_mempool: UnfilteredMempool {
                tip: genesis,
                txs: HashSet::new(),
            },
        };

        // Confirmed funding tx -> ghost parent -> child.
        let funding = make_tx(&[], &[150_000]);
        let funding_txid = funding.compute_txid();
        let parent = make_tx(&[OutPoint::new(funding_txid, 0)], &[100_000]);
        let parent_txid = parent.compute_txid();
        let tx = make_tx(&[OutPoint::new(parent_txid, 0)], &[90_000]);
        let txid = tx.compute_txid();

        let mut tx_cache = HashMap::new();
        tx_cache.insert(funding_txid, funding);
        tx_cache.insert(parent_txid, parent);
        tx_cache.insert(txid, tx);

        // The ghost state: in the unfiltered mempool, in no terminal state,
        // absent from the enforced mempool.
        inner.unfiltered_mempool.txs.insert(parent_txid);

        // Post-snapshot world.
        let mut mempool_txids: HashSet<Txid> = HashSet::new();

        drive_add(&mut inner, &mut tx_cache, &mut mempool_txids, txid);

        assert!(
            inner.mempool.txs.0.contains_key(&parent_txid),
            "ghost parent should be scrubbed from the unfiltered set and \
             admitted (a swallowed insert here spins the requeue forever)"
        );
        assert!(
            inner.mempool.txs.0.contains_key(&txid),
            "child should be admitted after its parent"
        );
    }

    /// A fresh, freshly-empty `SyncState` past initial sync, so every
    /// sequence message is processed rather than dropped by the
    /// `first_mempool_sequence` gate.
    fn fresh_sync_state() -> SyncState {
        SyncState {
            action_queue: SyncActionQueue::default(),
            blocks_needed: LinkedHashSet::new(),
            first_mempool_sequence: None,
            rejected_blocks: HashSet::new(),
            rejected_txs: HashSet::new(),
            request_queue: RequestQueue::default(),
            tx_cache: HashMap::new(),
            txs_needed: LinkedHashSet::new(),
            unavailable_txs: HashSet::new(),
            known_fees: HashMap::new(),
            mempool_txids: HashSet::new(),
        }
    }

    fn fresh_inner() -> MempoolSyncInner<DefaultEnforcer> {
        let genesis = BlockHash::all_zeros();
        let (tip_watch, _) = watch::channel(genesis);
        MempoolSyncInner {
            abandoned_pool: AbandonedPool::default(),
            reconsider_attempts: HashMap::new(),
            enforcer: DefaultEnforcer,
            mempool: Mempool::new(genesis),
            tip_watch,
            unfiltered_mempool: UnfilteredMempool {
                tip: genesis,
                txs: HashSet::new(),
            },
        }
    }

    fn is_fetch_queued(sync_state: &SyncState, txid: Txid) -> bool {
        sync_state
            .request_queue
            .inner
            .queue
            .lock()
            .contains(&RequestItem::Tx(txid, true))
    }

    /// A fresh `Added` sequence message must clear a stale `unavailable`
    /// verdict: the verdict recorded that a fetch lost a race with a removal,
    /// and the node re-announcing the tx means it is back. Leaving the verdict
    /// in place skips the tx forever (`try_add_tx_from_caches` short-circuits
    /// on it) while its descendants keep arriving. (issue #611)
    ///
    /// The clear belongs at *arrival* (`handle_tx_hash_msg`), which runs
    /// exactly once per message — NOT in the apply path, which re-runs for the
    /// same queued action on every drive while its fetch is pending. An earlier
    /// version of this test asserted the apply-path clear directly and drove a
    /// single round, so it could never observe the livelock that clear caused
    /// (see `readded_tx_that_loses_fetch_race_terminates_not_livelocks`).
    #[test]
    fn added_seq_message_clears_stale_unavailable_verdict() {
        let mut inner = fresh_inner();
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let parent_txid = parent.compute_txid();

        let mut sync_state = fresh_sync_state();
        // Stale verdict from a fetch that raced the tx's earlier removal.
        sync_state.unavailable_txs.insert(parent_txid);

        handle_tx_hash_msg::<_, DefaultEnforcer>(
            &mut inner,
            &mut sync_state,
            TxHashMessage {
                txid: parent_txid,
                event: TxHashEvent::Added,
                mempool_seq: 1,
                zmq_seq: 1,
            },
        )
        .expect("an Added arrival must not error");

        assert!(
            !sync_state.unavailable_txs.contains(&parent_txid),
            "a fresh `Added` arrival must clear the stale unavailable verdict"
        );
        assert!(
            sync_state.txs_needed.contains(&parent_txid),
            "the re-announced tx is needed again"
        );
        assert!(
            is_fetch_queued(&sync_state, parent_txid),
            "arrival must queue a fresh fetch so the tx can be admitted"
        );
    }

    /// Regression for the livelock found reviewing the #611 fix
    /// (LayerTwo-Labs/cusf-enforcer-mempool#114): a tx whose fetch loses the
    /// race with a removal — an ordinary RBF landing while `getrawtransaction`
    /// is in flight — must reach a terminal state, not be re-fetched forever.
    ///
    /// The queued `Added` action parks `Pending` while its fetch is out. When
    /// the fetch comes back `unavailable`, `handle_resp` marks the tx and
    /// re-drives the queue, re-applying that same action. Clearing the mark on
    /// re-application (as the apply path once did) removed the only terminal
    /// state a gone tx has: the tx was un-marked, re-fetched, failed the same
    /// way, and round it went — silently, with `task_errors` empty and the
    /// node under a sustained `getrawtransaction` flood. This drives that
    /// second round and requires it to terminate.
    #[test]
    fn readded_tx_that_loses_fetch_race_terminates_not_livelocks() {
        let mut inner = fresh_inner();
        let tx = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let txid = tx.compute_txid();
        let mut sync_state = fresh_sync_state();

        // 1. The node announces the tx: arrival queues its fetch + the action.
        handle_seq_message::<_, DefaultEnforcer>(
            &mut inner,
            &mut sync_state,
            SequenceMessage::TxHash(TxHashMessage {
                txid,
                event: TxHashEvent::Added,
                mempool_seq: 1,
                zmq_seq: 1,
            }),
        )
        .expect("an Added arrival must not error");
        assert!(!sync_state.action_queue.is_empty());

        // 2. First drive: not cached yet, so the action parks Pending at the
        //    head of the queue with its fetch outstanding.
        let applied =
            futures::executor::block_on(try_apply_next_sync_action::<
                _,
                DefaultEnforcer,
            >(
                &mut inner, &mut sync_state
            ))
            .expect("the first drive must not error");
        assert!(
            !applied,
            "not yet fetchable: the Added action must park Pending"
        );
        assert!(
            !sync_state.action_queue.is_empty(),
            "a Pending action stays at the head of the queue"
        );

        // 3. The fetch loses the race with a removal (RBF'd while in flight).
        //    This is exactly what `handle_resp` records for a
        //    `BatchTx { unavailable }` result. The dispatched request has left
        //    the queue, so a re-queued fetch below would be detectable.
        sync_state.txs_needed.remove(&txid);
        sync_state.unavailable_txs.insert(txid);
        sync_state
            .request_queue
            .remove(&RequestItem::Tx(txid, true));

        // 4. Re-drive, as `handle_resp` does after recording the result. With
        //    the apply-path clear this un-marked the tx, re-queued its fetch,
        //    and parked Pending again — forever. It must terminate instead.
        let applied =
            futures::executor::block_on(try_apply_next_sync_action::<
                _,
                DefaultEnforcer,
            >(
                &mut inner, &mut sync_state
            ))
            .expect("the re-drive must not error");
        assert!(
            applied,
            "the re-applied Added must terminate (Success), not park Pending \
             again — that is the livelock"
        );
        assert!(
            sync_state.action_queue.is_empty(),
            "the terminated action must be popped from the queue"
        );
        assert!(
            sync_state.unavailable_txs.contains(&txid),
            "the unavailable verdict is the terminal state; re-application \
             must not clear it"
        );
        assert!(
            inner.unfiltered_mempool.txs.contains(&txid),
            "a gone tx is recorded as handled: not admitted, not re-fetched"
        );
        assert!(
            !is_fetch_queued(&sync_state, txid),
            "no fresh fetch may be re-queued for a tx the node dropped"
        );
    }

    /// A parent the node accepted AFTER initial sync is absent from the
    /// `mempool_txids` snapshot. Deciding the parent fetch from the snapshot
    /// alone classifies it as confirmed and skips it, only for the child's
    /// resolution to discover it is a mempool parent and request it a round
    /// trip later. The live unfiltered mirror must be consulted too.
    #[test]
    fn needs_parent_fetch_sees_post_startup_mempool_parent() {
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let parent_txid = parent.compute_txid();
        let mut sync_state = fresh_sync_state();

        // Confirmed parent: nothing to fetch it for.
        assert!(
            !needs_parent_fetch(
                &sync_state,
                &HashSet::new(),
                &parent_txid
            ),
            "a confirmed parent is not fetched"
        );

        // The same parent, but the node accepted it after startup: it is a
        // mempool parent (dependency state matters) and must be fetched now.
        let live = HashSet::from([parent_txid]);
        assert!(
            needs_parent_fetch(&sync_state, &live, &parent_txid),
            "a post-startup mempool parent must be fetched, not read as \
             confirmed from the frozen snapshot"
        );

        // Snapshot members still count, and a cached parent is never fetched.
        sync_state.mempool_txids.insert(parent_txid);
        assert!(needs_parent_fetch(
            &sync_state,
            &HashSet::new(),
            &parent_txid
        ));
        sync_state.tx_cache.insert(parent_txid, parent);
        assert!(!needs_parent_fetch(
            &sync_state,
            &live,
            &parent_txid
        ));
    }

    /// Build an RPC `Block<true>` (verbose `getblock`) from real txs, for the
    /// block-connect / block-response tests. Only the fields the sync paths
    /// read (`hash`, `previousblockhash`, `height`, `tx`) are meaningful; the
    /// rest are placeholders.
    fn make_resp_block(
        hash: BlockHash,
        prev: BlockHash,
        height: u32,
        txs: &[&Transaction],
    ) -> bitcoin_jsonrpsee::client::Block<true> {
        bitcoin_jsonrpsee::client::Block {
            hash,
            confirmations: 1,
            strippedsize: 0,
            size: 0,
            weight: 0,
            height,
            version: bitcoin::block::Version::TWO,
            version_hex: String::new(),
            merkleroot: bitcoin::TxMerkleNode::all_zeros(),
            tx: txs
                .iter()
                .map(|tx| bitcoin_jsonrpsee::client::TxInfo {
                    hex: bitcoin::consensus::serialize(*tx),
                    txid: tx.compute_txid(),
                })
                .collect(),
            time: 0,
            mediantime: 0,
            nonce: 0,
            compact_target: bitcoin::CompactTarget::from_consensus(0x1d00_ffff),
            difficulty: 1.0,
            chainwork: String::new(),
            previousblockhash: Some(prev),
            nextblockhash: None,
        }
    }

    /// The restore scrub in `handle_tx_hash_msg` (mutation survivor from the
    /// review of #114): when a re-announced parent restores its abandoned
    /// descendant, a descendant left recorded in the unfiltered mirror has its
    /// re-insert silently swallowed by the already-present short-circuit in
    /// `try_add_tx_from_caches`. It must be scrubbed from the mirror. (#611)
    #[test]
    fn readded_parent_scrubs_restored_descendant_from_unfiltered_mirror() {
        let mut inner = fresh_inner();
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], &[90_000]);
        let child_txid = child.compute_txid();
        let mut sync_state = fresh_sync_state();

        // The child is parked awaiting its (absent) parent, and — the bug's
        // precondition — is still recorded in the unfiltered mirror.
        inner
            .abandoned_pool
            .insert(child, HashSet::from([parent_txid]));
        inner.unfiltered_mempool.txs.insert(child_txid);
        // The parent carried a stale `unavailable` verdict that this Added
        // arrival clears, triggering the restore.
        sync_state.unavailable_txs.insert(parent_txid);

        handle_tx_hash_msg::<_, DefaultEnforcer>(
            &mut inner,
            &mut sync_state,
            TxHashMessage {
                txid: parent_txid,
                event: TxHashEvent::Added,
                mempool_seq: 1,
                zmq_seq: 1,
            },
        )
        .expect("an Added arrival must not error");

        assert!(
            !inner.unfiltered_mempool.txs.contains(&child_txid),
            "the restored descendant must be scrubbed from the unfiltered \
             mirror so its re-insert is not swallowed"
        );
        assert!(
            sync_state.tx_cache.contains_key(&child_txid),
            "the restored descendant is cached for re-insertion"
        );
    }

    /// The restore scrub in `handle_resp_block` (the second, live restore site;
    /// the review's mutation testing found it uncovered): a confirmed parent
    /// arriving as a block response restores an abandoned descendant the block
    /// did NOT confirm; that descendant must be scrubbed from the unfiltered
    /// mirror, same swallow bug as the arrival path. (#611)
    #[test]
    fn block_response_scrubs_restored_descendant_from_unfiltered_mirror() {
        let mut inner = fresh_inner();
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], &[90_000]);
        let child_txid = child.compute_txid();
        let mut sync_state = fresh_sync_state();

        inner
            .abandoned_pool
            .insert(child, HashSet::from([parent_txid]));
        inner.unfiltered_mempool.txs.insert(child_txid);

        // A block confirming ONLY the parent; the child is still unconfirmed.
        let block = make_resp_block(
            BlockHash::from_byte_array([1; 32]),
            BlockHash::all_zeros(),
            1,
            &[&parent],
        );
        futures::executor::block_on(handle_resp_block::<_, DefaultEnforcer>(
            &mut inner,
            &mut sync_state,
            block,
        ))
        .expect("handling a block response must not error");

        assert!(
            !inner.unfiltered_mempool.txs.contains(&child_txid),
            "the restored descendant must be scrubbed from the unfiltered \
             mirror"
        );
        assert!(
            sync_state.tx_cache.contains_key(&child_txid),
            "the restored descendant is cached for re-insertion"
        );
    }

    /// The block-confirmed-restore skip in `handle_resp_block` (mutation
    /// survivor `d3f743f`, previously no test): a descendant restored by a
    /// block response that is ITSELF confirmed in that same block (parent and
    /// child mined together) must NOT be re-inserted — that would put an
    /// already-mined tx into every subsequent template. It is only cached.
    /// (#611)
    #[test]
    fn block_response_does_not_reinsert_a_descendant_mined_in_the_block() {
        let mut inner = fresh_inner();
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], &[90_000]);
        let child_txid = child.compute_txid();
        let mut sync_state = fresh_sync_state();

        inner
            .abandoned_pool
            .insert(child.clone(), HashSet::from([parent_txid]));

        // A block confirming BOTH parent and child.
        let block = make_resp_block(
            BlockHash::from_byte_array([2; 32]),
            BlockHash::all_zeros(),
            1,
            &[&parent, &child],
        );
        futures::executor::block_on(handle_resp_block::<_, DefaultEnforcer>(
            &mut inner,
            &mut sync_state,
            block,
        ))
        .expect("handling a block response must not error");

        assert!(
            sync_state.tx_cache.contains_key(&child_txid),
            "a confirmed restored descendant is still cached (its output value \
             may be needed)"
        );
        assert!(
            sync_state.action_queue.is_empty(),
            "a descendant mined in this same block must NOT be queued for \
             re-insertion into the mempool"
        );
    }

    /// The snapshot prune on the Removed apply path (mutation survivor
    /// `22179de`, previously no direct test): a tx removed from the node
    /// mempool must be pruned from the `mempool_txids` snapshot set, or the
    /// since-removed member keeps classifying as an unconfirmed mempool tx and
    /// a later child re-admits it. (#611)
    #[test]
    fn removed_tx_is_pruned_from_the_snapshot_set() {
        let mut inner = fresh_inner();
        let tx = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let txid = tx.compute_txid();
        let mut sync_state = fresh_sync_state();

        // The tx is a known node-mempool member, mirrored in the unfiltered
        // set.
        inner.unfiltered_mempool.txs.insert(txid);
        sync_state.mempool_txids.insert(txid);

        handle_seq_message::<_, DefaultEnforcer>(
            &mut inner,
            &mut sync_state,
            SequenceMessage::TxHash(TxHashMessage {
                txid,
                event: TxHashEvent::Removed,
                mempool_seq: 1,
                zmq_seq: 1,
            }),
        )
        .expect("a Removed arrival must not error");
        let applied =
            futures::executor::block_on(try_apply_next_sync_action::<
                _,
                DefaultEnforcer,
            >(
                &mut inner, &mut sync_state
            ))
            .expect("applying the Removed action must not error");

        assert!(applied, "the Removed action must apply");
        assert!(
            !sync_state.mempool_txids.contains(&txid),
            "a removed tx must be pruned from the snapshot set"
        );
        assert!(!inner.unfiltered_mempool.txs.contains(&txid));
    }

    /// Two mutation survivors at block connect: (a) the conflict sweep
    /// (`6d5dc0c`, its test exercised `spenders_of` in isolation) — a mempool
    /// tx that spends the same outpoint as a block-confirmed tx is evicted even
    /// though the node emits no removal message for it; (b) the snapshot prune
    /// (`22179de`, connect-path site) — the confirmed tx is pruned from
    /// `mempool_txids`. (#611)
    #[test]
    fn connect_block_evicts_conflicts_and_prunes_snapshot() {
        let mut inner = fresh_inner();
        let spent = OutPoint::new(Txid::all_zeros(), 0);
        // The RBF winner (confirmed in the block) and loser (in the mempool)
        // spend the same outpoint. Distinct txids via a different output shape.
        let winner = make_tx(&[spent], &[100_000]);
        let winner_txid = winner.compute_txid();
        let loser = make_tx(&[spent], &[90_000, 5_000]);
        let loser_txid = loser.compute_txid();
        assert_ne!(winner_txid, loser_txid);
        let mut sync_state = fresh_sync_state();

        // The loser sits in the enforced mempool and the unfiltered mirror.
        let loser_weight = loser.weight();
        inner
            .mempool
            .insert(
                loser,
                Amount::from_sat(1000),
                imbl::OrdSet::new(),
                loser_weight,
            )
            .expect("inserting the loser must succeed");
        inner.unfiltered_mempool.txs.insert(loser_txid);
        // The winner is a live snapshot member about to be confirmed.
        sync_state.mempool_txids.insert(winner_txid);
        assert!(
            inner.mempool.spenders_of(&spent).contains(&loser_txid),
            "precondition: the loser spends the contested outpoint"
        );

        // Both mempool tips start at genesis (fresh_inner); a block whose
        // parent is genesis connects cleanly.
        let block = make_resp_block(
            BlockHash::from_byte_array([3; 32]),
            BlockHash::all_zeros(),
            1,
            &[&winner],
        );
        {
            let ss = SyncStateBorrowedMut {
                blocks_needed: &mut sync_state.blocks_needed,
                rejected_blocks: &mut sync_state.rejected_blocks,
                rejected_txs: &mut sync_state.rejected_txs,
                request_queue: &sync_state.request_queue,
                tx_cache: &mut sync_state.tx_cache,
                txs_needed: &mut sync_state.txs_needed,
                unavailable_txs: &mut sync_state.unavailable_txs,
                known_fees: &mut sync_state.known_fees,
                mempool_txids: &mut sync_state.mempool_txids,
            };
            futures::executor::block_on(connect_block::<_, DefaultEnforcer>(
                &mut inner, ss, &block,
            ))
            .expect("connecting the block must not error");
        }

        assert!(
            !inner.mempool.spenders_of(&spent).contains(&loser_txid),
            "a mempool tx conflicting with a block-confirmed tx must be evicted"
        );
        assert!(
            !inner.unfiltered_mempool.txs.contains(&loser_txid),
            "the evicted conflict is scrubbed from the unfiltered mirror"
        );
        assert!(
            !sync_state.mempool_txids.contains(&winner_txid),
            "the block-confirmed tx must be pruned from the snapshot set"
        );
    }
    // ------------------------------------------------------------------
    // Incident 2026-09-19 (ecpool beta): every BMM bid rejected after
    // 969,715. Reproduction of the two halves.
    // ------------------------------------------------------------------

    /// Rejects the listed txids until a block has been connected, then
    /// accepts everything. Models an enforcer whose verdict depends on the
    /// chain tip (an M8 naming a block the enforcer has not connected yet is
    /// `BmmRequestExpired`; the same M8 is valid once that block connects).
    struct RejectUntilBlock {
        reject: HashSet<Txid>,
        blocks_connected: usize,
        /// Rejected whatever the tip (a permanently invalid tx).
        reject_forever: HashSet<Txid>,
        /// Returned as `remove_mempool_txs` by every `connect_block` (the
        /// shape of the production enforcer: the losing bids for the parent).
        remove_on_connect: HashSet<Txid>,
    }

    impl crate::cusf_enforcer::CusfEnforcer for RejectUntilBlock {
        type InvalidBlockReason = std::convert::Infallible;
        type SyncError = std::convert::Infallible;

        async fn sync_to_tip<
            Signal: std::future::Future<Output = ()> + Send,
        >(
            &mut self,
            _shutdown_signal: Signal,
            _tip: BlockHash,
        ) -> Result<
            (),
            crate::cusf_enforcer::SyncToTipError<
                Self::InvalidBlockReason,
                Self::SyncError,
            >,
        > {
            Ok(())
        }

        type ConnectBlockError = std::convert::Infallible;

        async fn connect_block(
            &mut self,
            _block: &bitcoin::Block,
        ) -> Result<
            crate::cusf_enforcer::ConnectBlockAction,
            Self::ConnectBlockError,
        > {
            self.blocks_connected += 1;
            Ok(crate::cusf_enforcer::ConnectBlockAction::Accept {
                remove_mempool_txs: self.remove_on_connect.clone(),
            })
        }

        type DisconnectBlockError = std::convert::Infallible;

        async fn disconnect_block(
            &mut self,
            _block_hash: BlockHash,
        ) -> Result<
            crate::cusf_enforcer::DisconnectBlockAction,
            Self::DisconnectBlockError,
        > {
            Ok(crate::cusf_enforcer::DisconnectBlockAction::default())
        }

        type AcceptTxError = std::convert::Infallible;

        fn accept_tx(
            &mut self,
            tx: &Transaction,
        ) -> Result<crate::cusf_enforcer::TxAcceptAction, Self::AcceptTxError>
        {
            let txid = tx.compute_txid();
            if self.reject_forever.contains(&txid)
                || (self.blocks_connected == 0 && self.reject.contains(&txid))
            {
                Ok(crate::cusf_enforcer::TxAcceptAction::Reject)
            } else {
                Ok(crate::cusf_enforcer::TxAcceptAction::Accept {
                    conflicts_with: HashSet::new(),
                    weight_tweak: 0,
                })
            }
        }

        type ValidateBlockError = std::convert::Infallible;

        fn validate_block(
            &self,
            _block: &bitcoin::Block,
        ) -> Result<Option<String>, Self::ValidateBlockError> {
            Ok(None)
        }
    }

    fn inner_with(
        enforcer: RejectUntilBlock,
    ) -> MempoolSyncInner<RejectUntilBlock> {
        let genesis = BlockHash::all_zeros();
        let (tip_watch, _) = watch::channel(genesis);
        MempoolSyncInner {
            abandoned_pool: AbandonedPool::default(),
            reconsider_attempts: HashMap::new(),
            enforcer,
            mempool: Mempool::new(genesis),
            tip_watch,
            unfiltered_mempool: UnfilteredMempool {
                tip: genesis,
                txs: HashSet::new(),
            },
        }
    }

    fn drive_insert(
        inner: &mut MempoolSyncInner<RejectUntilBlock>,
        sync_state: &mut SyncState,
        txid: Txid,
    ) -> ApplySyncActionResult {
        let ss = SyncStateBorrowedMut {
            blocks_needed: &mut sync_state.blocks_needed,
            rejected_blocks: &mut sync_state.rejected_blocks,
            rejected_txs: &mut sync_state.rejected_txs,
            request_queue: &sync_state.request_queue,
            tx_cache: &mut sync_state.tx_cache,
            txs_needed: &mut sync_state.txs_needed,
            unavailable_txs: &mut sync_state.unavailable_txs,
            known_fees: &mut sync_state.known_fees,
            mempool_txids: &mut sync_state.mempool_txids,
        };
        try_add_tx_from_caches::<_, RejectUntilBlock>(inner, ss, txid)
            .expect("insert must not error")
    }

    fn drive_connect(
        inner: &mut MempoolSyncInner<RejectUntilBlock>,
        sync_state: &mut SyncState,
        block: &bitcoin_jsonrpsee::client::Block<true>,
    ) {
        let ss = SyncStateBorrowedMut {
            blocks_needed: &mut sync_state.blocks_needed,
            rejected_blocks: &mut sync_state.rejected_blocks,
            rejected_txs: &mut sync_state.rejected_txs,
            request_queue: &sync_state.request_queue,
            tx_cache: &mut sync_state.tx_cache,
            txs_needed: &mut sync_state.txs_needed,
            unavailable_txs: &mut sync_state.unavailable_txs,
            known_fees: &mut sync_state.known_fees,
            mempool_txids: &mut sync_state.mempool_txids,
        };
        let reconsider =
            futures::executor::block_on(connect_block::<_, RejectUntilBlock>(
                inner, ss, block,
            ))
            .expect("connect must not error");
        // What the sync driver does with the returned list: an `InsertTx`
        // for each, parents first.
        for txid in reconsider {
            let res = drive_insert(inner, sync_state, txid);
            assert!(
                matches!(res, ApplySyncActionResult::Success { .. }),
                "a reconsidered tx is fully cached, so its insert terminates"
            );
        }
    }

    fn is_reject_queued(sync_state: &SyncState, txid: Txid) -> bool {
        sync_state
            .request_queue
            .inner
            .queue
            .lock()
            .contains(&RequestItem::RejectTx(txid))
    }

    /// Half 1 — the cascade. A tx the enforcer rejected on arrival is later
    /// MINED (the enforcer accepted the block that contains it, so by the
    /// enforcer's own rules it is valid there). Its outputs are now real
    /// coins. A child spending one of them, which the enforcer would accept,
    /// must be admitted. Today `rejected_txs` is never cleared and
    /// `try_get_parent_txs_from_caches` refuses any input whose txid is in
    /// it, so the child is rejected as "rejected parent" before `accept_tx`
    /// is ever asked — and deprioritised in the node by -21M BTC. Observed
    /// live: every BMM bid on ecpool beta after 969,715, because the bidder
    /// funds each bid from the previous mined bid's change output.
    #[test]
    fn child_of_a_mined_parent_is_admitted_even_if_the_parent_was_once_rejected()
     {
        let funding = OutPoint::new(Txid::all_zeros(), 0);
        let parent = make_tx(&[funding], &[100_000, 50_000]);
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 1)], &[40_000]);
        let child_txid = child.compute_txid();

        let mut inner = inner_with(RejectUntilBlock {
            reject: HashSet::from([parent_txid]),
            blocks_connected: 0,
            reject_forever: HashSet::new(),
            remove_on_connect: HashSet::new(),
        });
        let mut sync_state = fresh_sync_state();

        // 1. The parent arrives; the enforcer rejects it.
        sync_state.tx_cache.insert(parent_txid, parent.clone());
        sync_state
            .known_fees
            .insert(parent_txid, Amount::from_sat(1_000));
        let res = drive_insert(&mut inner, &mut sync_state, parent_txid);
        assert!(matches!(res, ApplySyncActionResult::Success { .. }));
        assert!(
            sync_state.rejected_txs.contains(&parent_txid),
            "precondition: the parent was rejected by the enforcer"
        );
        assert!(
            is_reject_queued(&sync_state, parent_txid),
            "precondition: the parent's deprioritisation was queued"
        );

        // 2. A block mines the parent; the enforcer accepts the block.
        let block = make_resp_block(
            BlockHash::from_byte_array([3; 32]),
            BlockHash::all_zeros(),
            1,
            &[&parent],
        );
        drive_connect(&mut inner, &mut sync_state, &block);
        assert_eq!(
            inner.mempool.chain.tip, block.hash,
            "precondition: block connected"
        );

        // 3. The child arrives with its fee known from the node. The enforcer
        //    accepts it (nothing about it is rejectable). It must be admitted.
        sync_state.tx_cache.insert(child_txid, child);
        sync_state
            .known_fees
            .insert(child_txid, Amount::from_sat(1_000));
        let res = drive_insert(&mut inner, &mut sync_state, child_txid);
        assert!(matches!(res, ApplySyncActionResult::Success { .. }));
        assert!(
            !sync_state.rejected_txs.contains(&child_txid),
            "a child of a MINED parent must not be rejected as 'rejected parent'"
        );
        assert!(
            !is_reject_queued(&sync_state, child_txid),
            "the child must not be deprioritised in the node"
        );
        assert!(
            inner.mempool.txs.0.contains_key(&child_txid),
            "the child must be in the enforced mempool (and so in block templates)"
        );
    }

    /// Half 2 — the seed. A tx the enforcer rejects only because of the
    /// current tip (an M8 that names a block the enforcer has not connected
    /// yet — the bidder's tx reached us before the block did) is rejected
    /// once and never asked about again. When the block it names connects,
    /// the enforcer would now accept it, but nothing re-evaluates it: it
    /// stays out of the enforced mempool, stays in `rejected_txs` (seeding
    /// half 1 once it is mined), and stays deprioritised in the node.
    #[test]
    fn tx_rejected_for_the_old_tip_is_reconsidered_when_the_tip_moves() {
        let funding = OutPoint::new(Txid::all_zeros(), 0);
        let tx = make_tx(&[funding], &[100_000]);
        let txid = tx.compute_txid();

        let mut inner = inner_with(RejectUntilBlock {
            reject: HashSet::from([txid]),
            blocks_connected: 0,
            reject_forever: HashSet::new(),
            remove_on_connect: HashSet::new(),
        });
        let mut sync_state = fresh_sync_state();
        sync_state.tx_cache.insert(txid, tx.clone());
        sync_state.known_fees.insert(txid, Amount::from_sat(1_000));

        // 1. Arrives before its block: rejected against the old tip.
        let res = drive_insert(&mut inner, &mut sync_state, txid);
        assert!(matches!(res, ApplySyncActionResult::Success { .. }));
        assert!(sync_state.rejected_txs.contains(&txid), "precondition");
        assert!(!inner.mempool.txs.0.contains_key(&txid), "precondition");

        // 2. The block it was waiting for connects (it does not contain the
        //    tx). From here on the enforcer accepts it.
        let block = make_resp_block(
            BlockHash::from_byte_array([4; 32]),
            BlockHash::all_zeros(),
            1,
            &[],
        );
        drive_connect(&mut inner, &mut sync_state, &block);
        assert!(
            matches!(
                inner.enforcer.accept_tx(&tx),
                Ok(crate::cusf_enforcer::TxAcceptAction::Accept { .. })
            ),
            "precondition: the enforcer now accepts the tx"
        );

        // 3. Still in the node's mempool, now valid: it must reach the
        //    enforced mempool. Today nothing re-evaluates a rejected tx.
        assert!(
            inner.mempool.txs.0.contains_key(&txid),
            "a tx rejected only for the old tip must be re-evaluated after the tip moves"
        );
        assert!(
            !sync_state.rejected_txs.contains(&txid),
            "and must leave rejected_txs, or it seeds the cascade when mined"
        );
        assert!(
            !is_reject_queued(&sync_state, txid),
            "the node-side deprioritisation must be reversed: the pending \
             RejectTx is cancelled by the UnrejectTx (or an UnrejectTx is \
             queued if the reject already went out)"
        );
    }

    fn is_unreject_queued(sync_state: &SyncState, txid: Txid) -> bool {
        sync_state
            .request_queue
            .inner
            .queue
            .lock()
            .contains(&RequestItem::UnrejectTx(txid))
    }

    /// Review finding 1: what THIS connect evicts (`remove_mempool_txs`, the
    /// losing bids for the parent block) is stale for this tip by the
    /// enforcer's own verdict and must NOT be reconsidered in the same call —
    /// a permissive enforcer would otherwise re-admit them into templates, and
    /// the node's deprioritisation would be reversed.
    #[test]
    fn txs_evicted_by_this_connect_are_not_reconsidered_by_it() {
        let stale = make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[1_000]);
        let stale_txid = stale.compute_txid();
        let mut inner = inner_with(RejectUntilBlock {
            reject: HashSet::new(),
            blocks_connected: 0,
            reject_forever: HashSet::new(),
            remove_on_connect: HashSet::from([stale_txid]),
        });
        let mut sync_state = fresh_sync_state();
        sync_state.tx_cache.insert(stale_txid, stale);
        sync_state
            .known_fees
            .insert(stale_txid, Amount::from_sat(100));
        // Accepted at arrival (the enforcer accepts everything here).
        let res = drive_insert(&mut inner, &mut sync_state, stale_txid);
        assert!(matches!(res, ApplySyncActionResult::Success { .. }));
        assert!(
            inner.mempool.txs.0.contains_key(&stale_txid),
            "precondition"
        );

        let block = make_resp_block(
            BlockHash::from_byte_array([5; 32]),
            BlockHash::all_zeros(),
            1,
            &[],
        );
        let reconsider = {
            let ss = SyncStateBorrowedMut {
                blocks_needed: &mut sync_state.blocks_needed,
                rejected_blocks: &mut sync_state.rejected_blocks,
                rejected_txs: &mut sync_state.rejected_txs,
                request_queue: &sync_state.request_queue,
                tx_cache: &mut sync_state.tx_cache,
                txs_needed: &mut sync_state.txs_needed,
                unavailable_txs: &mut sync_state.unavailable_txs,
                known_fees: &mut sync_state.known_fees,
                mempool_txids: &mut sync_state.mempool_txids,
            };
            futures::executor::block_on(connect_block::<_, RejectUntilBlock>(
                &mut inner, ss, &block,
            ))
            .expect("connect must not error")
        };
        assert!(
            reconsider.is_empty(),
            "the evicted tx must not be handed back for re-insert"
        );
        assert!(
            sync_state.rejected_txs.contains(&stale_txid),
            "it stays rejected"
        );
        assert!(
            !inner.mempool.txs.0.contains_key(&stale_txid),
            "and out of the enforced mempool"
        );
        assert!(
            is_reject_queued(&sync_state, stale_txid),
            "its RejectTx is still pending"
        );
        assert!(
            !is_unreject_queued(&sync_state, stale_txid),
            "and no UnrejectTx cancels it"
        );
    }

    /// A tx the enforcer still rejects is re-rejected, the node's delta is left
    /// alone (the pending pair cancels), and after two attempts it is no longer
    /// reconsidered at all.
    #[test]
    fn a_still_invalid_tx_is_rerejected_then_left_alone() {
        let junk = make_tx(&[OutPoint::new(Txid::all_zeros(), 1)], &[1_000]);
        let junk_txid = junk.compute_txid();
        let mut inner = inner_with(RejectUntilBlock {
            reject: HashSet::new(),
            blocks_connected: 0,
            reject_forever: HashSet::from([junk_txid]),
            remove_on_connect: HashSet::new(),
        });
        let mut sync_state = fresh_sync_state();
        sync_state.tx_cache.insert(junk_txid, junk);
        sync_state
            .known_fees
            .insert(junk_txid, Amount::from_sat(100));
        drive_insert(&mut inner, &mut sync_state, junk_txid);
        assert!(sync_state.rejected_txs.contains(&junk_txid), "precondition");
        assert!(is_reject_queued(&sync_state, junk_txid), "precondition");
        for i in 1..=3u8 {
            let block = make_resp_block(
                BlockHash::from_byte_array([10 + i; 32]),
                if i == 1 {
                    BlockHash::all_zeros()
                } else {
                    BlockHash::from_byte_array([9 + i; 32])
                },
                u32::from(i),
                &[],
            );
            // drive_connect re-drives the reconsidered list, so a re-rejection
            // lands back in rejected_txs within the same call.
            drive_connect(&mut inner, &mut sync_state, &block);
            assert!(
                sync_state.rejected_txs.contains(&junk_txid),
                "block {i}: still rejected"
            );
            assert!(
                !inner.mempool.txs.0.contains_key(&junk_txid),
                "block {i}: never admitted"
            );
            assert!(
                is_reject_queued(&sync_state, junk_txid),
                "block {i}: the node-side reject is still the pending request (unreject cancelled by the re-reject)"
            );
            assert!(
                !is_unreject_queued(&sync_state, junk_txid),
                "block {i}: no dangling unreject"
            );
        }
        assert_eq!(
            inner.reconsider_attempts.get(&junk_txid),
            Some(&2),
            "reconsidered exactly twice, then budget exhausted"
        );
    }

    /// Review finding 4: the ordering contract end to end through the real
    /// block-response path and action queue. A rejected parent and its child (rejected as "rejected
    /// parent") are both handed back after the block; pushed to the queue as
    /// the sync driver does, the parent is admitted first and the child then
    /// resolves it as a present mempool parent — never as a rootless orphan.
    #[test]
    fn reconsidered_parent_is_applied_before_its_child_through_the_action_queue()
     {
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000, 50_000]);
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 1)], &[40_000]);
        let child_txid = child.compute_txid();
        let mut inner = inner_with(RejectUntilBlock {
            reject: HashSet::from([parent_txid]),
            blocks_connected: 0,
            reject_forever: HashSet::new(),
            remove_on_connect: HashSet::new(),
        });
        let mut sync_state = fresh_sync_state();
        sync_state.tx_cache.insert(parent_txid, parent);
        sync_state
            .known_fees
            .insert(parent_txid, Amount::from_sat(1_000));
        sync_state.tx_cache.insert(child_txid, child);
        sync_state
            .known_fees
            .insert(child_txid, Amount::from_sat(1_000));
        drive_insert(&mut inner, &mut sync_state, parent_txid);
        drive_insert(&mut inner, &mut sync_state, child_txid);
        assert!(
            sync_state.rejected_txs.contains(&parent_txid),
            "precondition: parent rejected"
        );
        assert!(
            sync_state.rejected_txs.contains(&child_txid),
            "precondition: child rejected as rejected-parent"
        );

        let block = make_resp_block(
            BlockHash::from_byte_array([6; 32]),
            BlockHash::all_zeros(),
            1,
            &[],
        );
        let reconsider = {
            let ss = SyncStateBorrowedMut {
                blocks_needed: &mut sync_state.blocks_needed,
                rejected_blocks: &mut sync_state.rejected_blocks,
                rejected_txs: &mut sync_state.rejected_txs,
                request_queue: &sync_state.request_queue,
                tx_cache: &mut sync_state.tx_cache,
                txs_needed: &mut sync_state.txs_needed,
                unavailable_txs: &mut sync_state.unavailable_txs,
                known_fees: &mut sync_state.known_fees,
                mempool_txids: &mut sync_state.mempool_txids,
            };
            futures::executor::block_on(connect_block::<_, RejectUntilBlock>(
                &mut inner, ss, &block,
            ))
            .expect("connect must not error")
        };
        assert_eq!(
            reconsider,
            vec![parent_txid, child_txid],
            "parents before children"
        );
        // Undo: connect_block already moved the tips and un-rejected both. Roll
        // the state back so the SAME block can now arrive the way production
        // sees it — as a Connected sequence message at the head of the action
        // queue followed by the block response — and the real push site in
        // `handle_resp_block` is what orders the inserts.
        inner.mempool.chain.tip = BlockHash::all_zeros();
        inner.unfiltered_mempool.tip = BlockHash::all_zeros();
        inner.reconsider_attempts.clear();
        for txid in [parent_txid, child_txid] {
            sync_state.rejected_txs.insert(txid);
            inner.unfiltered_mempool.txs.insert(txid);
        }
        sync_state
            .action_queue
            .push_back(SyncAction::SequenceMessage(
                SequenceMessage::BlockHash(BlockHashMessage {
                    block_hash: block.hash,
                    event: BlockHashEvent::Connected,
                    zmq_seq: 1,
                }),
            ));
        futures::executor::block_on(handle_resp_block::<_, RejectUntilBlock>(
            &mut inner,
            &mut sync_state,
            block.clone(),
        ))
        .expect("block response must not error");
        assert_eq!(inner.mempool.chain.tip, block.hash, "the block connected");
        assert!(
            inner.mempool.txs.0.is_empty(),
            "nothing admitted yet: the inserts are queued, not applied"
        );
        let applied =
            futures::executor::block_on(try_apply_next_sync_action::<
                _,
                RejectUntilBlock,
            >(
                &mut inner, &mut sync_state
            ))
            .expect("first drive must not error");
        assert!(applied);
        assert!(
            inner.mempool.txs.0.contains_key(&parent_txid),
            "the parent is admitted by the first drive"
        );
        assert!(
            !inner.mempool.txs.0.contains_key(&child_txid),
            "the child is not yet applied"
        );
        let applied =
            futures::executor::block_on(try_apply_next_sync_action::<
                _,
                RejectUntilBlock,
            >(
                &mut inner, &mut sync_state
            ))
            .expect("second drive must not error");
        assert!(applied);
        assert!(
            inner.mempool.txs.0.contains_key(&child_txid),
            "the child is admitted with its parent present"
        );
        assert!(sync_state.action_queue.is_empty());
        assert!(sync_state.rejected_txs.is_empty());
    }

    /// A node over real JSON-RPC, in process: what a `getrawtransaction` and a
    /// `getmempoolentry` really answer with or without `txindex`.
    mod fake_node {
        use std::sync::Arc;

        use bitcoin::{Amount, Transaction, Txid, Wtxid};
        use bitcoin_jsonrpsee::{
            http_client::{HttpClient, HttpClientBuilder},
            jsonrpsee::{
                RpcModule,
                server::{Server, ServerHandle},
                types::ErrorObjectOwned,
            },
        };
        use parking_lot::Mutex;
        use serde::Serialize;

        use std::collections::HashMap;

        #[derive(Default)]
        pub struct FakeNode {
            pub txindex: bool,
            pub mempool: HashMap<Txid, (Transaction, Amount)>,
            pub confirmed: HashMap<Txid, Transaction>,
            /// Every `getmempoolentry` answers with this code while set.
            pub entry_error: Option<i32>,
            /// Txids asked of `getrawtransaction`, in order.
            pub raw_tx_asked: Vec<Txid>,
            /// Txids asked of `getmempoolentry`, in order.
            pub entry_asked: Vec<Txid>,
        }

        #[derive(Clone, Serialize)]
        struct Fees {
            #[serde(with = "bitcoin::amount::serde::as_btc")]
            base: Amount,
            #[serde(with = "bitcoin::amount::serde::as_btc")]
            modified: Amount,
            #[serde(with = "bitcoin::amount::serde::as_btc")]
            ancestor: Amount,
            #[serde(with = "bitcoin::amount::serde::as_btc")]
            descendant: Amount,
        }

        #[derive(Clone, Serialize)]
        struct Entry {
            vsize: u64,
            weight: u64,
            descendantcount: u64,
            descendantsize: u64,
            ancestorcount: u64,
            ancestorsize: u64,
            wtxid: Wtxid,
            fees: Fees,
            depends: Vec<Txid>,
            spentby: Vec<Txid>,
            unbroadcast: bool,
        }

        const NOT_FOUND: i32 = -5;

        pub async fn start(
            node: Arc<Mutex<FakeNode>>,
        ) -> (HttpClient, ServerHandle) {
            let server = Server::builder()
                .build("127.0.0.1:0")
                .await
                .expect("bind fake node");
            let addr = server.local_addr().expect("fake node addr");
            let mut module = RpcModule::new(node);
            module
                .register_method("getrawtransaction", |params, ctx, _| {
                    let txid: Txid = params.sequence().next()?;
                    let mut node = ctx.lock();
                    node.raw_tx_asked.push(txid);
                    let tx = node
                        .mempool
                        .get(&txid)
                        .map(|(tx, _)| tx)
                        .or(node.confirmed.get(&txid).filter(|_| node.txindex));
                    match tx {
                        Some(tx) => {
                            Ok(bitcoin::consensus::encode::serialize_hex(tx))
                        }
                        None => Err(ErrorObjectOwned::owned::<()>(
                            NOT_FOUND,
                            "No such mempool transaction. Use -txindex or \
                             provide a block hash",
                            None,
                        )),
                    }
                })
                .unwrap();
            module
                .register_method("getmempoolentry", |params, ctx, _| {
                    let txid: Txid = params.sequence().next()?;
                    let mut node = ctx.lock();
                    node.entry_asked.push(txid);
                    if let Some(code) = node.entry_error {
                        return Err(ErrorObjectOwned::owned::<()>(
                            code,
                            "fake node failing on purpose",
                            None,
                        ));
                    }
                    match node.mempool.get(&txid) {
                        Some((tx, fee)) => Ok(Entry {
                            vsize: tx.vsize() as u64,
                            weight: tx.weight().to_wu(),
                            descendantcount: 1,
                            descendantsize: 1,
                            ancestorcount: 1,
                            ancestorsize: 1,
                            wtxid: tx.compute_wtxid(),
                            fees: Fees {
                                base: *fee,
                                modified: *fee,
                                ancestor: *fee,
                                descendant: *fee,
                            },
                            depends: Vec::new(),
                            spentby: Vec::new(),
                            unbroadcast: false,
                        }),
                        None => Err(ErrorObjectOwned::owned::<()>(
                            NOT_FOUND,
                            "Transaction not in mempool",
                            None,
                        )),
                    }
                })
                .unwrap();
            let handle = server.start(module);
            let client = HttpClientBuilder::default()
                .build(format!("http://{addr}"))
                .expect("fake node client");
            (client, handle)
        }
    }

    /// The sync task's loop, minus ZMQ: sequence messages are handed in, and
    /// every queued request goes to the node through the real
    /// [`batched_request`] until the queue runs dry.
    struct NodeHarness {
        node: Arc<parking_lot::Mutex<fake_node::FakeNode>>,
        client: bitcoin_jsonrpsee::http_client::HttpClient,
        _server: bitcoin_jsonrpsee::jsonrpsee::server::ServerHandle,
        inner: MempoolSyncInner<DefaultEnforcer>,
        sync_state: SyncState,
        mempool_seq: u64,
        round_trips: usize,
    }

    impl NodeHarness {
        async fn start(txindex: bool) -> Self {
            let node = Arc::new(parking_lot::Mutex::new(fake_node::FakeNode {
                txindex,
                ..Default::default()
            }));
            let (client, _server) = fake_node::start(node.clone()).await;
            Self {
                node,
                client,
                _server,
                inner: fresh_inner(),
                sync_state: fresh_sync_state(),
                mempool_seq: 0,
                round_trips: 0,
            }
        }

        fn is_synced(&self) -> bool {
            MempoolSyncingBorrowed {
                inner: &self.inner,
                sync_state: &self.sync_state,
            }
            .is_synced()
        }

        async fn seq(
            &mut self,
            txid: Txid,
            event: TxHashEvent,
        ) -> Result<(), SyncTaskError<DefaultEnforcer>> {
            self.mempool_seq += 1;
            let msg = SequenceMessage::TxHash(TxHashMessage {
                txid,
                event,
                mempool_seq: self.mempool_seq,
                zmq_seq: 0,
            });
            handle_seq_message(&mut self.inner, &mut self.sync_state, msg)?;
            while try_apply_next_sync_action(
                &mut self.inner,
                &mut self.sync_state,
            )
            .await?
            {}
            Ok(())
        }

        /// The node accepts `tx` and announces it.
        async fn node_accepts(
            &mut self,
            tx: &Transaction,
            fee_sat: u64,
        ) -> Result<(), SyncTaskError<DefaultEnforcer>> {
            let txid = tx.compute_txid();
            self.node
                .lock()
                .mempool
                .insert(txid, (tx.clone(), Amount::from_sat(fee_sat)));
            self.seq(txid, TxHashEvent::Added).await
        }

        /// The node drops `tx` and announces it.
        async fn node_drops(
            &mut self,
            txid: Txid,
        ) -> Result<(), SyncTaskError<DefaultEnforcer>> {
            self.node.lock().mempool.remove(&txid);
            self.seq(txid, TxHashEvent::Removed).await
        }

        /// Re-drive the action queue, as every stream message does.
        async fn drive(
            &mut self,
        ) -> Result<(), SyncTaskError<DefaultEnforcer>> {
            while try_apply_next_sync_action(
                &mut self.inner,
                &mut self.sync_state,
            )
            .await?
            {}
            Ok(())
        }

        /// What `init_sync_mempool` sets up from `getrawmempool`, with
        /// `known_fees` empty: `getrawmempool verbose` failed or raced.
        async fn snapshot(
            &mut self,
            txids: &[Txid],
        ) -> Result<(), SyncTaskError<DefaultEnforcer>> {
            for txid in txids {
                self.sync_state
                    .request_queue
                    .push_back(RequestItem::Tx(*txid, true));
                self.sync_state.txs_needed.replace(*txid);
                self.sync_state.mempool_txids.insert(*txid);
                self.sync_state
                    .action_queue
                    .push_back(SyncAction::InsertTx(*txid));
            }
            self.drive().await
        }

        /// A block mining `txs` connects. Mined txs leave the node's mempool
        /// without a `Removed`, as in Core.
        async fn connect(
            &mut self,
            txs: &[&Transaction],
        ) -> Result<(), SyncTaskError<DefaultEnforcer>> {
            let prev = self.inner.unfiltered_mempool.tip;
            let mut hash = [7u8; 32];
            hash[0] = self.inner.mempool.chain.blocks.len() as u8;
            let hash = BlockHash::from_byte_array(hash);
            {
                let mut node = self.node.lock();
                for tx in txs {
                    let txid = tx.compute_txid();
                    node.mempool.remove(&txid);
                    node.confirmed.insert(txid, (*tx).clone());
                }
            }
            let msg = SequenceMessage::BlockHash(BlockHashMessage {
                block_hash: hash,
                event: BlockHashEvent::Connected,
                zmq_seq: 0,
            });
            handle_seq_message(&mut self.inner, &mut self.sync_state, msg)?;
            // The fake node has no `getblock`; answer that request here.
            self.sync_state
                .request_queue
                .remove(&RequestItem::Block(hash));
            let block = make_resp_block(hash, prev, 1, txs);
            handle_resp(
                &mut self.inner,
                &mut self.sync_state,
                BatchedResponseItem::Single(ResponseItem::Block(Box::new(
                    block,
                ))),
            )
            .await
        }

        /// Serve the next queued request, if any.
        async fn serve_one(
            &mut self,
        ) -> Result<bool, SyncTaskError<DefaultEnforcer>> {
            let Some(Some(request)) =
                self.sync_state.request_queue.next().now_or_never()
            else {
                return Ok(false);
            };
            self.round_trips += 1;
            let resp = batched_request(&self.client, request).await?;
            handle_resp(&mut self.inner, &mut self.sync_state, resp).await?;
            Ok(true)
        }

        /// Serve queued requests until none is left.
        async fn pump(
            &mut self,
        ) -> Result<(), SyncTaskError<DefaultEnforcer>> {
            while self.serve_one().await? {}
            Ok(())
        }

        fn fee_of(&self, txid: &Txid) -> Option<Amount> {
            self.inner
                .mempool
                .txs
                .0
                .get(txid)
                .map(|(_tx, info)| info.fees.base)
        }
    }

    /// A tx in a block, and a tx in the node's mempool spending it.
    fn confirmed_parent_and_child() -> (Transaction, Transaction) {
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let child =
            make_tx(&[OutPoint::new(parent.compute_txid(), 0)], &[90_000]);
        (parent, child)
    }

    /// The standby's stall, 2026-11-05 (`prune=40000 txindex=0`): a tx the
    /// node accepted after startup is not in `known_fees`. Its confirmed
    /// parent cannot be fetched without `txindex`; the child must not depend
    /// on that.
    #[tokio::test]
    async fn no_txindex_new_tx_with_confirmed_parent_completes_sync() {
        let (parent, child) = confirmed_parent_and_child();
        let child_txid = child.compute_txid();
        let parent_txid = parent.compute_txid();
        let mut h = NodeHarness::start(false).await;
        h.node.lock().confirmed.insert(parent_txid, parent);

        h.node_accepts(&child, 10_000).await.unwrap();
        h.pump().await.unwrap();

        assert!(h.is_synced(), "sync must complete");
        assert!(!h.inner.abandoned_pool.contains(&child_txid));
        assert_eq!(h.fee_of(&child_txid), Some(Amount::from_sat(10_000)));
        assert!(
            !h.node.lock().raw_tx_asked.contains(&parent_txid),
            "a confirmed parent is not fetched for its fee"
        );
    }

    /// The same arrival on a node with `txindex`: same fee, same mempool.
    #[tokio::test]
    async fn txindex_new_tx_with_confirmed_parent_same_result() {
        let (parent, child) = confirmed_parent_and_child();
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(true).await;
        h.node.lock().confirmed.insert(parent.compute_txid(), parent);

        h.node_accepts(&child, 10_000).await.unwrap();
        h.pump().await.unwrap();

        assert!(h.is_synced());
        assert_eq!(h.fee_of(&child_txid), Some(Amount::from_sat(10_000)));
        assert_eq!(h.inner.mempool.txs.0.len(), 1);
    }

    /// A fee from `getrawmempool verbose` and one from `getmempoolentry` give
    /// the same mempool entry.
    #[tokio::test]
    async fn snapshot_fee_and_entry_fee_give_the_same_result() {
        let (_parent, child) = confirmed_parent_and_child();
        let child_txid = child.compute_txid();
        let fee = Amount::from_sat(10_000);

        let mut from_entry = NodeHarness::start(false).await;
        from_entry.node_accepts(&child, fee.to_sat()).await.unwrap();
        from_entry.pump().await.unwrap();

        let mut from_snapshot = NodeHarness::start(false).await;
        from_snapshot.sync_state.known_fees.insert(child_txid, fee);
        from_snapshot.node_accepts(&child, fee.to_sat()).await.unwrap();
        from_snapshot.pump().await.unwrap();
        assert!(
            from_snapshot.node.lock().entry_asked.is_empty(),
            "a fee already known is not asked for again"
        );

        assert_eq!(from_entry.fee_of(&child_txid), Some(fee));
        assert_eq!(
            from_entry.fee_of(&child_txid),
            from_snapshot.fee_of(&child_txid)
        );
    }

    /// `-5`: the tx is gone. It is not admitted, nothing is left waiting, and
    /// the `Removed` that follows applies cleanly.
    #[tokio::test]
    async fn entry_not_found_means_left_the_mempool() {
        let (parent, child) = confirmed_parent_and_child();
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;
        h.node.lock().confirmed.insert(parent.compute_txid(), parent);

        h.node_accepts(&child, 10_000).await.unwrap();
        h.serve_one().await.unwrap(); // the tx's bytes
        // Gone from the node, its `Removed` still on the wire.
        h.node.lock().mempool.remove(&child_txid);
        h.pump().await.unwrap();

        assert_eq!(h.fee_of(&child_txid), None, "never admitted");
        assert!(h.sync_state.unavailable_txs.contains(&child_txid));
        assert!(h.is_synced(), "nothing waits on a tx that left");

        h.seq(child_txid, TxHashEvent::Removed).await.unwrap();
        assert!(h.is_synced());
        assert!(!h.inner.unfiltered_mempool.txs.contains(&child_txid));
    }

    /// Any other failure says nothing about the tx: it surfaces as an error,
    /// the tx is not marked gone and is not admitted at any fee.
    #[tokio::test]
    async fn transient_entry_error_is_not_gone() {
        let (parent, child) = confirmed_parent_and_child();
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;
        h.node.lock().confirmed.insert(parent.compute_txid(), parent);
        h.node.lock().entry_error = Some(-28); // RPC_IN_WARMUP

        h.node_accepts(&child, 10_000).await.unwrap();
        let err = h.pump().await.expect_err("must surface");
        assert!(
            matches!(
                err,
                SyncTaskError::Request(RequestError::JsonRpc {
                    method: "getmempoolentry",
                    ..
                })
            ),
            "{err:?}"
        );
        assert!(!h.sync_state.unavailable_txs.contains(&child_txid));
        assert_eq!(h.fee_of(&child_txid), None);
        assert!(!h.is_synced(), "the tx is still waiting for its fee");

        // The node recovers: the same tx is admitted, at its real fee.
        h.node.lock().entry_error = None;
        h.drive().await.unwrap();
        h.pump().await.unwrap();
        assert!(h.is_synced());
        assert_eq!(h.fee_of(&child_txid), Some(Amount::from_sat(10_000)));
    }

    /// A `Removed` overtakes the fee fetch and cancels it: the queue head must
    /// not sit waiting for an answer that will never come (the apply timeout
    /// would kill the task).
    #[tokio::test]
    async fn removal_while_fee_is_pending_does_not_stall() {
        let (parent, child) = confirmed_parent_and_child();
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;
        h.node.lock().confirmed.insert(parent.compute_txid(), parent);

        h.node_accepts(&child, 10_000).await.unwrap();
        h.serve_one().await.unwrap(); // the tx's bytes
        h.node_drops(child_txid).await.unwrap();
        h.pump().await.unwrap();

        assert!(h.is_synced());
        assert_eq!(h.fee_of(&child_txid), None);
        assert!(!h.sync_state.known_fees.contains_key(&child_txid));
    }

    /// A burst costs one call per batch, not one per tx.
    #[tokio::test]
    async fn burst_of_new_txs_is_batched_with_right_fees() {
        const BURST: u64 = 2_500;
        let parent =
            make_tx(&[OutPoint::new(Txid::all_zeros(), 0)], &[100_000]);
        let parent_txid = parent.compute_txid();
        let mut h = NodeHarness::start(false).await;
        h.node.lock().confirmed.insert(parent_txid, parent);

        let mut txs = Vec::new();
        for i in 0..BURST {
            let fee = 1_000 + i;
            let tx = make_tx(&[OutPoint::new(parent_txid, 0)], &[100_000 - fee]);
            h.node_accepts(&tx, fee).await.unwrap();
            txs.push((tx.compute_txid(), fee));
        }
        h.pump().await.unwrap();

        assert!(h.is_synced());
        for (txid, fee) in &txs {
            assert_eq!(h.fee_of(txid), Some(Amount::from_sat(*fee)));
        }
        // 3 `getrawtransaction` + 3 `getmempoolentry` batches of up to 1,000.
        assert_eq!(h.round_trips, 6, "batched, not per tx");
        assert_eq!(h.node.lock().entry_asked.len(), BURST as usize);
        assert!(!h.node.lock().raw_tx_asked.contains(&parent_txid));
    }

    /// Unconfirmed chain in announcement order: parent, then child.
    #[tokio::test]
    async fn unconfirmed_chain_parent_announced_first() {
        let (grandparent, parent) = confirmed_parent_and_child();
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], &[85_000]);
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;
        h.node
            .lock()
            .confirmed
            .insert(grandparent.compute_txid(), grandparent);

        h.node_accepts(&parent, 10_000).await.unwrap();
        h.node_accepts(&child, 5_000).await.unwrap();
        h.pump().await.unwrap();

        assert!(h.is_synced());
        assert_eq!(h.fee_of(&parent_txid), Some(Amount::from_sat(10_000)));
        assert_eq!(h.fee_of(&child_txid), Some(Amount::from_sat(5_000)));
        let (_tx, info) = &h.inner.mempool.txs.0[&child_txid];
        assert!(info.depends.contains(&parent_txid), "package kept");
        assert_eq!(info.fees.ancestor, Amount::from_sat(15_000));
    }

    /// Unconfirmed chain out of order, as a snapshot can list it: the child
    /// comes first and must wait for its parent, never go in as an orphan.
    #[tokio::test]
    async fn unconfirmed_chain_child_listed_before_parent() {
        let (grandparent, parent) = confirmed_parent_and_child();
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], &[85_000]);
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;
        {
            let mut node = h.node.lock();
            node.confirmed.insert(grandparent.compute_txid(), grandparent);
            node.mempool
                .insert(parent_txid, (parent, Amount::from_sat(10_000)));
            node.mempool
                .insert(child_txid, (child, Amount::from_sat(5_000)));
        }
        h.snapshot(&[child_txid, parent_txid]).await.unwrap();
        h.pump().await.unwrap();

        assert!(h.is_synced());
        assert_eq!(h.fee_of(&parent_txid), Some(Amount::from_sat(10_000)));
        assert_eq!(h.fee_of(&child_txid), Some(Amount::from_sat(5_000)));
        let (_tx, info) = &h.inner.mempool.txs.0[&child_txid];
        assert!(info.depends.contains(&parent_txid), "no rootless orphan");
    }

    /// The edge case left: the node's mirror says a parent is in the mempool,
    /// the node cannot supply it. Sync still ends. The child stays out of the
    /// mempool until the node settles it, here by removing it.
    #[tokio::test]
    async fn genuinely_unavailable_parent_ends_sync_child_left_out() {
        let (_grandparent, parent) = confirmed_parent_and_child();
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], &[85_000]);
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;
        // The node has the child but no longer the parent.
        h.node
            .lock()
            .mempool
            .insert(child_txid, (child, Amount::from_sat(5_000)));
        h.snapshot(&[parent_txid, child_txid]).await.unwrap();
        h.pump().await.unwrap();

        assert!(h.is_synced(), "a parked tx must not hold sync open");
        assert!(h.inner.abandoned_pool.contains(&child_txid));
        assert_eq!(h.fee_of(&child_txid), None, "left out, not invented");

        h.node_drops(child_txid).await.unwrap();
        assert!(!h.inner.abandoned_pool.contains(&child_txid));
        assert!(h.is_synced());
    }

    /// The same parked child, then mined. No `Removed` ever comes for a mined
    /// tx, so the block has to clear it.
    #[tokio::test]
    async fn parked_child_mined_leaves_the_abandoned_pool() {
        let (_grandparent, parent) = confirmed_parent_and_child();
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], &[85_000]);
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;
        h.node
            .lock()
            .mempool
            .insert(child_txid, (child.clone(), Amount::from_sat(5_000)));
        h.snapshot(&[parent_txid, child_txid]).await.unwrap();
        h.pump().await.unwrap();
        assert!(h.inner.abandoned_pool.contains(&child_txid));

        h.connect(&[&child]).await.unwrap();

        assert!(!h.inner.abandoned_pool.contains(&child_txid));
        assert_eq!(h.fee_of(&child_txid), None);
        assert!(h.is_synced());
    }

    /// A parked child whose parent gets mined is back in, at its real fee.
    #[tokio::test]
    async fn parked_child_returns_when_parent_is_mined() {
        let (grandparent, parent) = confirmed_parent_and_child();
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], &[85_000]);
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;
        h.node
            .lock()
            .confirmed
            .insert(grandparent.compute_txid(), grandparent);
        h.node
            .lock()
            .mempool
            .insert(child_txid, (child, Amount::from_sat(5_000)));
        h.snapshot(&[parent_txid, child_txid]).await.unwrap();
        h.pump().await.unwrap();
        assert!(h.inner.abandoned_pool.contains(&child_txid));

        h.connect(&[&parent]).await.unwrap();
        h.pump().await.unwrap();

        assert!(h.inner.abandoned_pool.is_empty());
        assert_eq!(h.fee_of(&child_txid), Some(Amount::from_sat(5_000)));
        assert!(h.is_synced());
    }

    /// The block's response comes first, then the tx's fee answers -5 because
    /// that block mined it. A later child of that tx spends a confirmed coin:
    /// a leftover `unavailable` mark would park it for good.
    #[tokio::test]
    async fn gone_because_mined_leaves_no_stale_unavailable_mark() {
        let (grandparent, parent) = confirmed_parent_and_child();
        let parent_txid = parent.compute_txid();
        let child = make_tx(&[OutPoint::new(parent_txid, 0)], &[85_000]);
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;
        h.node
            .lock()
            .confirmed
            .insert(grandparent.compute_txid(), grandparent);

        h.node_accepts(&parent, 10_000).await.unwrap();
        h.serve_one().await.unwrap(); // the parent's bytes
        // Queue: Added(parent), Connected(block). The block's response goes
        // first; the node has already dropped the parent.
        h.node.lock().mempool.remove(&parent_txid);
        h.connect(&[&parent]).await.unwrap();
        h.pump().await.unwrap();
        assert!(
            !h.sync_state.unavailable_txs.contains(&parent_txid),
            "the mark goes with the block"
        );
        assert_eq!(h.fee_of(&parent_txid), None);

        h.node_accepts(&child, 5_000).await.unwrap();
        h.pump().await.unwrap();

        assert!(h.is_synced());
        assert!(!h.inner.abandoned_pool.contains(&child_txid));
        assert_eq!(h.fee_of(&child_txid), Some(Amount::from_sat(5_000)));
    }

    /// Removed, then announced again while the bytes are still cached: the
    /// old `unavailable` mark must not end the new insert as "left".
    #[tokio::test]
    async fn readded_cached_tx_is_admitted() {
        let (_parent, child) = confirmed_parent_and_child();
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;

        h.node_accepts(&child, 10_000).await.unwrap();
        h.pump().await.unwrap();
        assert!(h.fee_of(&child_txid).is_some());

        h.node_drops(child_txid).await.unwrap();
        assert_eq!(h.fee_of(&child_txid), None);
        assert!(!h.sync_state.known_fees.contains_key(&child_txid));

        h.node_accepts(&child, 10_000).await.unwrap();
        h.pump().await.unwrap();
        assert!(h.is_synced());
        assert_eq!(h.fee_of(&child_txid), Some(Amount::from_sat(10_000)));
    }

    /// `known_fees` now grows with every arrival, so it must shrink as well.
    #[tokio::test]
    async fn known_fees_forgets_mined_txs() {
        let (_parent, child) = confirmed_parent_and_child();
        let child_txid = child.compute_txid();
        let mut h = NodeHarness::start(false).await;
        h.node_accepts(&child, 10_000).await.unwrap();
        h.pump().await.unwrap();
        assert!(h.sync_state.known_fees.contains_key(&child_txid));

        h.connect(&[&child]).await.unwrap();

        assert!(!h.sync_state.known_fees.contains_key(&child_txid));
        assert_eq!(h.fee_of(&child_txid), None);
    }
}
