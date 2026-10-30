use std::{
    collections::HashMap,
    convert::Infallible,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use bitcoin::{
    Amount, Block, BlockHash, Network, ScriptBuf, Transaction, TxOut, Txid,
    Weight, WitnessMerkleNode, Wtxid, amount::CheckedSum, hashes::Hash as _,
    merkle_tree, script::PushBytesBuf,
};
use bitcoin_jsonrpsee::client::{
    BlockTemplate, BlockTemplateRequest, BlockTemplateTransaction,
    CoinbaseTxnOrValue, MODE_PROPOSAL, MODE_TEMPLATE, NetworkInfo,
};
use chrono::{DateTime, Utc};
use educe::Educe;
use futures::FutureExt;
use jsonrpsee::{core::RpcResult, proc_macros::rpc, types::ErrorCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    cusf_block_producer::{
        self, CusfBlockProducer, FilledBlockTemplate, InitialBlockTemplate,
        initial_block_template::SuffixTxsItem,
    },
    mempool::{self, Mempool, MempoolSync},
};

/// `getblocktemplate` result, which BIP22/BIP23 overload by request mode.
///
/// Serialized untagged because the wire format has no discriminant. The
/// caller knows which mode it asked for, so deserialization is written by
/// hand.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum BlockTemplateResponse {
    /// `mode: "template"`: a block template.
    Template(Box<BlockTemplate>),
    /// `mode: "proposal"`: the verdict on the proposed block.
    Proposal(Option<String>),
}

/// Not `#[serde(untagged)]`, which would be shorter but reports every failure
/// as "data did not match any variant of untagged enum BlockTemplateResponse",
/// discarding the reason the template itself failed to parse.
impl<'de> Deserialize<'de> for BlockTemplateResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{MapAccess, Visitor, value::MapAccessDeserializer};

        struct ResponseVisitor;

        impl<'de> Visitor<'de> for ResponseVisitor {
            type Value = BlockTemplateResponse;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                formatter.write_str(
                    "a block template object, a rejection reason string, or null",
                )
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(BlockTemplateResponse::Proposal(None))
            }

            fn visit_str<E: serde::de::Error>(
                self,
                reason: &str,
            ) -> Result<Self::Value, E> {
                Ok(BlockTemplateResponse::Proposal(Some(reason.to_owned())))
            }

            fn visit_string<E: serde::de::Error>(
                self,
                reason: String,
            ) -> Result<Self::Value, E> {
                Ok(BlockTemplateResponse::Proposal(Some(reason)))
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                BlockTemplate::deserialize(MapAccessDeserializer::new(map)).map(
                    |template| {
                        BlockTemplateResponse::Template(Box::new(template))
                    },
                )
            }
        }

        deserializer.deserialize_any(ResponseVisitor)
    }
}

impl BlockTemplateResponse {
    pub fn into_template(self) -> Option<Box<BlockTemplate>> {
        match self {
            Self::Template(template) => Some(template),
            Self::Proposal(_) => None,
        }
    }
}

const SERVER_CAPABILITIES: &[&str] = &["proposal"];

// Bitcoin Core's error codes, paired below with the exact messages it uses for
// `getblocktemplate`, so a caller cannot tell the two servers apart.
// https://github.com/bitcoin/bitcoin/blob/6c4fe401e908cff1b67d80035b117aae15fe7db6/src/rpc/protocol.h#L41
const RPC_TYPE_ERROR: i32 = -3;
// https://github.com/bitcoin/bitcoin/blob/6c4fe401e908cff1b67d80035b117aae15fe7db6/src/rpc/protocol.h#L44
const RPC_INVALID_PARAMETER: i32 = -8;
// https://github.com/bitcoin/bitcoin/blob/6c4fe401e908cff1b67d80035b117aae15fe7db6/src/rpc/protocol.h#L46
const RPC_DESERIALIZATION_ERROR: i32 = -22;

#[rpc(client, server)]
pub trait Rpc {
    /// BIP22/BIP23 `getblocktemplate`.
    ///
    /// With `mode` absent or `"template"`, builds a template. With
    /// `mode: "proposal"`, validates the block in `data` against the current
    /// tip instead of building anything.
    #[method(name = "getblocktemplate")]
    async fn get_block_template(
        &self,
        request: BlockTemplateRequest,
    ) -> RpcResult<BlockTemplateResponse>;

    /// Returns None if the block is invalid, otherwise the error code
    /// describing why the block was rejected.
    #[method(name = "submitblock")]
    async fn submit_block(
        &self,
        block_hex: String,
    ) -> RpcResult<Option<String>>;
}

// cached block templates, with their generation timestamp
#[derive(Clone, Debug)]
struct CachedBlockTemplates {
    coinbasetxn: Option<(Box<BlockTemplate>, DateTime<Utc>)>,
    coinbasevalue: Option<(Box<BlockTemplate>, DateTime<Utc>)>,
    cache_lifetime: Duration,
}

impl CachedBlockTemplates {
    fn new(cache_lifetime: Duration) -> Self {
        Self {
            coinbasetxn: None,
            coinbasevalue: None,
            cache_lifetime,
        }
    }

    /// returns a cached block template, if it is not expired
    fn try_take(
        self,
        coinbasetxn: bool,
        now: DateTime<Utc>,
        tip_block_hash: BlockHash,
        wake_gen: u64,
    ) -> Option<Box<BlockTemplate>> {
        let (block_template, generated_ts) = if coinbasetxn {
            self.coinbasetxn
        } else {
            self.coinbasevalue
        }?;
        let age = now.signed_duration_since(generated_ts).to_std().ok()?;
        if age > self.cache_lifetime {
            return None;
        }
        // A template from before a wake lacks what the wake was for, and its
        // `longpollid` would make the client's next long-poll return at once.
        let built_at_wake_gen = block_template
            .long_poll_id
            .as_deref()
            .and_then(parse_long_poll_wake_gen);
        if tip_block_hash == block_template.prev_blockhash
            && built_at_wake_gen == Some(wake_gen)
        {
            Some(block_template)
        } else {
            None
        }
    }

    fn put(&mut self, block_template: Box<BlockTemplate>, now: DateTime<Utc>) {
        match &block_template.coinbase_txn_or_value {
            CoinbaseTxnOrValue::Txn(_) => {
                self.coinbasetxn = Some((block_template, now))
            }
            CoinbaseTxnOrValue::ValueSats(_) => {
                self.coinbasevalue = Some((block_template, now))
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum CreateServerError {
    #[error("Sample block template cannot set coinbasetxn field")]
    SampleBlockTemplate,
}

/// How long a BIP22 long-poll `getblocktemplate` request may be held open
/// waiting for a tip change before a fresh template is returned anyway. The
/// timeout return refreshes `curtime` and the tx set, so it doubles as the
/// client's periodic-refresh cadence. If a client's request timeout is shorter
/// than this they simply abort before we respond and never benefits from long
/// polling.
const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// A `longpollid` is the tip hash the template was built on followed by
/// a monotonic sequence number and the [`TemplateWake`] generation the
/// template was built at (16 hex digits each). The sequence number makes the
/// id unique per template-build event, as BIP22 requires. Without it, a
/// timeout return would reuse the previous id for a different (refreshed)
/// template. Clients treat the whole string as opaque, per the BIP.
fn parse_long_poll_tip(long_poll_id: &str) -> Option<BlockHash> {
    long_poll_id.get(..64)?.parse().ok()
}

/// The [`TemplateWake`] generation a `longpollid` was built at, see
/// [`parse_long_poll_tip`]. `None` for an id without one.
fn parse_long_poll_wake_gen(long_poll_id: &str) -> Option<u64> {
    let gen_hex = long_poll_id.get(80..96)?;
    if long_poll_id.len() != 96 {
        return None;
    }
    u64::from_str_radix(gen_hex, 16).ok()
}

/// At most one [`TemplateWake`] generation bump per this interval. Each bump
/// makes every parked long-poll return and bypasses the template cache, and
/// bitcoind relays M8-shaped txs for the minimum relay fee, so wakes must not
/// be a way to flood every client (and through the pool every miner) with new
/// templates.
pub const MIN_WAKE_INTERVAL: Duration = Duration::from_secs(1);

/// Lets the block producer cut a BIP22 long-poll short when the template
/// should change although the tip has not, e.g. when a BMM request (M8) for
/// the current tip enters the mempool: such a request is only valid for the
/// next block, so waiting for the tip change or the timeout would miss it.
///
/// Each [`Self::wake`] bumps a generation counter, at most once per
/// [`MIN_WAKE_INTERVAL`]: a wake inside the interval is deferred to its end,
/// and any further wakes before then are folded into that one. So a burst
/// costs at most one wake-driven template per client per interval, while the
/// first wake after a quiet spell takes effect at once. Every template
/// carries the generation it was built at in its `longpollid`, and a
/// long-poll returns as soon as the generation differs from that one, so a
/// wake between two requests is not lost. Tip changes are not affected.
#[derive(Clone, Debug)]
pub struct TemplateWake(std::sync::Arc<TemplateWakeInner>);

#[derive(Debug)]
struct TemplateWakeInner {
    wake_gen: tokio::sync::watch::Sender<u64>,
    state: parking_lot::Mutex<TemplateWakeState>,
}

#[derive(Debug, Default)]
struct TemplateWakeState {
    /// When the generation was last bumped.
    last_bump: Option<std::time::Instant>,
    /// A bump is scheduled for the end of the current interval.
    deferred: bool,
}

impl TemplateWake {
    pub fn new() -> Self {
        Self(std::sync::Arc::new(TemplateWakeInner {
            wake_gen: tokio::sync::watch::Sender::new(0),
            state: parking_lot::Mutex::default(),
        }))
    }

    /// Ask parked long-polls to return a fresh template, now or, within
    /// [`MIN_WAKE_INTERVAL`] of the last bump, at the end of the interval.
    pub fn wake(&self) {
        let mut state = self.0.state.lock();
        if state.deferred {
            return;
        }
        let now = std::time::Instant::now();
        let due = state.last_bump.map(|last| last + MIN_WAKE_INTERVAL);
        match due {
            Some(due) if now < due => {
                // Outside a runtime there is no timer to defer to; bump now
                // rather than lose the wake.
                let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                    Self::bump(&self.0, &mut state, now);
                    return;
                };
                state.deferred = true;
                let inner = std::sync::Arc::clone(&self.0);
                drop(state);
                runtime.spawn(async move {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(
                        due,
                    ))
                    .await;
                    let mut state = inner.state.lock();
                    state.deferred = false;
                    Self::bump(&inner, &mut state, std::time::Instant::now());
                });
            }
            _ => Self::bump(&self.0, &mut state, now),
        }
    }

    fn bump(
        inner: &TemplateWakeInner,
        state: &mut TemplateWakeState,
        now: std::time::Instant,
    ) {
        state.last_bump = Some(now);
        inner
            .wake_gen
            .send_modify(|wake_gen| *wake_gen = wake_gen.wrapping_add(1));
    }

    /// Observe the wake generation.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.0.wake_gen.subscribe()
    }
}

impl Default for TemplateWake {
    fn default() -> Self {
        Self::new()
    }
}

/// Park a long-poll whose template was built on `long_poll_tip` at wake
/// generation `long_poll_wake_gen` until the tip changes, the wake generation
/// differs from `long_poll_wake_gen`, or `timeout` passes, whichever is
/// first. `wait_for` checks the current value before waiting, so a change
/// between building the previous template and parking is not missed. With no
/// `long_poll_wake_gen` (an id from before wakes existed), the generation at
/// the time of parking is used.
async fn long_poll_wait(
    mut tip_rx: tokio::sync::watch::Receiver<BlockHash>,
    wake_rx: Option<tokio::sync::watch::Receiver<u64>>,
    long_poll_tip: BlockHash,
    long_poll_wake_gen: Option<u64>,
    timeout: Duration,
) {
    let tip_changed = tip_rx.wait_for(|tip| *tip != long_poll_tip);
    let woken = async {
        match wake_rx {
            Some(mut wake_rx) => {
                let parked_gen =
                    long_poll_wake_gen.unwrap_or_else(|| *wake_rx.borrow());
                // A closed channel never wakes: fall back to tip/timeout.
                if wake_rx.wait_for(|g| *g != parked_gen).await.is_err() {
                    std::future::pending::<()>().await
                }
            }
            None => std::future::pending::<()>().await,
        }
    };
    let _timed_out: Result<(), tokio::time::error::Elapsed> =
        tokio::time::timeout(
            timeout,
            futures::future::select(
                std::pin::pin!(tip_changed),
                std::pin::pin!(woken),
            ),
        )
        .await
        .map(|_| ());
}

pub struct Server<Enforcer, RpcClient> {
    coinbase_spk: ScriptBuf,
    mempool: MempoolSync<Enforcer>,
    network: Network,
    network_info: NetworkInfo,
    rpc_client: RpcClient,
    cached_block_templates: Option<parking_lot::RwLock<CachedBlockTemplates>>,
    sample_block_template: BlockTemplate,
    /// Map of block hashes to known targets for the next block
    known_targets: parking_lot::RwLock<HashMap<BlockHash, bitcoin::Target>>,
    /// Tip observer used to park long-poll requests until the tip changes.
    tip_rx: tokio::sync::watch::Receiver<BlockHash>,
    /// Uniquifying suffix for `longpollid`s, see [`parse_long_poll_tip`].
    long_poll_seq: AtomicU64,
    /// Generation observer of the producer's [`TemplateWake`], if any, see
    /// [`Self::with_template_wake`].
    wake_rx: Option<tokio::sync::watch::Receiver<u64>>,
}

impl<Enforcer, RpcClient> Server<Enforcer, RpcClient> {
    /// `cached_template_lifetime`, if set, allows a block template to be
    /// cached for up to the specified duration.
    /// The block template will be regenerated if the cached template is older
    /// than the specified duration.
    pub fn new(
        coinbase_spk: ScriptBuf,
        mempool: MempoolSync<Enforcer>,
        network: Network,
        network_info: NetworkInfo,
        rpc_client: RpcClient,
        cached_template_lifetime: Option<Duration>,
        sample_block_template: BlockTemplate,
    ) -> Result<Self, CreateServerError> {
        if matches!(
            sample_block_template.coinbase_txn_or_value,
            CoinbaseTxnOrValue::Txn(_)
        ) {
            return Err(CreateServerError::SampleBlockTemplate);
        };
        let cached_block_templates =
            cached_template_lifetime.map(|cached_template_lifetime| {
                parking_lot::RwLock::new(CachedBlockTemplates::new(
                    cached_template_lifetime,
                ))
            });
        let tip_rx = mempool.subscribe_tip();
        Ok(Self {
            coinbase_spk,
            mempool,
            network,
            network_info,
            rpc_client,
            cached_block_templates,
            sample_block_template,
            known_targets: parking_lot::RwLock::new(HashMap::new()),
            tip_rx,
            long_poll_seq: AtomicU64::new(0),
            wake_rx: None,
        })
    }

    /// Let `template_wake` cut long-polls short, see [`TemplateWake`].
    pub fn with_template_wake(mut self, template_wake: &TemplateWake) -> Self {
        self.wake_rx = Some(template_wake.subscribe());
        self
    }

    /// Current [`TemplateWake`] generation, 0 without one.
    fn wake_gen(&self) -> u64 {
        self.wake_rx.as_ref().map_or(0, |wake_rx| *wake_rx.borrow())
    }
}

fn log_error<Err>(err: Err) -> anyhow::Error
where
    anyhow::Error: From<Err>,
{
    let err = anyhow::Error::from(err);
    tracing::error!("{err:#}");
    err
}

fn internal_error<Err>(err: Err) -> jsonrpsee::types::ErrorObjectOwned
where
    anyhow::Error: From<Err>,
{
    let err = anyhow::Error::from(err);
    let err_msg = format!("{err:#}");
    jsonrpsee::types::ErrorObjectOwned::owned(
        ErrorCode::InternalError.code(),
        ErrorCode::InternalError.message(),
        Some(err_msg),
    )
}

/// Renders an iterator as a bounded debug list.
///
/// Takes a closure rather than a collection so `Display::fmt` can start a fresh
/// iterator on each call. That lets callers pass a projection without
/// materialising it, and since `tracing` only evaluates field expressions when
/// the event is enabled, a disabled event costs nothing.
#[repr(transparent)]
struct DisplayList<F>(F);

impl<F, I> std::fmt::Display for DisplayList<F>
where
    F: Fn() -> I,
    I: Iterator,
    I::Item: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[repr(transparent)]
        struct DebugDisplay<T>(T);

        impl<T> std::fmt::Debug for DebugDisplay<T>
        where
            T: std::fmt::Display,
        {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        /// Entries rendered in full by [`DisplayList`] before the rest are elided.
        const DISPLAY_LIST_MAX: usize = 10;

        let mut iter = (self.0)();
        let () = f
            .debug_list()
            .entries((&mut iter).take(DISPLAY_LIST_MAX).map(DebugDisplay))
            .finish()?;
        match iter.count() {
            0 => Ok(()),
            elided => write!(f, " (+{elided} more)"),
        }
    }
}

/// Compute the block reward for the specified height
fn get_block_reward(height: u32, fees: Amount, network: Network) -> Amount {
    let subsidy_sats = 50 * Amount::ONE_BTC.to_sat();
    let subsidy_halving_interval = match network {
        Network::Regtest => 150,
        _ => bitcoin::constants::SUBSIDY_HALVING_INTERVAL,
    };
    let halvings = height / subsidy_halving_interval;
    if halvings >= 64 {
        fees
    } else {
        fees + Amount::from_sat(subsidy_sats >> halvings)
    }
}

const WITNESS_RESERVED_VALUE: [u8; 32] = [0; 32];

/// Add witness commitment output to the coinbase tx, and return a copy of the
/// witness commitment spk.
/// The coinbase tx should not include the witness commitment txout.
/// Signet challenge should be `Some` for signets, and `None` otherwise.
fn add_witness_commitment_output(
    coinbase_tx: &mut Transaction,
    transactions: &[BlockTemplateTransaction],
) -> ScriptBuf {
    let witness_root = {
        let hashes = std::iter::once(Wtxid::all_zeros().to_raw_hash())
            .chain(transactions.iter().map(|tx| tx.hash.to_raw_hash()));
        merkle_tree::calculate_root(hashes)
            .map(WitnessMerkleNode::from_raw_hash)
            .unwrap()
    };
    let witness_commitment = Block::compute_witness_commitment(
        &witness_root,
        &WITNESS_RESERVED_VALUE,
    );
    // https://github.com/bitcoin/bips/blob/master/bip-0141.mediawiki#commitment-structure
    let witness_commitment_spk = {
        const WITNESS_COMMITMENT_HEADER: [u8; 4] = [0xaa, 0x21, 0xa9, 0xed];
        let mut push_bytes = PushBytesBuf::from(WITNESS_COMMITMENT_HEADER);
        let () = push_bytes
            .extend_from_slice(witness_commitment.as_byte_array())
            .unwrap();
        ScriptBuf::new_op_return(push_bytes)
    };
    coinbase_tx.output.push(TxOut {
        value: Amount::ZERO,
        script_pubkey: witness_commitment_spk.clone(),
    });
    witness_commitment_spk
}

#[derive(Debug, Error)]
enum FinalizeCoinbaseTxError {
    #[error("Coinbase reward underflow")]
    CoinbaseRewardUnderflow,
    #[error("Fee overflow")]
    FeeOverflow,
    #[error(
        "Negative tx fee for tx `{txid}` at index `{tx_index}`: `{}`",
        .fee.display_dynamic()
    )]
    NegativeTxFee {
        txid: Txid,
        tx_index: usize,
        fee: bitcoin::SignedAmount,
    },
}

/// Generate a BIP34 height script
fn bip34_height_script(height: u32) -> ScriptBuf {
    let mut builder =
        bitcoin::blockdata::script::Builder::new().push_int(height as i64);
    while builder.len() < 2 {
        builder = builder.push_opcode(bitcoin::opcodes::OP_0);
    }
    builder.into_script()
}

/// Finalize coinbase tx.
/// The witness commitment output in the coinbase is not added on signets.
/// Returns the coinbase tx, and (on networks other than signets) the witness
/// commitment spk.
fn finalize_coinbase_tx(
    coinbase_spk: ScriptBuf,
    block_height: u32,
    network: Network,
    mut coinbase_txouts: Vec<TxOut>,
    transactions: &[BlockTemplateTransaction],
) -> Result<(Transaction, Option<ScriptBuf>), FinalizeCoinbaseTxError> {
    let fees = transactions.iter().enumerate().try_fold(
        Amount::ZERO,
        |fees_acc, (tx_index, tx)| {
            let fee = tx.fee.to_unsigned().map_err(|_| {
                FinalizeCoinbaseTxError::NegativeTxFee {
                    txid: tx.txid,
                    tx_index,
                    fee: tx.fee,
                }
            })?;
            fees_acc
                .checked_add(fee)
                .ok_or(FinalizeCoinbaseTxError::FeeOverflow)
        },
    )?;
    let block_reward = get_block_reward(block_height, fees, network);
    // Remaining block reward value to add to coinbase txouts
    let coinbase_reward = coinbase_txouts.iter().try_fold(
        block_reward,
        |reward_acc, txout| {
            reward_acc
                .checked_sub(txout.value)
                .ok_or(FinalizeCoinbaseTxError::CoinbaseRewardUnderflow)
        },
    )?;
    tracing::debug!(
        block_reward = %block_reward.display_dynamic(),
        coinbase_reward = %coinbase_reward.display_dynamic(),
        fees = %fees.display_dynamic(),
    );
    if coinbase_reward > Amount::ZERO {
        coinbase_txouts.push(TxOut {
            value: coinbase_reward,
            script_pubkey: coinbase_spk,
        })
    }
    let mut coinbase_tx = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![bitcoin::TxIn {
            previous_output: bitcoin::OutPoint {
                txid: Txid::all_zeros(),
                vout: 0xFFFF_FFFF,
            },
            sequence: bitcoin::Sequence::MAX,
            witness: bitcoin::Witness::from_slice(&[WITNESS_RESERVED_VALUE]),
            script_sig: bip34_height_script(block_height),
        }],
        output: coinbase_txouts,
    };
    let witness_commitment_spk = match network {
        Network::Signet => None,
        _ => Some(add_witness_commitment_output(
            &mut coinbase_tx,
            transactions,
        )),
    };
    Ok((coinbase_tx, witness_commitment_spk))
}

#[derive(Educe)]
#[educe(Debug(bound()))]
#[derive(Error)]
enum BuildBlockError<BP>
where
    BP: CusfBlockProducer,
{
    #[error(transparent)]
    FinalizeBlockTemplate(BP::FinalizeBlockTemplateError),
    #[error(transparent)]
    FinalizeCoinbaseTx(#[from] FinalizeCoinbaseTxError),
    #[error(transparent)]
    InitialBlockTemplate(BP::InitialBlockTemplateError),
    #[error(transparent)]
    MempoolInsert(#[from] crate::mempool::MempoolInsertError),
    #[error(transparent)]
    MempoolRemove(#[from] crate::mempool::MempoolRemoveError),
    #[error("weight overflow")]
    WeightOverflow,
}

// select block txs, and coinbase txouts if coinbasetxn is set
async fn block_txs<const COINBASE_TXN: bool, BP>(
    block_producer: &BP,
    mempool: &crate::mempool::Mempool,
    parent_block_hash: &BlockHash,
    coinbase_spk: &ScriptBuf,
)
    -> Result<
            (<typewit::const_marker::Bool<COINBASE_TXN> as cusf_block_producer::CoinbaseTxn>::CoinbaseTxouts,
             Vec<BlockTemplateTransaction>),
            BuildBlockError<BP>
        >
    where BP: CusfBlockProducer,
    typewit::const_marker::Bool<COINBASE_TXN>: cusf_block_producer::CoinbaseTxn
     {
    let mut initial_block_template =
        InitialBlockTemplate::<COINBASE_TXN>::default();
    tracing::debug!("Generating initial block template");
    let () = block_producer
        .initial_block_template(
            parent_block_hash,
            typewit::MakeTypeWitness::MAKE,
            &mut initial_block_template,
        )
        .await
        .map_err(BuildBlockError::InitialBlockTemplate)?;
    let prefix_txids: hashlink::LinkedHashSet<Txid> = initial_block_template
        .prefix_txs
        .iter()
        .map(|(tx, _fee)| tx.compute_txid())
        .collect();
    let suffix_txids: hashlink::LinkedHashSet<Txid> = initial_block_template
        .suffix_txs
        .iter()
        .filter_map(|suffix_tx| match suffix_tx {
            SuffixTxsItem::Tx((tx, _)) => Some(tx.compute_txid()),
            SuffixTxsItem::Reserved { .. } => None,
        })
        .collect();
    {
        tracing::debug!(
            prefix_txids = %DisplayList(|| prefix_txids.iter()),
            suffix_txids = %DisplayList(|| suffix_txids.iter()),
        );
    }
    let mut mempool = mempool.clone();
    tracing::debug!("Inserting prefix txs into cloned mempool");
    for (tx, fee) in initial_block_template.prefix_txs.iter().cloned() {
        let weight = tx.weight();
        match mempool.insert(tx, fee, Default::default(), weight) {
            Ok(_)
            | Err(crate::mempool::MempoolInsertError::TxAlreadyExists {
                ..
            }) => (),
            Err(err) => return Err(err.into()),
        }
    }
    // depends field must be set later
    let mut res_txs: Vec<_> = {
        initial_block_template
            .prefix_txs
            .iter()
            .map(|(tx, fee)| BlockTemplateTransaction {
                data: bitcoin::consensus::serialize(tx),
                txid: tx.compute_txid(),
                hash: tx.compute_wtxid(),
                depends: Vec::new(),
                fee: (*fee).try_into().unwrap(),
                sigops: None,
                weight: tx.weight().to_wu(),
            })
            .collect()
    };
    // Remove prefix/excluded/suffix txs
    {
        tracing::debug!("Removing prefix txs");
        let _removed_txs = mempool
            .try_filter(false, |tx, _| {
                let txid = tx.compute_txid();
                Result::<_, Infallible>::Ok(!prefix_txids.contains(&txid))
            })
            .map_err(|err| match err {
                either::Either::Left(err) => err,
            })?;
        tracing::debug!("Removing prefix txs");
        tracing::debug!(
            excluded_txids =
                %DisplayList(|| initial_block_template.exclude_mempool_txs.iter()),
            "Removing excluded/suffix txs"
        );
        let _removed_txs = mempool
            .try_filter(true, |tx, _| {
                let txid = tx.compute_txid();
                let excluded =
                    initial_block_template.exclude_mempool_txs.contains(&txid)
                        || suffix_txids.contains(&txid);
                Result::<_, Infallible>::Ok(!excluded)
            })
            .map_err(|err| match err {
                either::Either::Left(err) => err,
            })?;
    }
    tracing::debug!("Proposing txs for inclusion in block");
    let coinbase_txouts_weight = {
        let mut txouts_weight = Weight::ZERO;
        match typewit::MakeTypeWitness::MAKE {
            typewit::const_marker::BoolWit::True(wit) => {
                let wit = wit.map(cusf_block_producer::CoinbaseTxouts);
                for tx_out in wit
                    .in_ref()
                    .to_right(&initial_block_template.coinbase_txouts)
                {
                    txouts_weight = txouts_weight
                        .checked_add(tx_out.weight())
                        .ok_or(BuildBlockError::WeightOverflow)?;
                }
            }
            typewit::const_marker::BoolWit::False(_) => (),
        };
        const COINBASE_WITNESS_COMMITMENT_TXOUT_WEIGHT: Weight = {
            let weight_wu = Weight::from_non_witness_data_size(Amount::SIZE as u64).to_wu()
                // SPK weight
                + Weight::from_non_witness_data_size(39).to_wu();
            Weight::from_wu(weight_wu)
        };
        // The block reward payout txout is appended to the coinbase txouts in
        // `finalize_coinbase_tx`, and its spk has no fixed size, so its weight
        // must be reserved here.
        let payout_txout_weight = TxOut {
            value: Amount::MAX_MONEY,
            script_pubkey: coinbase_spk.clone(),
        }
        .weight();
        Weight::from_wu(
            txouts_weight.to_wu()
                + COINBASE_WITNESS_COMMITMENT_TXOUT_WEIGHT.to_wu()
                + payout_txout_weight.to_wu(),
        )
    };
    let prefix_txs_weight = {
        let mut weight = Weight::ZERO;
        for (tx, _) in &initial_block_template.prefix_txs {
            weight = weight
                .checked_add(tx.weight())
                .ok_or(BuildBlockError::WeightOverflow)?;
        }
        weight
    };
    let suffix_txs_weight = {
        let mut weight = Weight::ZERO;
        for suffix_tx in &initial_block_template.suffix_txs {
            weight = weight
                .checked_add(suffix_tx.weight())
                .ok_or(BuildBlockError::WeightOverflow)?;
        }
        weight
    };
    let initial_block_template_weight = coinbase_txouts_weight
        .checked_add(prefix_txs_weight)
        .and_then(|weight| weight.checked_add(suffix_txs_weight))
        .ok_or(BuildBlockError::WeightOverflow)?;
    let mempool_txs = mempool.propose_txs(Some(Weight::from_wu(
        mempool::MAX_USABLE_BLOCK_WEIGHT
            .to_wu()
            .saturating_sub(initial_block_template_weight.to_wu()),
    )))?;
    tracing::debug!(
        proposed_txids = %DisplayList(|| mempool_txs.iter().map(|tx| tx.txid)),
        "Proposed {} tx(s) for inclusion in block", mempool_txs.len(),
    );
    let mut filled_block_template: FilledBlockTemplate<COINBASE_TXN> =
        initial_block_template.into();
    filled_block_template
        .prefix_txs
        .extend(mempool_txs.iter().map(|tx| {
            let fee = tx.fee.unsigned_abs();
            let tx = bitcoin::consensus::deserialize(&tx.data).unwrap();
            (tx, fee)
        }));
    tracing::debug!("Adding block template suffix");
    let () = block_producer
        .finalize_block_template(
            parent_block_hash,
            typewit::MakeTypeWitness::MAKE,
            &mut filled_block_template,
        )
        .await
        .map_err(BuildBlockError::FinalizeBlockTemplate)?;
    res_txs.extend(mempool_txs);
    res_txs.extend(filled_block_template.suffix_txs().iter().map(
        |(tx, fee)| BlockTemplateTransaction {
            data: bitcoin::consensus::serialize(tx),
            txid: tx.compute_txid(),
            hash: tx.compute_wtxid(),
            depends: Vec::new(),
            fee: (*fee).try_into().unwrap(),
            sigops: None,
            weight: tx.weight().to_wu(),
        },
    ));
    // Fill depends
    {
        let mut tx_indexes = std::collections::HashMap::new();
        for (idx, tx) in res_txs.iter_mut().enumerate() {
            tx_indexes.insert(tx.txid, idx as u32);
            let tx_inputs =
                bitcoin::consensus::deserialize::<Transaction>(&tx.data)
                    .unwrap()
                    .input;
            for txin in tx_inputs {
                if let Some(tx_idx) = tx_indexes.get(&txin.previous_output.txid)
                {
                    tx.depends.push(*tx_idx)
                }
            }
            tx.depends.sort();
            tx.depends.dedup();
        }
    }
    Ok((filled_block_template.coinbase_txouts, res_txs))
}

struct MempoolQueryOutput {
    prev_blockhash: BlockHash,
    tip_block_mediantime: u32,
    tip_block_height: u32,
    coinbase_txn: Option<Transaction>,
    block_txs: Vec<BlockTemplateTransaction>,
    default_witness_commitment: Option<Vec<u8>>,
}

/// Query the mempool state during GBT
async fn query_mempool<'a, Enforcer>(
    coinbase_spk: ScriptBuf,
    coinbasetxn: bool,
    network: Network,
    mempool: &'a Mempool,
    enforcer: &'a Enforcer,
) -> Result<MempoolQueryOutput, BuildBlockError<Enforcer>>
where
    Enforcer: CusfBlockProducer,
{
    let tip_block = mempool.tip();
    let (coinbase_txn, block_txs, default_witness_commitment) = if coinbasetxn {
        tracing::debug!("Filling block txs");
        let (coinbase_txouts, block_txs) = block_txs::<true, _>(
            enforcer,
            mempool,
            &tip_block.hash,
            &coinbase_spk,
        )
        .await?;
        tracing::debug!("Finalizing coinbase txn");
        let (coinbase_tx, witness_commitment_spk) = finalize_coinbase_tx(
            coinbase_spk,
            tip_block.height + 1,
            network,
            coinbase_txouts,
            &block_txs,
        )?;
        let default_witness_commitment =
            witness_commitment_spk.map(|spk| spk.to_bytes());
        (Some(coinbase_tx), block_txs, default_witness_commitment)
    } else {
        let ((), block_txs) = block_txs::<false, _>(
            enforcer,
            mempool,
            &tip_block.hash,
            &coinbase_spk,
        )
        .await?;
        (None, block_txs, None)
    };
    Ok(MempoolQueryOutput {
        prev_blockhash: tip_block.hash,
        tip_block_mediantime: tip_block.mediantime,
        tip_block_height: tip_block.height,
        coinbase_txn,
        block_txs,
        default_witness_commitment,
    })
}

enum CachedMempoolQueryOutput {
    Cached(Box<BlockTemplate>),
    Queried(Box<MempoolQueryOutput>),
}

impl<BP, RpcClient> Server<BP, RpcClient>
where
    BP: CusfBlockProducer + Send + Sync + 'static,
    RpcClient: bitcoin_jsonrpsee::client::MainClient
        + jsonrpsee::core::client::ClientT
        + Send
        + Sync
        + 'static,
{
    async fn validate_proposal(
        &self,
        request: BlockTemplateRequest,
    ) -> RpcResult<Option<String>> {
        // https://github.com/bitcoin/bitcoin/blob/6c4fe401e908cff1b67d80035b117aae15fe7db6/src/rpc/mining.cpp#L733
        let Some(data) = request.data().map(str::to_owned) else {
            return Err(jsonrpsee::types::ErrorObjectOwned::owned(
                RPC_TYPE_ERROR,
                "Missing data String key for proposal",
                None::<()>,
            ));
        };
        // https://github.com/bitcoin/bitcoin/blob/6c4fe401e908cff1b67d80035b117aae15fe7db6/src/rpc/mining.cpp#L737
        let block: Block = bitcoin::consensus::encode::deserialize_hex(&data)
            .map_err(|_| {
            jsonrpsee::types::ErrorObjectOwned::owned(
                RPC_DESERIALIZATION_ERROR,
                "Block decode failed",
                None::<()>,
            )
        })?;

        // Consensus validity is the node's question, so ask the node rather
        // than reimplementing `CheckBlock` here. This covers the `bad-*`
        // reject reasons and the `duplicate*` family.
        //
        // A failure to reach the node must never read as acceptance, so the
        // only non-error outcome here is an answer the node actually gave.
        let core_verdict: Option<String> = self
            .rpc_client
            .request(
                "getblocktemplate",
                jsonrpsee::rpc_params![BlockTemplateRequest {
                    mode: Some(MODE_PROPOSAL.into()),
                    data: Some(data.into()),
                    rules: request.rules,
                    capabilities: Default::default(),
                    long_poll_id: None,
                }],
            )
            .await
            .map_err(|err| match err {
                // The node's own -8/-22 and friends are more precise than
                // anything restated here, so pass them through unchanged.
                jsonrpsee::core::ClientError::Call(err) => err,
                err => {
                    let err = log_error(err);
                    internal_error(err)
                }
            })?;
        if core_verdict.is_some() {
            return Ok(core_verdict);
        }

        // Holds the mempool read lock for the duration of the enforcer's
        // check, which is the same contract `query_mempool` runs under.
        self.mempool
            .with(|mempool, enforcer| {
                let tip = mempool.tip().hash;
                async move {
                    // The enforcer can only evaluate a block against its own
                    // tip, so anything else is unanswerable rather than
                    // invalid.
                    if block.header.prev_blockhash != tip {
                        return Ok(Some(
                            "inconclusive-not-best-prevblk".to_owned(),
                        ));
                    }
                    enforcer.validate_block(&block).map_err(|err| {
                        let err = log_error(err);
                        internal_error(err)
                    })
                }
                .boxed()
            })
            .await
            .ok_or_else(|| {
                let err = anyhow::anyhow!("Mempool unavailable");
                let err = log_error(err);
                internal_error(err)
            })?
    }

    async fn build_block_template(
        &self,
        request: BlockTemplateRequest,
    ) -> RpcResult<BlockTemplate> {
        const NONCE_RANGE: [u8; 8] = [0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF];

        // BIP22 long polling: when the request carries the longpollid of the
        // tip we are currently on, park it until the tip changes or the
        // producer's `TemplateWake` fires, then fall through and serve a
        // fresh template. A stale or unparseable longpollid returns
        // immediately. The timeout return serves a refreshed template (new
        // curtime / tx set) for the same tip. See `long_poll_wait`.
        if let Some(long_poll_id) = &request.long_poll_id
            && let Some(long_poll_tip) = parse_long_poll_tip(long_poll_id)
        {
            long_poll_wait(
                self.tip_rx.clone(),
                self.wake_rx.clone(),
                long_poll_tip,
                parse_long_poll_wake_gen(long_poll_id),
                LONG_POLL_TIMEOUT,
            )
            .await;
        }
        // Read before the mempool is queried: a wake fires while the mempool
        // sync holds the write lock it inserts the tx under, so a template
        // built at generation `wake_gen` contains every tx whose wake made
        // the generation `wake_gen` or lower. A wake racing this read at
        // worst yields one extra template.
        let wake_gen = self.wake_gen();

        let now = Utc::now();
        let BlockTemplate {
            version,
            ref rules,
            ref version_bits_available,
            version_bits_required,
            ref coinbase_aux,
            ref coinbase_txn_or_value,
            ref mutable,
            sigop_limit,
            size_limit,
            weight_limit,
            ref signet_challenge,
            ..
        } = self.sample_block_template;
        let current_time_adjusted =
            (now.timestamp() + self.network_info.time_offset_s) as u64;
        let cached_mempool_query_output = self
            .mempool
            .with(|mempool, enforcer| {
                let coinbasetxn = request.capabilities.contains("coinbasetxn");
                let cached_block_templates = self
                    .cached_block_templates
                    .as_ref()
                    .map(|cached_block_templates| {
                        cached_block_templates.read().clone()
                    });
                let coinbase_spk = self.coinbase_spk.clone();
                let network = self.network;
                async move {
                    if let Some(cached_block_templates) = cached_block_templates
                        && let Some(cached_block_template) =
                            cached_block_templates.try_take(
                                coinbasetxn,
                                now,
                                mempool.tip().hash,
                                wake_gen,
                            )
                    {
                        Ok(CachedMempoolQueryOutput::Cached(
                            cached_block_template,
                        ))
                    } else {
                        query_mempool(
                            coinbase_spk,
                            request.capabilities.contains("coinbasetxn"),
                            network,
                            mempool,
                            enforcer,
                        )
                        .await
                        .map(|query_mempool_output| {
                            CachedMempoolQueryOutput::Queried(Box::new(
                                query_mempool_output,
                            ))
                        })
                    }
                }
                .boxed()
            })
            .await
            .ok_or_else(|| {
                let err = anyhow::anyhow!("Mempool unavailable");
                let err = log_error(err);
                internal_error(err)
            })?
            .map_err(|err: BuildBlockError<_>| {
                let err = log_error(err);
                internal_error(err)
            })?;
        let MempoolQueryOutput {
            prev_blockhash,
            tip_block_mediantime,
            tip_block_height,
            coinbase_txn,
            block_txs,
            default_witness_commitment,
        } = match cached_mempool_query_output {
            CachedMempoolQueryOutput::Cached(block_template) => {
                return Ok(*block_template);
            }
            CachedMempoolQueryOutput::Queried(query_output) => *query_output,
        };
        let target = {
            let known_target =
                self.known_targets.read().get(&prev_blockhash).copied();
            if let Some(target) = known_target {
                target
            } else {
                // We used to calculate the next block's target here. This didn't
                // work with signets with custom block times. Instead we always
                // read directly from Core. This only happens 1 time per chain tip,
                // so the performance impact is negligible.
                let mining_info = self
                    .rpc_client
                    .get_mining_info()
                    .await
                    .map_err(internal_error)?;
                let target = mining_info.next.target;
                self.known_targets.write().insert(prev_blockhash, target);
                target
            }
        };
        let coinbase_txn_or_value = if let Some(coinbase_txn) = coinbase_txn {
            let fee = coinbase_txn
                .output
                .iter()
                .map(|txout| txout.value)
                .checked_sum()
                .ok_or_else(|| {
                    let err = anyhow::anyhow!(
                        "Value overflow error in coinbase output"
                    );
                    let err = log_error(err);
                    internal_error(err)
                })?;
            let txn = BlockTemplateTransaction {
                txid: coinbase_txn.compute_txid(),
                hash: coinbase_txn.compute_wtxid(),
                depends: Vec::new(),
                fee: -bitcoin::SignedAmount::try_from(fee).unwrap(),
                sigops: None,
                weight: coinbase_txn.weight().to_wu(),
                data: bitcoin::consensus::serialize(&coinbase_txn),
            };
            CoinbaseTxnOrValue::Txn(txn)
        } else {
            coinbase_txn_or_value.clone()
        };
        let mintime =
            // TODO: calculate this correctly
            /*
            std::cmp::max(
                tip_block_mediantime as u64 + 1,
                current_time_adjusted,
            )
            */
            tip_block_mediantime as u64 + 1;
        let height = tip_block_height + 1;
        let res = BlockTemplate {
            capabilities: SERVER_CAPABILITIES
                .iter()
                .map(|capability| (*capability).to_owned())
                .collect(),
            version,
            rules: rules.clone(),
            version_bits_available: version_bits_available.clone(),
            version_bits_required,
            prev_blockhash,
            transactions: block_txs,
            coinbase_aux: coinbase_aux.clone(),
            coinbase_txn_or_value,
            long_poll_id: Some(format!(
                "{prev_blockhash}{:016x}{wake_gen:016x}",
                self.long_poll_seq.fetch_add(1, Ordering::Relaxed)
            )),
            target: target.to_be_bytes(),
            mintime,
            mutable: mutable.clone(),
            nonce_range: NONCE_RANGE,
            sigop_limit,
            size_limit,
            weight_limit,
            current_time: current_time_adjusted,
            compact_target: target.to_compact_lossy(),
            height,
            default_witness_commitment,
            signet_challenge: signet_challenge.clone(),
        };
        if let Some(cached_block_templates) = &self.cached_block_templates {
            let () = cached_block_templates
                .write()
                .put(Box::new(res.clone()), now);
        }
        Ok(res)
    }
}

#[async_trait]
impl<BP, RpcClient> RpcServer for Server<BP, RpcClient>
where
    BP: CusfBlockProducer + Send + Sync + 'static,
    RpcClient: bitcoin_jsonrpsee::client::MainClient + Send + Sync + 'static,
{
    async fn get_block_template(
        &self,
        request: BlockTemplateRequest,
    ) -> RpcResult<BlockTemplateResponse> {
        // A `mode` that is not a string is the same error as an unrecognised
        // one, as in Core.
        // https://github.com/bitcoin/bitcoin/blob/6c4fe401e908cff1b67d80035b117aae15fe7db6/src/rpc/mining.cpp#L726
        let Ok(mode) = request.mode() else {
            return Err(jsonrpsee::types::ErrorObjectOwned::owned(
                RPC_INVALID_PARAMETER,
                "Invalid mode",
                None::<()>,
            ));
        };
        match mode {
            MODE_TEMPLATE => {
                self.build_block_template(request).await.map(|template| {
                    BlockTemplateResponse::Template(Box::new(template))
                })
            }
            MODE_PROPOSAL => self
                .validate_proposal(request)
                .await
                .map(BlockTemplateResponse::Proposal),
            // https://github.com/bitcoin/bitcoin/blob/6c4fe401e908cff1b67d80035b117aae15fe7db6/src/rpc/mining.cpp#L763
            _ => Err(jsonrpsee::types::ErrorObjectOwned::owned(
                RPC_INVALID_PARAMETER,
                "Invalid mode",
                None::<()>,
            )),
        }
    }

    async fn submit_block(
        &self,
        block_hex: String,
    ) -> RpcResult<Option<String>> {
        self.rpc_client
            .submit_block(block_hex)
            .await
            .map_err(|err| match err {
                jsonrpsee::core::ClientError::Call(err) => err,
                err => internal_error(err),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod long_poll_wake {
        use std::time::Instant;

        use bitcoin::hashes::Hash as _;

        use super::*;

        const TEST_TIMEOUT: Duration = Duration::from_secs(5);

        fn tip(byte: u8) -> BlockHash {
            BlockHash::from_byte_array([byte; 32])
        }

        /// Park a long-poll on tip 1 built at wake generation `parked_gen`,
        /// run `act` after 200 ms, and return how long the poll was parked.
        async fn parked_for(
            template_wake: &TemplateWake,
            parked_gen: Option<u64>,
            act: impl FnOnce(),
        ) -> Duration {
            let (_tip_tx, tip_rx) = tokio::sync::watch::channel(tip(1));
            let start = Instant::now();
            let wait = tokio::spawn(long_poll_wait(
                tip_rx,
                Some(template_wake.subscribe()),
                tip(1),
                parked_gen,
                TEST_TIMEOUT,
            ));
            tokio::time::sleep(Duration::from_millis(200)).await;
            act();
            wait.await.unwrap();
            start.elapsed()
        }

        #[tokio::test]
        async fn wake_returns_parked_long_poll() {
            let template_wake = TemplateWake::new();
            let parked =
                parked_for(&template_wake, Some(0), || template_wake.wake())
                    .await;
            assert!(parked < Duration::from_secs(1), "parked {parked:?}");
        }

        #[tokio::test]
        async fn no_wake_parks_until_timeout() {
            let template_wake = TemplateWake::new();
            let parked = parked_for(&template_wake, Some(0), || ()).await;
            assert!(parked >= TEST_TIMEOUT, "parked {parked:?}");
        }

        /// A wake between building the template and parking its long-poll
        /// is not lost: the poll returns at once.
        #[tokio::test]
        async fn wake_before_parking_returns_at_once() {
            let template_wake = TemplateWake::new();
            template_wake.wake();
            let parked = parked_for(&template_wake, Some(0), || ()).await;
            assert!(parked < Duration::from_secs(1), "parked {parked:?}");
        }

        /// A poll parked at the newest generation is not woken by older
        /// wakes (no spin after a burst).
        #[tokio::test]
        async fn current_gen_parks() {
            let template_wake = TemplateWake::new();
            for _ in 0..5 {
                template_wake.wake();
            }
            // One bump at once, the other four folded into one deferred bump.
            tokio::time::sleep(MIN_WAKE_INTERVAL + Duration::from_millis(200))
                .await;
            assert_eq!(*template_wake.subscribe().borrow(), 2);
            let parked = parked_for(&template_wake, Some(2), || ()).await;
            assert!(parked >= TEST_TIMEOUT, "parked {parked:?}");
        }

        #[tokio::test]
        async fn tip_change_still_returns() {
            let template_wake = TemplateWake::new();
            let (tip_tx, tip_rx) = tokio::sync::watch::channel(tip(1));
            let start = Instant::now();
            let wait = tokio::spawn(long_poll_wait(
                tip_rx,
                Some(template_wake.subscribe()),
                tip(1),
                Some(0),
                TEST_TIMEOUT,
            ));
            tokio::time::sleep(Duration::from_millis(200)).await;
            tip_tx.send(tip(2)).unwrap();
            wait.await.unwrap();
            assert!(start.elapsed() < Duration::from_secs(1));
        }

        /// A burst of wakes bumps the generation once at once and once more
        /// at the end of `MIN_WAKE_INTERVAL`, so a long-poll parked on the
        /// first bump is held for the rest of the interval.
        #[tokio::test]
        async fn wake_burst_is_coalesced() {
            let template_wake = TemplateWake::new();
            let wake_rx = template_wake.subscribe();
            let burst_start = Instant::now();
            for _ in 0..10 {
                template_wake.wake();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(*wake_rx.borrow(), 1, "first wake is immediate");
            let (_tip_tx, tip_rx) = tokio::sync::watch::channel(tip(1));
            long_poll_wait(
                tip_rx,
                Some(template_wake.subscribe()),
                tip(1),
                Some(1),
                TEST_TIMEOUT,
            )
            .await;
            let held = burst_start.elapsed();
            assert!(
                held >= Duration::from_millis(900)
                    && held < Duration::from_millis(1500),
                "wake-driven return after {held:?}"
            );
            assert_eq!(*wake_rx.borrow(), 2, "the rest fold into one bump");
            tokio::time::sleep(Duration::from_millis(1200)).await;
            assert_eq!(*wake_rx.borrow(), 2, "no further bumps");
        }

        /// A tip change returns at once, even while wakes are held back.
        #[tokio::test]
        async fn tip_change_returns_during_wake_hold() {
            let template_wake = TemplateWake::new();
            template_wake.wake();
            template_wake.wake(); // deferred to the end of the interval
            let (tip_tx, tip_rx) = tokio::sync::watch::channel(tip(1));
            let start = Instant::now();
            let wait = tokio::spawn(long_poll_wait(
                tip_rx,
                Some(template_wake.subscribe()),
                tip(1),
                Some(1),
                TEST_TIMEOUT,
            ));
            tokio::time::sleep(Duration::from_millis(100)).await;
            tip_tx.send(tip(2)).unwrap();
            wait.await.unwrap();
            assert!(
                start.elapsed() < Duration::from_millis(500),
                "parked {:?}",
                start.elapsed()
            );
        }

        #[test]
        fn long_poll_id_round_trip() {
            let id = format!("{}{:016x}{:016x}", tip(7), 3u64, 42u64);
            assert_eq!(parse_long_poll_tip(&id), Some(tip(7)));
            assert_eq!(parse_long_poll_wake_gen(&id), Some(42));
            let old_id = format!("{}{:016x}", tip(7), 3u64);
            assert_eq!(parse_long_poll_tip(&old_id), Some(tip(7)));
            assert_eq!(parse_long_poll_wake_gen(&old_id), None);
        }
    }

    /// A block consists of the header, the txs array length, the coinbase tx,
    /// and the txs selected within `MAX_USABLE_BLOCK_WEIGHT`, so the weight
    /// reserved for the coinbase tx (in `MAX_USABLE_BLOCK_WEIGHT`) and for its
    /// txouts (`coinbase_txouts_weight` in `block_txs`) must cover the
    /// coinbase tx that is actually generated, for any payout spk.
    #[test]
    fn coinbase_weight_is_reserved() {
        // Only the spk length affects the coinbase weight.
        // P2WPKH, P2SH, P2PKH, and P2TR/P2WSH payout spk lengths
        for spk_len in [22, 23, 25, 34] {
            let coinbase_spk = ScriptBuf::from_bytes(vec![0u8; spk_len]);
            let (coinbase_tx, _witness_commitment_spk) = finalize_coinbase_tx(
                coinbase_spk,
                800_000,
                Network::Bitcoin,
                Vec::new(),
                &[],
            )
            .unwrap();
            // Weight of the coinbase txouts, which `block_txs` charges
            // against `MAX_USABLE_BLOCK_WEIGHT`
            let coinbase_txouts_weight: Weight = coinbase_tx
                .output
                .iter()
                .map(|tx_out| tx_out.weight())
                .sum();
            let header_weight = Weight::from_non_witness_data_size(
                bitcoin::block::Header::SIZE as u64,
            );
            // 3 bytes for encoding txs array length
            let txs_len_weight = Weight::from_non_witness_data_size(3);
            let block_weight = header_weight
                + txs_len_weight
                + coinbase_tx.weight()
                + (mempool::MAX_USABLE_BLOCK_WEIGHT - coinbase_txouts_weight);
            assert!(
                block_weight <= Weight::MAX_BLOCK,
                "weight `{block_weight}` exceeds limit for {spk_len}-byte spk"
            );
        }
    }

    /// `Server::build_block_template` with wakes, over a mempool with no
    /// sync task and no node behind the RPC client.
    mod server_wake {
        use std::{
            collections::HashSet, sync::atomic::AtomicBool, time::Instant,
        };

        use bitcoin::{
            Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Txid,
            Witness, absolute::LockTime, hashes::Hash as _,
            transaction::Version,
        };
        use bitcoin_jsonrpsee::client::Block;

        use super::*;
        use crate::{
            cusf_block_producer::{CoinbaseTxn, typewit},
            cusf_enforcer::{
                ConnectBlockAction, CusfEnforcer, DefaultEnforcer,
                DisconnectBlockAction, SyncToTipError, TxAcceptAction,
            },
        };

        /// `DefaultEnforcer` that, if armed, wakes once while a template is
        /// being built: a wake landing after the mempool was queried.
        struct WakeDuringQuery(Option<(TemplateWake, AtomicBool)>);

        impl CusfEnforcer for WakeDuringQuery {
            type InvalidBlockReason = std::convert::Infallible;
            type SyncError = std::convert::Infallible;

            async fn sync_to_tip<Signal: Future<Output = ()> + Send>(
                &mut self,
                _shutdown_signal: Signal,
                _block_hash: BlockHash,
            ) -> Result<
                (),
                SyncToTipError<Self::InvalidBlockReason, Self::SyncError>,
            > {
                Ok(())
            }

            type ConnectBlockError = std::convert::Infallible;

            async fn connect_block(
                &mut self,
                _block: &bitcoin::Block,
            ) -> Result<ConnectBlockAction, Self::ConnectBlockError>
            {
                Ok(ConnectBlockAction::default())
            }

            type DisconnectBlockError = std::convert::Infallible;

            async fn disconnect_block(
                &mut self,
                _block_hash: BlockHash,
            ) -> Result<DisconnectBlockAction, Self::DisconnectBlockError>
            {
                Ok(DisconnectBlockAction::default())
            }

            type AcceptTxError = std::convert::Infallible;

            fn accept_tx(
                &mut self,
                tx: &Transaction,
            ) -> Result<TxAcceptAction, Self::AcceptTxError> {
                DefaultEnforcer.accept_tx(tx)
            }

            type ValidateBlockError = std::convert::Infallible;

            fn validate_block(
                &self,
                _block: &bitcoin::Block,
            ) -> Result<Option<String>, Self::ValidateBlockError> {
                Ok(None)
            }
        }

        impl CusfBlockProducer for WakeDuringQuery {
            type InitialBlockTemplateError = std::convert::Infallible;

            async fn initial_block_template<const COINBASE_TXN: bool>(
                &self,
                _parent_block_hash: &BlockHash,
                _coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
                _template: &mut InitialBlockTemplate<COINBASE_TXN>,
            ) -> Result<(), Self::InitialBlockTemplateError>
            where
                typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
            {
                Ok(())
            }

            type FinalizeBlockTemplateError = std::convert::Infallible;

            async fn finalize_block_template<const COINBASE_TXN: bool>(
                &self,
                _parent_block_hash: &BlockHash,
                _coinbase_txn_wit: typewit::const_marker::BoolWit<COINBASE_TXN>,
                _template: &mut FilledBlockTemplate<COINBASE_TXN>,
            ) -> Result<(), Self::FinalizeBlockTemplateError>
            where
                typewit::const_marker::Bool<COINBASE_TXN>: CoinbaseTxn,
            {
                if let Some((template_wake, fired)) = &self.0
                    && !fired.swap(true, Ordering::SeqCst)
                {
                    template_wake.wake();
                }
                Ok(())
            }
        }

        type TestServer =
            Server<WakeDuringQuery, bitcoin_jsonrpsee::http_client::HttpClient>;

        fn tip_block() -> Block<true> {
            Block {
                hash: BlockHash::from_byte_array([9; 32]),
                confirmations: 1,
                strippedsize: 0,
                size: 0,
                weight: 0,
                height: 100,
                version: bitcoin::block::Version::TWO,
                version_hex: String::new(),
                merkleroot: bitcoin::TxMerkleNode::all_zeros(),
                tx: Vec::new(),
                time: 0,
                mediantime: 0,
                nonce: 0,
                compact_target: bitcoin::CompactTarget::from_consensus(
                    0x207f_ffff,
                ),
                difficulty: 1.0,
                chainwork: String::new(),
                previousblockhash: None,
                nextblockhash: None,
            }
        }

        fn sample_block_template() -> BlockTemplate {
            BlockTemplate {
                capabilities: Vec::new(),
                version: bitcoin::block::Version::TWO,
                rules: vec!["segwit".to_owned()],
                version_bits_available: Default::default(),
                version_bits_required: bitcoin::block::Version::from_consensus(
                    0,
                ),
                prev_blockhash: tip_block().hash,
                transactions: Vec::new(),
                coinbase_aux: Default::default(),
                coinbase_txn_or_value: CoinbaseTxnOrValue::ValueSats(0),
                long_poll_id: None,
                target: [0xff; 32],
                mintime: 0,
                mutable: Vec::new(),
                nonce_range: [0; 8],
                sigop_limit: 80_000,
                size_limit: 4_000_000,
                weight_limit: Weight::MAX_BLOCK,
                current_time: 0,
                compact_target: bitcoin::CompactTarget::from_consensus(
                    0x207f_ffff,
                ),
                height: 101,
                signet_challenge: None,
                default_witness_commitment: None,
            }
        }

        /// A cached server (templates live for 60 s) with `template_wake`.
        fn server(
            enforcer: WakeDuringQuery,
            template_wake: &TemplateWake,
        ) -> TestServer {
            let mempool = MempoolSync::for_test(
                enforcer,
                Mempool::with_tip_for_test(tip_block()),
            );
            // Never called: the target is known below.
            let rpc_client =
                bitcoin_jsonrpsee::http_client::HttpClientBuilder::default()
                    .build("http://127.0.0.1:1")
                    .unwrap();
            let server = Server::new(
                ScriptBuf::new(),
                mempool,
                Network::Regtest,
                NetworkInfo { time_offset_s: 0 },
                rpc_client,
                Some(Duration::from_secs(60)),
                sample_block_template(),
            )
            .unwrap()
            .with_template_wake(template_wake);
            server.known_targets.write().insert(
                tip_block().hash,
                bitcoin::Target::from_be_bytes([0xff; 32]),
            );
            server
        }

        fn request(long_poll_id: Option<String>) -> BlockTemplateRequest {
            BlockTemplateRequest {
                mode: None,
                data: None,
                rules: vec!["segwit".to_owned()],
                capabilities: HashSet::new(),
                long_poll_id,
            }
        }

        fn bid_tx() -> Transaction {
            Transaction {
                version: Version::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::new(Txid::all_zeros(), 0),
                    sequence: Sequence::MAX,
                    script_sig: ScriptBuf::new(),
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(1000),
                    script_pubkey: ScriptBuf::new_op_return([1u8; 4]),
                }],
            }
        }

        fn wake_gen_of(template: &BlockTemplate) -> Option<u64> {
            template
                .long_poll_id
                .as_deref()
                .and_then(parse_long_poll_wake_gen)
        }

        fn contains(template: &BlockTemplate, txid: Txid) -> bool {
            template.transactions.iter().any(|tx| tx.txid == txid)
        }

        /// A long-poll parked on a cached template returns soon after a wake
        /// whose tx is inserted under the same write lock, with the tx and
        /// the new generation (not the cached template from before).
        #[tokio::test]
        async fn woken_long_poll_serves_the_new_tx() {
            let template_wake = TemplateWake::new();
            let server = std::sync::Arc::new(server(
                WakeDuringQuery(None),
                &template_wake,
            ));
            let first =
                server.build_block_template(request(None)).await.unwrap();
            assert_eq!(wake_gen_of(&first), Some(0));
            let start = Instant::now();
            let poll = tokio::spawn({
                let server = server.clone();
                async move {
                    server
                        .build_block_template(request(first.long_poll_id))
                        .await
                }
            });
            tokio::time::sleep(Duration::from_millis(200)).await;
            let tx = bid_tx();
            let txid = tx.compute_txid();
            server
                .mempool
                .with_mut_for_test(|mempool| {
                    template_wake.wake();
                    let weight = tx.weight();
                    mempool
                        .insert(
                            tx,
                            Amount::from_sat(1000),
                            Default::default(),
                            weight,
                        )
                        .unwrap();
                })
                .await
                .unwrap();
            let woken = tokio::time::timeout(Duration::from_secs(3), poll)
                .await
                .expect("long-poll not woken")
                .unwrap()
                .unwrap();
            assert!(start.elapsed() < Duration::from_secs(1));
            assert!(contains(&woken, txid), "woken template lacks the tx");
            assert_eq!(wake_gen_of(&woken), Some(1));
        }

        /// A wake landing after the mempool was queried must not be claimed
        /// by the template being built (it lacks the tx the wake is for): the
        /// template keeps the old generation, so the next long-poll returns
        /// at once with a fresh one.
        #[tokio::test]
        async fn wake_after_query_is_not_claimed() {
            let template_wake = TemplateWake::new();
            let server = server(
                WakeDuringQuery(Some((
                    template_wake.clone(),
                    AtomicBool::new(false),
                ))),
                &template_wake,
            );
            let first =
                server.build_block_template(request(None)).await.unwrap();
            assert_eq!(*template_wake.subscribe().borrow(), 1, "hook fired");
            assert_eq!(wake_gen_of(&first), Some(0));
            let start = Instant::now();
            let next = server
                .build_block_template(request(first.long_poll_id))
                .await
                .unwrap();
            assert!(start.elapsed() < Duration::from_secs(1));
            assert_eq!(wake_gen_of(&next), Some(1));
        }
    }
}
