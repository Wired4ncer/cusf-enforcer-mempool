//! A node with `-prune` and no `-txindex`, taking new txs throughout the
//! initial sync. Every tx that arrives after `getrawmempool verbose` needs its
//! fee from somewhere else, and `getrawtransaction` cannot supply a confirmed
//! parent here. Reproduces the standby pool's stall of 2026-11-05: the sync
//! never completed.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bitcoin::{Amount, OutPoint, Txid};
use bitcoin_jsonrpsee::{
    MainClient as _,
    client::{
        GetRawTransactionClient as _, GetRawTransactionVerbose,
        RawMempoolTxInfo,
    },
    jsonrpsee::{core::client::ClientT as _, rpc_params},
};
use tokio::time::{sleep, timeout};

use crate::{
    mock_enforcer::MockEnforcer,
    setup::{
        Directories, RegtestNode, start_mempool_sync, wait_for_local_mempool,
    },
    util::{
        BinPaths, RpcClient, get_new_address, send_to_address, spend_output,
    },
};

/// Long enough for several sends to land after the fee snapshot. Far longer
/// than this sync takes with txindex (well under a second).
const SYNC_TIMEOUT: Duration = Duration::from_secs(45);
const SEEDED_TXS: usize = 300;
const RESERVED_COINS: usize = 60;
const SEND_INTERVAL: Duration = Duration::from_millis(5);
const FEE_SAT: u64 = 5_000;

async fn base_fee(rpc: &RpcClient, txid: Txid) -> anyhow::Result<Amount> {
    let info: RawMempoolTxInfo = rpc
        .request("getmempoolentry", rpc_params![txid.to_string()])
        .await?;
    Ok(info.fees.base)
}

pub async fn test_pruned_no_txindex_sync(
    bin_paths: BinPaths,
    directories: Directories,
) -> anyhow::Result<()> {
    let node =
        RegtestNode::new_pruned_no_txindex(&bin_paths, directories).await?;
    let rpc = node.rpc_client.clone();
    // More mature coinbases, so sends spend confirmed coins.
    for _ in 0..10 {
        rpc.generate_to_address(
            10,
            &node.mining_address.clone().into_unchecked(),
        )
        .await?;
    }

    // The premise: this node cannot return a confirmed tx.
    let tip = rpc.getbestblockhash().await?;
    let block: bitcoin_jsonrpsee::client::Block<true> = rpc
        .request("getblock", rpc_params![tip.to_string(), 2])
        .await?;
    let confirmed = block.tx[0].txid;
    let err = rpc
        .get_raw_transaction(confirmed, GetRawTransactionVerbose::<false>, None)
        .await
        .expect_err("precondition: no txindex");
    anyhow::ensure!(
        err.to_string().contains("-5") || err.to_string().contains("txindex"),
        "unexpected error: {err}"
    );

    // Confirmed coins kept back from the wallet's own coin selection: each tx
    // below spends one, so its parent is only in a block.
    let unspent: Vec<serde_json::Value> =
        rpc.request("listunspent", rpc_params![100]).await?;
    let mut reserved = Vec::new();
    for utxo in unspent.iter().take(RESERVED_COINS) {
        let txid: Txid = utxo["txid"].as_str().unwrap_or_default().parse()?;
        let vout = utxo["vout"].as_u64().unwrap_or_default() as u32;
        let sat = (utxo["amount"].as_f64().unwrap_or_default() * 1e8) as u64;
        reserved.push((OutPoint::new(txid, vout), sat));
    }
    anyhow::ensure!(reserved.len() == RESERVED_COINS, "too few coins");
    let locks: Vec<serde_json::Value> = reserved
        .iter()
        .map(|(outpoint, _)| {
            serde_json::json!({
                "txid": outpoint.txid.to_string(),
                "vout": outpoint.vout,
            })
        })
        .collect();
    let locked: bool = rpc
        .request("lockunspent", rpc_params![false, locks])
        .await?;
    anyhow::ensure!(locked, "lockunspent");
    let (late_outpoint, late_sat) =
        reserved.pop().expect("RESERVED_COINS is not zero");

    // A mempool to chew on, so the sync lasts long enough to be overtaken.
    let address = get_new_address(&rpc).await?;
    for _ in 0..SEEDED_TXS {
        send_to_address(&rpc, &address, 100_000).await?;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let sender = {
        let (rpc, stop) = (rpc.clone(), stop.clone());
        tokio::spawn(async move {
            let mut sent = 0u32;
            for (outpoint, sat) in reserved {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                spend_output(&rpc, outpoint, sat, FEE_SAT).await?;
                sent += 1;
                sleep(SEND_INTERVAL).await;
            }
            anyhow::Ok(sent)
        })
    };

    let synced = timeout(
        SYNC_TIMEOUT,
        start_mempool_sync(&node, MockEnforcer::new(), None),
    )
    .await;
    stop.store(true, Ordering::Relaxed);
    let sent = sender.await??;
    let (mempool_sync, task_errors) = synced.map_err(|_| {
        anyhow::anyhow!(
            "initial sync did not complete in {SYNC_TIMEOUT:?} ({sent} txs sent)"
        )
    })??;

    // Everything the node holds reaches us, and at the node's own fee.
    let node_txids: std::collections::HashSet<Txid> =
        rpc.request("getrawmempool", rpc_params![]).await?;
    anyhow::ensure!(!node_txids.is_empty(), "the node holds nothing");
    wait_for_local_mempool(
        &mempool_sync,
        &task_errors,
        Duration::from_secs(20),
        |txids| node_txids.is_subset(txids),
        "every node tx in the enforced mempool",
    )
    .await?;

    // A tx after sync, spending a confirmed coin: this one's fee is the
    // entry's, with no snapshot in play.
    let late = spend_output(&rpc, late_outpoint, late_sat, FEE_SAT).await?;
    wait_for_local_mempool(
        &mempool_sync,
        &task_errors,
        Duration::from_secs(20),
        |txids| txids.contains(&late),
        "a tx sent after sync",
    )
    .await?;

    // Read from the template the pool would mine, not from internals.
    let ours: Vec<(Txid, i64)> = mempool_sync
        .with(|mempool, _| {
            Box::pin(async move {
                mempool
                    .propose_txs(None)
                    .map(|txs| {
                        txs.into_iter()
                            .map(|tx| (tx.txid, tx.fee.to_sat()))
                            .collect()
                    })
                    .unwrap_or_default()
            })
        })
        .await
        .ok_or_else(|| anyhow::anyhow!("mempool unavailable"))?;
    anyhow::ensure!(!ours.is_empty(), "empty template");
    for (txid, fee) in ours {
        let node_fee = base_fee(&rpc, txid).await?;
        anyhow::ensure!(
            fee == node_fee.to_sat() as i64 && fee > 0,
            "{txid}: template {fee} sat, node {node_fee}"
        );
    }
    task_errors.ensure_empty("the end of the test")?;
    Ok(())
}
