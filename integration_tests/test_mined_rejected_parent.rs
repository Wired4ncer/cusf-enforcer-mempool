//! A tx the enforcer rejected on arrival, and that a block then MINED, must
//! not poison the txs that later spend its outputs.
//!
//! Seen on a betanet node: a BMM bid reached the node seconds before the
//! mainchain block it names, was rejected as expired against the old tip, and
//! was then mined. The bidder funds each bid from the previous bid's change,
//! so every later bid spent the rejected-but-mined tx. `rejected_txs` was only
//! ever inserted into, so parent resolution refused every later bid until
//! restart.

use std::time::Duration;

use bitcoin::{Transaction, Txid, consensus::encode::deserialize_hex};
use bitcoin_jsonrpsee::jsonrpsee::{core::client::ClientT as _, rpc_params};

use crate::{
    setup::TestSetup,
    util::{
        generate_block, prioritised_txids, signed_spend_hex, submit_child_of,
        wait_for_mempool_pred, wallet_outputs_of,
    },
};

const FEE_SAT: u64 = 5_000;

pub async fn test_mined_rejected_parent(
    setup: TestSetup,
) -> anyhow::Result<()> {
    let rpc = &setup.node.rpc_client;

    // P: an ordinary funding tx, admitted.
    let p = setup.submit_and_wait(2_000_000).await?;

    // A: spends P. The enforcer refuses it on arrival.
    let (outpoint, value_sat) = wallet_outputs_of(rpc, p)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("{p} has no wallet output"))?;
    let a_hex = signed_spend_hex(rpc, outpoint, value_sat, FEE_SAT).await?;
    let a: Txid = deserialize_hex::<Transaction>(&a_hex)?.compute_txid();
    setup.enforcer.reject_tx(a);
    let sent: String = rpc
        .request("sendrawtransaction", rpc_params![a_hex])
        .await?;
    anyhow::ensure!(sent.parse::<Txid>()? == a, "txid mismatch for A");

    // Precondition: the node holds A, we do not, and A carries the reject mark.
    wait_for_mempool_pred(
        rpc,
        Duration::from_secs(10),
        |t| t.contains(&a),
        "A in node mempool",
    )
    .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !prioritised_txids(rpc).await?.contains(&a) {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "precondition: A was never deprioritised (not rejected)"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::ensure!(
        !setup.local_mempool_txids().await.contains(&a),
        "precondition: A must be absent from the enforced mempool"
    );

    // A block mines P and A anyway (another miner, or a tip-dependent verdict
    // that no longer holds).
    let block =
        generate_block(rpc, &setup.node.mining_address, &[p, a]).await?;
    setup
        .wait_for_local_tip(block, Duration::from_secs(10))
        .await?;

    // B: spends A's (now confirmed) output. Nothing about B is objectionable.
    let b = submit_child_of(rpc, a, FEE_SAT).await?;
    let admitted = setup
        .wait_for_local_mempool(
            Duration::from_secs(15),
            |t| t.contains(&b),
            "B (child of the mined, once-rejected A) in the enforced mempool",
        )
        .await;
    if let Err(err) = admitted {
        let b_marked = prioritised_txids(rpc).await?.contains(&b);
        anyhow::bail!(
            "B never admitted (B listed by getprioritisedtransactions = \
             {b_marked}): {err:#}"
        );
    }
    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "MempoolSync task surfaced errors: {:?}",
        setup.task_errors.snapshot()
    );
    Ok(())
}
