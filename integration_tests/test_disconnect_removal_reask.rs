//! Variant of `orphan_admitted_after_disconnect_removal` that judges the
//! OUTCOME rather than one policy: after the enforcer removes a package at
//! block disconnect and a grandchild arrives,
//!   (a) the template must be minable, and
//!   (b) a removed tx may only be back in the template if the enforcer was
//!       asked about it again after the removal (re-judged against the new
//!       tip), never resurrected from a cache.
//! `still_rejects = true`: the enforcer keeps refusing the parent on re-ask,
//! so none of the three may be proposed.

use std::{collections::HashSet, time::Duration};

use bitcoin::Txid;
use bitcoin_jsonrpsee::MainClient as _;

use crate::{
    setup::{TestSetup, local_block_template},
    util::{
        TemplateViolation, generate_block, get_new_address, submit_child_of,
        template_violations, wait_for_mempool_pred,
    },
};

const CHAINED_FEE_SAT: u64 = 5_000;

async fn run(setup: TestSetup, still_rejects: bool) -> anyhow::Result<()> {
    let rpc = &setup.node.rpc_client;

    let parent = setup.submit_and_wait(2_000_000).await?;
    let child = submit_child_of(rpc, parent, CHAINED_FEE_SAT).await?;
    setup
        .wait_for_local_mempool(
            Duration::from_secs(10),
            |t| t.contains(&parent) && t.contains(&child),
            "package in local mempool",
        )
        .await?;

    setup
        .enforcer
        .set_always_remove_on_disconnect(HashSet::from([parent]));
    let block = generate_block(rpc, &setup.node.mining_address, &[]).await?;
    setup
        .wait_for_local_tip(block, Duration::from_secs(10))
        .await?;
    if still_rejects {
        setup.enforcer.reject_tx(parent);
    }
    let asks_before = setup
        .enforcer
        .accept_tx_calls()
        .iter()
        .filter(|t| **t == parent)
        .count();

    rpc.invalidate_block(block).await?;
    // Precondition: the removal happened locally.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let t = local_block_template(&setup.mempool_sync).await?;
        let ids: Vec<Txid> = t.iter().map(|t| t.txid).collect();
        if !ids.contains(&parent) && !ids.contains(&child) {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "precondition: package never removed at disconnect: {ids:?}"
        );
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
    wait_for_mempool_pred(
        rpc,
        Duration::from_secs(10),
        |t| t.contains(&parent) && t.contains(&child),
        "package still in bitcoind mempool",
    )
    .await?;

    let grandchild = submit_child_of(rpc, child, CHAINED_FEE_SAT).await?;
    wait_for_mempool_pred(
        rpc,
        Duration::from_secs(10),
        |t| t.contains(&grandchild),
        "grandchild in bitcoind mempool",
    )
    .await?;
    let barrier_addr = get_new_address(rpc).await?;
    let barrier = generate_block(rpc, &barrier_addr, &[]).await?;
    setup
        .wait_for_local_tip(barrier, Duration::from_secs(10))
        .await?;
    // Give any post-connect reconsideration time to land.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let template = local_block_template(&setup.mempool_sync).await?;
    let txids: Vec<Txid> = template.iter().map(|t| t.txid).collect();
    let asks_after = setup
        .enforcer
        .accept_tx_calls()
        .iter()
        .filter(|t| **t == parent)
        .count();

    let violations = template_violations(rpc, &template).await?;
    anyhow::ensure!(
        violations.is_empty(),
        "template is not minable: {}; template {txids:?}",
        violations
            .iter()
            .map(TemplateViolation::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    );
    let parent_in = txids.contains(&parent);
    if parent_in {
        anyhow::ensure!(
            !still_rejects,
            "parent back in the template although the enforcer still rejects \
             it (asks {asks_before} -> {asks_after})"
        );
        anyhow::ensure!(
            asks_after > asks_before,
            "parent back in the template WITHOUT being re-asked \
             (asks {asks_before} -> {asks_after}) — resurrected from a cache"
        );
    } else {
        anyhow::ensure!(
            !txids.contains(&child) && !txids.contains(&grandchild),
            "descendant proposed without the parent: {txids:?}"
        );
    }
    anyhow::ensure!(
        setup.task_errors.is_empty(),
        "task errors: {:?}",
        setup.task_errors.snapshot()
    );
    eprintln!(
        "REASK-RESULT still_rejects={still_rejects} parent_in={parent_in} \
         child_in={} grandchild_in={} asks {asks_before}->{asks_after} \
         template_len={}",
        txids.contains(&child),
        txids.contains(&grandchild),
        txids.len()
    );
    Ok(())
}

pub async fn test_disconnect_removal_reask_accepts(
    setup: TestSetup,
) -> anyhow::Result<()> {
    run(setup, false).await
}

pub async fn test_disconnect_removal_reask_rejects(
    setup: TestSetup,
) -> anyhow::Result<()> {
    run(setup, true).await
}
