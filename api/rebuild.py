"""Rebuild orchestration — the "Rebuild state" mechanism.

Implements the schema design's procedure: it actually cancels and resubmits
the real Flink fold job (stage 2) rather than faking a Python-side replay. The
fold is rebuilt into a shadow table and only swapped into the live page_state on
success, so a failed rebuild never leaves a broken leaderboard on screen.

Submission mechanism (a deliberate implementation choice): the API shells
into the JobManager container via the Docker SDK to run the same `flink run -py`
command used to submit stage 2 at startup — the proven path, no fragile
REST-multipart python upload. Cancel + progress metrics use the Flink REST API.
"""
import asyncio
import os
import re
import time

import httpx

FLINK_REST = os.environ.get("FLINK_REST_URL", "http://flink-jobmanager:8081")
FLINK_CONTAINER = os.environ.get("FLINK_CONTAINER", "pageledger-flink-jobmanager")
FLINK_JM_ADDRESS = os.environ.get("FLINK_JM_ADDRESS", "flink-jobmanager:8081")
STAGE2_FILE = os.environ.get("STAGE2_JOB_FILE", "/opt/flink/usrlib/stage2_fold.py")
STAGE2_PREFIX = "pageledger-stage2-fold"
STEADY_GROUP = "flink-stage2-fold"
COOLDOWN_SECONDS = 30
POLL_TIMEOUT_SECONDS = 300   # hard cap on a single rebuild's replay
POLL_STALL_SECONDS = 90      # no numRecordsIn progress for this long -> fail

DISCREPANCY_SQL = """
select count(*) from (
  select wiki, page_title from (select * from page_state except select * from page_state_shadow) a
  union
  select wiki, page_title from (select * from page_state_shadow except select * from page_state) b
) d
"""


TERMINAL_STATES = {"CANCELED", "FINISHED", "FAILED"}


# --- Flink REST helpers -----------------------------------------------------
async def _active_stage2_jids(client: httpx.AsyncClient):
    """Any non-terminal stage-2 job (RUNNING, RESTARTING, INITIALIZING, …), so a
    job wedged in a restart loop is still cancelled rather than left as a duplicate."""
    r = await client.get(f"{FLINK_REST}/jobs/overview")
    r.raise_for_status()
    return [
        j["jid"] for j in r.json().get("jobs", [])
        if j["state"] not in TERMINAL_STATES and j["name"].startswith(STAGE2_PREFIX)
    ]


async def _cancel_job(client: httpx.AsyncClient, jid: str):
    # Flink 1.20: PATCH /jobs/:jid?mode=cancel
    await client.patch(f"{FLINK_REST}/jobs/{jid}", params={"mode": "cancel"})


async def _wait_job_stopped(client: httpx.AsyncClient, jid: str, timeout: float = 30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = await client.get(f"{FLINK_REST}/jobs/{jid}")
        if r.status_code != 200:
            return True
        if r.json().get("state") in ("CANCELED", "FINISHED", "FAILED"):
            return True
        await asyncio.sleep(0.5)
    return False


async def _fold_num_records_in(client: httpx.AsyncClient, jid: str):
    """numRecordsIn of the KEYED PROCESS (fold) vertex = valid events consumed."""
    r = await client.get(f"{FLINK_REST}/jobs/{jid}")
    if r.status_code != 200:
        return None
    verts = r.json().get("vertices", [])
    vid = next((v["id"] for v in verts if "PROCESS" in v["name"].upper()), None)
    if vid is None and verts:
        vid = verts[-1]["id"]
    if vid is None:
        return None
    r2 = await client.get(
        f"{FLINK_REST}/jobs/{jid}/vertices/{vid}/subtasks/metrics",
        params={"get": "numRecordsIn", "agg": "sum"},
    )
    if r2.status_code != 200:
        return None
    for m in r2.json():
        if m["id"] == "numRecordsIn":
            return int(m.get("sum") or 0)
    return 0


# --- job submission via Docker exec (sync; run in a thread) ------------------
def _submit_stage2_sync(sink_table: str, group_id: str) -> str:
    import docker  # imported lazily so the module loads even without the SDK

    dcli = docker.from_env()
    container = dcli.containers.get(FLINK_CONTAINER)
    cmd = [
        "flink", "run", "-d", "-m", FLINK_JM_ADDRESS,
        "-py", STAGE2_FILE,
        "--sink-table", sink_table,
        "--group-id", group_id,
    ]
    res = container.exec_run(cmd)
    out = res.output.decode("utf-8", "replace")
    if res.exit_code != 0:
        raise RuntimeError(f"flink run failed (exit {res.exit_code}): ...{out[-400:]}")
    m = re.search(r"JobID\s+([0-9a-f]{32})", out)
    if not m:
        raise RuntimeError(f"could not parse JobID from output: ...{out[-400:]}")
    return m.group(1)


async def _submit_stage2(sink_table: str, group_id: str) -> str:
    return await asyncio.to_thread(_submit_stage2_sync, sink_table, group_id)


# --- guards -----------------------------------------------------------------
async def can_start(pool) -> tuple[bool, str]:
    if await pool.fetchval("select 1 from rebuild_log where status = 'running' limit 1"):
        return False, "a rebuild is already in progress"
    if await pool.fetchval(
        "select 1 from rebuild_log where completed_at > now() - interval '30 seconds' limit 1"
    ):
        return False, "a rebuild just finished — try again in a few seconds"
    return True, ""


async def start(pool) -> int:
    """Insert a running rebuild_log row and return its id (call under the lock)."""
    return await pool.fetchval("insert into rebuild_log (status) values ('running') returning id")


# --- the procedure ----------------------------------------------------------
async def run_rebuild(app, rebuild_id: int):
    pool = app.state.pool
    st = app.state.rebuild
    st.clear()
    st.update(rebuild_id=rebuild_id, total=0, processed=0)
    shadow_jid = None

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            # 3. snapshot the denominator for progress
            total = await pool.fetchval("select count(*) from page_events")
            st["total"] = total

            # 4. fresh shadow table
            await pool.execute("drop table if exists page_state_shadow")
            await pool.execute("create table page_state_shadow (like page_state including all)")

            # 5. cancel the steady-state fold job(s)
            for jid in await _active_stage2_jids(client):
                await _cancel_job(client, jid)
                await _wait_job_stopped(client, jid)

            # 6. submit the fold job targeting the shadow, from earliest
            shadow_jid = await _submit_stage2("page_state_shadow", f"flink-stage2-rebuild-{rebuild_id}")

            # 7. poll numRecordsIn until it catches up to the snapshot, with a
            #    stall/timeout guard so a wedged job fails the rebuild instead of
            #    hanging 'running' forever.
            poll_start = time.time()
            best = 0
            last_change = poll_start
            while True:
                processed = await _fold_num_records_in(client, shadow_jid)
                if processed is not None:
                    st["processed"] = processed
                    if processed > best:
                        best, last_change = processed, time.time()
                    if processed >= total:
                        break
                now = time.time()
                if now - poll_start > POLL_TIMEOUT_SECONDS:
                    raise RuntimeError(f"rebuild timed out at {best}/{total} after {int(now - poll_start)}s")
                if now - last_change > POLL_STALL_SECONDS:
                    raise RuntimeError(f"rebuild stalled at {best}/{total} (no progress for {POLL_STALL_SECONDS}s)")
                await asyncio.sleep(1)
            await asyncio.sleep(4)  # let the JDBC sink flush its final batch

            # 8. stop the shadow job BEFORE touching its table, then compute + swap
            await _cancel_job(client, shadow_jid)
            await _wait_job_stopped(client, shadow_jid)
            shadow_jid = None

            discrepancy = await pool.fetchval(DISCREPANCY_SQL)

            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("truncate page_state")
                    await conn.execute("insert into page_state select * from page_state_shadow")
                    await conn.execute("drop table page_state_shadow")

            # 9. resubmit the steady-state job, back on page_state
            await _submit_stage2("page_state", STEADY_GROUP)

            # 10. record success
            await pool.execute(
                "update rebuild_log set status='succeeded', completed_at=now(), "
                "events_replayed=$1, discrepancy_count=$2 where id=$3",
                st["processed"], discrepancy, rebuild_id,
            )
            st.update(discrepancy=discrepancy, events_replayed=st["processed"])
        except Exception as e:  # noqa: BLE001
            # failure path: never touched live page_state on the write side.
            await pool.execute(
                "update rebuild_log set status='failed', completed_at=now(), error_message=$1 where id=$2",
                str(e)[:1000], rebuild_id,
            )
            # best-effort cleanup: stop a dangling shadow job, drop the shadow,
            # and make sure a steady-state job is running again.
            try:
                if shadow_jid:
                    await _cancel_job(client, shadow_jid)
                    await _wait_job_stopped(client, shadow_jid)
                await pool.execute("drop table if exists page_state_shadow")
                if not await _active_stage2_jids(client):
                    await _submit_stage2("page_state", STEADY_GROUP)
            except Exception:  # noqa: BLE001
                pass
            st["error"] = str(e)
        finally:
            st["done"] = True
