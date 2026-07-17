"""PageLedger API — read-only endpoints over the gold read model.

FastAPI + asyncpg (no ORM: the schema is two small tables). Every endpoint is a
plain query against page_events / page_state, computed on request. The queries
are exactly those in the schema design.

Rebuild endpoints (POST /api/rebuild, GET /api/rebuild/status) are added in step 8.
The static dashboard is mounted in step 7.
"""
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

PG = dict(
    host=os.environ.get("POSTGRES_HOST", "postgres"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    database=os.environ.get("POSTGRES_DB", "pageledger"),
    user=os.environ.get("POSTGRES_USER", "pageledger"),
    password=os.environ.get("POSTGRES_PASSWORD", "pageledger"),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(**PG, min_size=1, max_size=10)
    try:
        yield
    finally:
        await app.state.pool.close()


app = FastAPI(title="PageLedger API", lifespan=lifespan)


@app.get("/api/stats")
async def stats():
    """The four stat-card numbers (schema design's Stat Card Queries)."""
    async with app.state.pool.acquire() as conn:
        events_per_min = await conn.fetchval(
            "select count(*) from page_events where ingested_at > now() - interval '1 minute'"
        )
        pages_tracked = await conn.fetchval("select count(*) from page_state")
        editors_seen = await conn.fetchval("select count(distinct editor) from page_events")
        total_events = await conn.fetchval("select count(*) from page_events")
    return {
        "events_per_min": events_per_min,
        "pages_tracked": pages_tracked,
        "editors_seen": editors_seen,
        "total_events": total_events,
    }


@app.get("/api/feed")
async def feed(limit: int = Query(15, ge=1, le=100)):
    """Most recent events for the live feed, newest first.

    Ordered by ingested_at (real processing time), not occurred_at — in FIXTURE
    mode occurred_at can be old/out-of-order, so ingested_at is the correct basis
    for a live feed and its 'Ns ago' rendering.
    """
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            "select wiki, page_title, event_type, editor, byte_delta, "
            "       occurred_at, ingested_at "
            "from page_events order by ingested_at desc, id desc limit $1",
            limit,
        )
    return [dict(r) for r in rows]


@app.get("/api/leaderboard")
async def leaderboard(limit: int = Query(50, ge=1, le=200)):
    """Pages from page_state, sorted by most recent activity."""
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            "select wiki, page_title, edit_count, net_byte_delta, unique_editors, "
            "       last_editor, created_at, last_edited_at, is_deleted "
            "from page_state order by last_edited_at desc limit $1",
            limit,
        )
    return [dict(r) for r in rows]


@app.get("/api/pages/{wiki}/{page_title:path}/history")
async def page_history(wiki: str, page_title: str):
    """Full chronological page_events history for one page (powers row expansion),
    plus its current derived state so the expansion can show the resolving line.

    page_title uses the :path converter so titles containing '/' (e.g.
    'User:Foo/sandbox') route correctly.
    """
    async with app.state.pool.acquire() as conn:
        events = await conn.fetch(
            "select event_type, editor, byte_delta, revision_id, occurred_at "
            "from page_events where wiki = $1 and page_title = $2 "
            "order by occurred_at asc, id asc",
            wiki,
            page_title,
        )
        current = await conn.fetchrow(
            "select edit_count, net_byte_delta, unique_editors, last_editor, "
            "       created_at, last_edited_at "
            "from page_state where wiki = $1 and page_title = $2",
            wiki,
            page_title,
        )
    return {
        "wiki": wiki,
        "page_title": page_title,
        "events": [dict(e) for e in events],
        "current": dict(current) if current else None,
    }


@app.exception_handler(404)
async def not_found(request: Request, exc):
    """Any non-API URL redirects to / — there's only one page (app-flow spec)."""
    if request.url.path.startswith("/api"):
        return JSONResponse({"detail": "not found"}, status_code=404)
    return RedirectResponse("/")


# Serve the single-page dashboard. Mounted last so /api routes take precedence.
app.mount("/", StaticFiles(directory="static", html=True), name="static")
