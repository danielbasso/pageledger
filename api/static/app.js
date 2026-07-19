/* PageLedger dashboard — polling, row expansion, theme, connection handling,
   and the rebuild trigger + live progress. */

const POLL_MS = 3000;
const CHEVRON_SVG =
  '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m9 18 6-6-6-6"/></svg>';

const expandedKeys = new Set();
const historyCache = new Map();
let lastLeaderboard = [];
let connected = true;

/* ---------- helpers ---------- */
function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
function fmtNum(n) { return Number(n).toLocaleString("en-US"); }
function deltaClass(n) { return n < 0 ? "delta-neg" : "delta-pos"; }
function fmtDelta(n) {
  const sign = n < 0 ? "−" : "+"; // note: minus glyph for visual clarity
  return sign + Math.abs(Number(n)).toLocaleString("en-US");
}
function deltaSpan(n) { return `<span class="${deltaClass(n)}">${fmtDelta(n)}</span>`; }
function timeAgo(iso) {
  if (!iso) return "";
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return `${Math.floor(s)}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}
function timeOf(iso) {
  const d = new Date(iso);
  return d.toLocaleTimeString("en-GB", { hour12: false });
}
async function getJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`${url} -> ${r.status}`);
  return r.json();
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function icons() { if (window.lucide) lucide.createIcons(); }

/* ---------- theme ---------- */
function applyTheme(t) {
  document.documentElement.setAttribute("data-theme", t);
  const icon = document.getElementById("themeIcon");
  icon.setAttribute("data-lucide", t === "dark" ? "moon" : "sun");
  icons();
}
function toggleTheme() {
  const next = document.documentElement.getAttribute("data-theme") === "light" ? "dark" : "light";
  localStorage.setItem("pl-theme", next);
  applyTheme(next);
}
function initTheme() {
  const saved = localStorage.getItem("pl-theme");
  const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  applyTheme(saved || (prefersDark ? "dark" : "light"));
}

/* ---------- connection banner ---------- */
function setConnected(ok) {
  connected = ok;
  document.getElementById("connBanner").classList.toggle("show", !ok);
}

/* ---------- renderers ---------- */
function renderStats(s) {
  document.getElementById("statEventsMin").textContent = fmtNum(s.events_per_min);
  document.getElementById("statPages").textContent = fmtNum(s.pages_tracked);
  document.getElementById("statEditors").textContent = fmtNum(s.editors_seen);
  document.getElementById("statTotal").textContent = fmtNum(s.total_events);
}

function renderFeed(events) {
  const body = document.getElementById("feedBody");
  if (!events.length) {
    body.innerHTML =
      '<div class="empty-state"><i data-lucide="loader-2" class="spin"></i><span>Waiting for first events…</span></div>';
    icons();
    return;
  }
  body.innerHTML = events.map((e) => {
    const badge = e.event_type === "PageCreated"
      ? '<span class="badge badge-new">NEW</span>'
      : '<span class="badge badge-edit">EDIT</span>';
    return `<div class="feed-row">${badge}` +
      `<span class="feed-title">${esc(e.page_title)}</span>` +
      `<span class="delta ${deltaClass(e.byte_delta)}">${fmtDelta(e.byte_delta)}</span>` +
      `<span class="feed-time">${timeAgo(e.ingested_at)}</span></div>`;
  }).join("");
}

function historyHtml(key) {
  const h = historyCache.get(key);
  if (!h) return '<div class="ev loading">loading…</div>';
  let s = h.events.map((e) =>
    `<div class="ev">${timeOf(e.occurred_at)} · ${esc(e.event_type)} · ${esc(e.editor)} · ${deltaSpan(e.byte_delta)}</div>`
  ).join("");
  const c = h.current;
  if (c) {
    s += `<div class="cur">current — ${fmtNum(c.edit_count)} edits · ${deltaSpan(c.net_byte_delta)} net · last: ${esc(c.last_editor)}</div>`;
  }
  return s;
}

function renderLeaderboard(rows) {
  lastLeaderboard = rows;
  const tb = document.getElementById("leaderboardBody");
  if (!rows.length) {
    tb.innerHTML =
      '<tr><td colspan="5"><div class="empty-state"><i data-lucide="loader-2" class="spin"></i><span>Waiting for first events…</span></div></td></tr>';
    icons();
    return;
  }
  let html = "";
  for (const r of rows) {
    const key = r.wiki + "\x1f" + r.page_title;
    const open = expandedKeys.has(key);
    html +=
      `<tr class="expandable" data-key="${esc(key)}">` +
      `<td><div class="title-cell"><span class="chevron ${open ? "open" : ""}">${CHEVRON_SVG}</span>` +
      `<span class="lead-title">${esc(r.page_title)}</span></div></td>` +
      `<td class="num">${fmtNum(r.edit_count)}</td>` +
      `<td class="num ${deltaClass(r.net_byte_delta)}">${fmtDelta(r.net_byte_delta)}</td>` +
      `<td>${esc(r.last_editor)}</td>` +
      `<td class="mono faint">${timeAgo(r.last_edited_at)}</td></tr>`;
    if (open) {
      html += `<tr class="expand-row"><td colspan="5"><div class="expand-inner">${historyHtml(key)}</div></td></tr>`;
    }
  }
  tb.innerHTML = html;
  tb.querySelectorAll("tr.expandable").forEach((tr) =>
    tr.addEventListener("click", () => onRowClick(tr.dataset.key))
  );
}

/* ---------- row expansion ---------- */
async function onRowClick(key) {
  if (expandedKeys.has(key)) {
    expandedKeys.delete(key);
    renderLeaderboard(lastLeaderboard);
    return;
  }
  expandedKeys.add(key);
  renderLeaderboard(lastLeaderboard); // shows "loading…"
  if (!historyCache.has(key)) {
    const [wiki, title] = key.split("\x1f");
    const url = `/api/pages/${encodeURIComponent(wiki)}/${title.split("/").map(encodeURIComponent).join("/")}/history`;
    try {
      historyCache.set(key, await getJSON(url));
    } catch (e) {
      expandedKeys.delete(key); // let the user retry
    }
    renderLeaderboard(lastLeaderboard);
  }
}

/* ---------- rebuild ---------- */
let rebuildPolling = false;

function applyRebuildStatus(s) {
  const btn = document.getElementById("rebuildBtn");
  const icon = document.getElementById("rebuildIcon");
  const line = document.getElementById("rebuildStatus");
  line.classList.remove("err");
  if (s.status === "running") {
    btn.disabled = true;
    icon.classList.add("spin");
    const total = s.total_known_events || 0;
    const pct = Math.round((s.progress || 0) * 100);
    line.textContent = `replaying ${fmtNum(total)} events… ${pct}%`;
  } else if (s.status === "succeeded") {
    btn.disabled = false;
    icon.classList.remove("spin");
    const d = s.discrepancy_count || 0;
    line.textContent = `last rebuilt ${timeAgo(s.completed_at)} — ${fmtNum(d)} discrepanc${d === 1 ? "y" : "ies"}`;
  } else if (s.status === "failed") {
    btn.disabled = false;
    icon.classList.remove("spin");
    line.textContent = "rebuild failed — showing previous state";
    line.classList.add("err");
  } else {
    btn.disabled = false;
    icon.classList.remove("spin");
    line.textContent = "";
  }
}

async function driveRebuild() {
  if (rebuildPolling) return;
  rebuildPolling = true;
  try {
    while (true) {
      let s;
      try {
        s = await getJSON("/api/rebuild/status");
      } catch (e) {
        await sleep(1000);
        continue; // transient; the main poll shows the connection banner
      }
      applyRebuildStatus(s);
      if (s.status !== "running") break;
      await sleep(1000);
    }
  } finally {
    rebuildPolling = false;
  }
}

async function runRebuild() {
  // optimistic: disable + spin immediately on click
  document.getElementById("rebuildBtn").disabled = true;
  document.getElementById("rebuildIcon").classList.add("spin");
  const line = document.getElementById("rebuildStatus");
  line.classList.remove("err");
  line.textContent = "starting rebuild…";
  try {
    const r = await fetch("/api/rebuild", { method: "POST" });
    if (r.status === 409) {
      const b = await r.json().catch(() => ({}));
      line.textContent = b.detail || "a rebuild is already in progress";
      driveRebuild(); // reflect whatever is actually happening
      return;
    }
    driveRebuild(); // 202 accepted — poll progress to completion
  } catch (e) {
    document.getElementById("rebuildBtn").disabled = false;
    document.getElementById("rebuildIcon").classList.remove("spin");
    line.textContent = "could not start rebuild";
    line.classList.add("err");
  }
}

/* ---------- poll loop ---------- */
async function poll() {
  try {
    const [stats, feed, leaderboard] = await Promise.all([
      getJSON("/api/stats"),
      getJSON("/api/feed?limit=15"),
      getJSON("/api/leaderboard?limit=50"),
    ]);
    // success: update everything and clear any banner
    renderStats(stats);
    renderFeed(feed);
    renderLeaderboard(leaderboard);
    setConnected(true);
    // keep the rebuild status line fresh ("last rebuilt …"); if a rebuild is
    // running (possibly started elsewhere), hand off to the faster poller.
    if (!rebuildPolling) {
      const rs = await getJSON("/api/rebuild/status");
      applyRebuildStatus(rs);
      if (rs.status === "running") driveRebuild();
    }
  } catch (e) {
    // failure: freeze last-known data on screen, show the banner
    setConnected(false);
  }
}

function init() {
  initTheme();
  icons();
  poll();
  setInterval(poll, POLL_MS);
}
document.addEventListener("DOMContentLoaded", init);
