-- PageLedger read-model schema.
-- Three tables, defined once, run automatically at first Postgres container start
-- (mounted into docker-entrypoint-initdb.d/). No migrations framework by design.
--
-- Nothing in this schema is ever hand-written or manually seeded: page_events and
-- page_state are populated exclusively by the Flink pipeline. There is no seed data
-- for an event-sourced read model — only recorded events to replay.
--
-- Statements are verbatim from the schema design.

-- ---------------------------------------------------------------------------
-- page_events — append-only log of every validated event; the Postgres mirror
-- of the wiki.events.valid Kafka topic. Written forever-forward by Flink stage 1;
-- never touched by a rebuild. Powers the live feed, per-page history, and all
-- four stat-card numbers.
-- ---------------------------------------------------------------------------
create table page_events (
  id bigserial primary key,
  wiki text not null,
  page_title text not null,
  event_type text not null check (event_type in ('PageCreated', 'PageEdited')),
  revision_id bigint not null,
  editor text not null,
  byte_delta integer not null,
  occurred_at timestamptz not null,                 -- the event's own timestamp, from Wikipedia
  ingested_at timestamptz not null default now(),   -- when this pipeline wrote the row
  unique (wiki, revision_id)                         -- idempotent under Flink checkpoint redelivery
);

create index idx_page_events_occurred_at on page_events (occurred_at desc);
create index idx_page_events_ingested_at on page_events (ingested_at desc);
create index idx_page_events_page on page_events (wiki, page_title, occurred_at);
create index idx_page_events_editor on page_events (editor);

-- ---------------------------------------------------------------------------
-- page_state — the folded gold projection, one row per page. Maintained by
-- Flink's keyed state and upserted via the JDBC sink. Natural composite key
-- (wiki, page_title) IS the aggregate's domain identity.
-- ---------------------------------------------------------------------------
create table page_state (
  wiki text not null,
  page_title text not null,
  created_at timestamptz not null,
  last_edited_at timestamptz not null,
  edit_count integer not null default 0,
  net_byte_delta integer not null default 0,
  unique_editors integer not null default 0,
  last_editor text not null,
  is_deleted boolean not null default false,
  primary key (wiki, page_title)
);

create index idx_page_state_last_edited on page_state (last_edited_at desc);

-- ---------------------------------------------------------------------------
-- rebuild_log — audit history of each "Rebuild state" attempt. Persistent, so
-- "last rebuilt N min ago" survives an API restart.
-- ---------------------------------------------------------------------------
create table rebuild_log (
  id bigserial primary key,
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  status text not null default 'running' check (status in ('running', 'succeeded', 'failed')),
  events_replayed integer,
  discrepancy_count integer,
  error_message text
);

create index idx_rebuild_log_started_at on rebuild_log (started_at desc);
