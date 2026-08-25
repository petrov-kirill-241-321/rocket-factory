create table if not exists outbox_events (
    id uuid primary key,
    topic text not null,
    event_id uuid not null unique,
    event_type text not null,
    order_id uuid not null,
    user_id uuid not null,
    payload jsonb not null,
    attempts int not null default 0,
    last_error text,
    locked_until timestamptz,
    created_at timestamptz not null,
    published_at timestamptz
);

create index if not exists outbox_events_topic_idx on outbox_events (topic);
create index if not exists outbox_events_event_id_idx on outbox_events (event_id);
create index if not exists outbox_events_event_type_idx on outbox_events (event_type);
create index if not exists outbox_events_order_id_idx on outbox_events (order_id);
create index if not exists outbox_events_created_at_idx on outbox_events (created_at);
create index if not exists outbox_events_unpublished_idx on outbox_events (created_at)
    where published_at is null and attempts < 10;
create index if not exists outbox_events_locked_until_idx on outbox_events (locked_until);
