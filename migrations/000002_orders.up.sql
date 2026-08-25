create table if not exists orders (
    id uuid primary key,
    user_id uuid not null,
    status text not null,
    total_amount numeric(12,2) not null,
    idempotency_key text,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create unique index if not exists orders_user_id_idempotency_key_uq
    on orders (user_id, idempotency_key)
    where idempotency_key is not null;
create index if not exists orders_user_id_idx on orders (user_id);
create index if not exists orders_status_idx on orders (status);
create index if not exists orders_created_at_idx on orders (created_at);

create table if not exists order_items (
    id uuid primary key,
    order_id uuid not null references orders (id) on delete cascade,
    sku text not null,
    name text not null,
    quantity int not null check (quantity > 0),
    unit_price numeric(12,2) not null,
    created_at timestamptz not null
);

create index if not exists order_items_order_id_idx on order_items (order_id);
create index if not exists order_items_created_at_idx on order_items (created_at);

create table if not exists processed_events (
    event_id uuid not null,
    event_type text not null,
    consumer_name text not null,
    processed_at timestamptz not null,
    primary key (event_id, consumer_name)
);

create index if not exists processed_events_event_id_idx on processed_events (event_id);
create index if not exists processed_events_event_type_idx on processed_events (event_type);
create index if not exists processed_events_processed_at_idx on processed_events (processed_at);
