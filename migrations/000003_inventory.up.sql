create table if not exists inventory_items (
    id uuid primary key,
    sku text not null unique,
    name text not null,
    quantity_available int not null check (quantity_available >= 0),
    quantity_reserved int not null check (quantity_reserved >= 0),
    unit_price numeric(12,2) not null,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create index if not exists inventory_items_sku_idx on inventory_items (sku);
create index if not exists inventory_items_created_at_idx on inventory_items (created_at);

create table if not exists reservations (
    id uuid primary key,
    order_id uuid not null,
    user_id uuid not null,
    status text not null,
    reason text,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create unique index if not exists reservations_order_id_uq on reservations (order_id);
create index if not exists reservations_order_id_idx on reservations (order_id);
create index if not exists reservations_user_id_idx on reservations (user_id);
create index if not exists reservations_status_idx on reservations (status);
create index if not exists reservations_created_at_idx on reservations (created_at);

insert into inventory_items (id, sku, name, quantity_available, quantity_reserved, unit_price, created_at, updated_at)
values
    ('00000000-0000-0000-0000-000000000101', 'ENGINE-X1', 'Engine X1', 20, 0, 1250.00, now(), now()),
    ('00000000-0000-0000-0000-000000000102', 'FUEL-TANK', 'Fuel Tank', 30, 0, 420.00, now(), now()),
    ('00000000-0000-0000-0000-000000000103', 'NAV-MODULE', 'Navigation Module', 15, 0, 890.00, now(), now()),
    ('00000000-0000-0000-0000-000000000104', 'HULL-AERO', 'Aero Hull', 10, 0, 2100.00, now(), now())
on conflict (sku) do nothing;
