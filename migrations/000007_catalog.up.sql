-- Каталог принадлежит order-service: это источник истины по цене и названию товара.
-- inventory_items остаётся источником истины по остаткам.
-- Цена больше не принимается от клиента.

create table if not exists catalog_items (
    sku text primary key,
    name text not null,
    unit_price numeric(12,2) not null check (unit_price > 0),
    active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists catalog_items_active_idx on catalog_items (active);

insert into catalog_items (sku, name, unit_price, active)
values
    ('ENGINE-X1', 'Engine X1', 1250.00, true),
    ('FUEL-TANK', 'Fuel Tank', 420.00, true),
    ('NAV-MODULE', 'Navigation Module', 890.00, true),
    ('HULL-AERO', 'Aero Hull', 2100.00, true)
on conflict (sku) do nothing;
