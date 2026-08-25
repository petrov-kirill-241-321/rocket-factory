-- Без позиций резерва невозможно освободить или списать остатки.
-- Добавляем статусы released/committed в жизненный цикл резерва.

create table if not exists reservation_items (
    id uuid primary key,
    reservation_id uuid not null references reservations (id) on delete cascade,
    sku text not null,
    name text not null,
    quantity int not null check (quantity > 0),
    created_at timestamptz not null default now()
);

create index if not exists reservation_items_reservation_id_idx
    on reservation_items (reservation_id);
create unique index if not exists reservation_items_reservation_sku_uq
    on reservation_items (reservation_id, sku);
