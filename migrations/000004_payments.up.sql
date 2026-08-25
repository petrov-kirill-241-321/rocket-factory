create table if not exists payments (
    id uuid primary key,
    order_id uuid not null,
    user_id uuid not null,
    status text not null,
    amount numeric(12,2) not null,
    idempotency_key text,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create unique index if not exists payments_order_user_idempotency_key_uq
    on payments (order_id, user_id, idempotency_key)
    where idempotency_key is not null;
create index if not exists payments_order_id_idx on payments (order_id);
create index if not exists payments_user_id_idx on payments (user_id);
create index if not exists payments_status_idx on payments (status);
create index if not exists payments_created_at_idx on payments (created_at);
