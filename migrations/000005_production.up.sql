create table if not exists production_tasks (
    id uuid primary key,
    order_id uuid not null unique,
    user_id uuid not null,
    status text not null,
    started_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create index if not exists production_tasks_order_id_idx on production_tasks (order_id);
create index if not exists production_tasks_user_id_idx on production_tasks (user_id);
create index if not exists production_tasks_status_idx on production_tasks (status);
create index if not exists production_tasks_created_at_idx on production_tasks (created_at);
