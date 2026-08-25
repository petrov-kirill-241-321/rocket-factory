create table if not exists users (
    id uuid primary key,
    email text not null unique,
    password_hash text not null,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create index if not exists users_email_idx on users (email);
create index if not exists users_created_at_idx on users (created_at);
