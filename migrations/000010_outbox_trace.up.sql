-- Outbox публикует событие в отдельной горутине спустя время после коммита транзакции,
-- поэтому trace context нужно сохранять вместе с событием, иначе трейс рвётся.

alter table outbox_events
    add column if not exists trace_context jsonb;
