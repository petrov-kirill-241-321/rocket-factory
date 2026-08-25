-- Защита от двойного списания без Idempotency-Key.
-- На один заказ может существовать только один активный (pending/succeeded) платёж.

delete from payments p
where p.status in ('pending', 'succeeded')
  and exists (
      select 1
      from payments other
      where other.order_id = p.order_id
        and other.status in ('pending', 'succeeded')
        and (other.created_at, other.id) < (p.created_at, p.id)
  );

create unique index if not exists payments_order_id_active_uq
    on payments (order_id)
    where status in ('pending', 'succeeded');
