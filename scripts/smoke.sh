#!/usr/bin/env bash
# Сквозная проверка жизненного цикла заказа.
#
# Скрипт проходит весь путь: регистрация, вход, создание заказа, ожидание
# резерва склада, оплата, ожидание завершения производства. На каждом шаге
# проверяется ожидаемый статус, поэтому падение сразу указывает на сломанное звено.
#
# Использование: ./scripts/smoke.sh [base_url]

set -euo pipefail

BASE="${1:-http://localhost:${NGINX_PORT:-8080}}"
EMAIL="pilot-$(date +%s)@example.com"
PASSWORD="password123"

for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 || { echo "нужен $tool" >&2; exit 1; }
done

step() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
fail() { printf '\033[31mОШИБКА: %s\033[0m\n' "$1" >&2; exit 1; }

# wait_status опрашивает заказ, пока он не перейдёт в ожидаемый статус.
wait_status() {
    local order_id="$1" expected="$2" timeout="${3:-30}" status=""
    for _ in $(seq "$timeout"); do
        status=$(curl -fsS "$BASE/api/orders/$order_id" -H "Authorization: Bearer $TOKEN" | jq -r .status)
        [ "$status" = "$expected" ] && { echo "статус: $status"; return 0; }
        sleep 1
    done
    fail "ожидался статус '$expected', получен '$status'"
}

step "Проверка доступности $BASE"
curl -fsS "$BASE/health" >/dev/null || fail "шлюз недоступен"

step "Регистрация пользователя $EMAIL"
curl -fsS "$BASE/api/auth/register" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" | jq -c .

step "Вход"
TOKEN=$(curl -fsS "$BASE/api/auth/login" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" | jq -r .token)
[ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] || fail "токен не получен"

step "Создание заказа (цена берётся из каталога, клиент её не передаёт)"
ORDER=$(curl -fsS "$BASE/api/orders" \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: smoke-$(date +%s)" \
    -d '{"items":[{"sku":"ENGINE-X1","quantity":1},{"sku":"FUEL-TANK","quantity":2}]}')
echo "$ORDER" | jq -c .
ORDER_ID=$(echo "$ORDER" | jq -r .id)
TOTAL=$(echo "$ORDER" | jq -r .total_amount)
[ "$TOTAL" = "2090.00" ] || fail "сумма заказа $TOTAL, ожидалась 2090.00"

step "Ожидание резерва склада"
wait_status "$ORDER_ID" "inventory_reserved" 30

step "Оплата"
curl -fsS "$BASE/api/orders/$ORDER_ID/pay" \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: smoke-pay-$(date +%s)" \
    -d '{"simulate":"success"}' | jq -c .

step "Ожидание завершения производства"
wait_status "$ORDER_ID" "completed" 60

step "Повторная оплата должна быть отклонена"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/orders/$ORDER_ID/pay" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{}')
[ "$CODE" = "409" ] || fail "ожидался 409 при повторной оплате, получен $CODE"
echo "повторная оплата отклонена с кодом $CODE"

step "Неизвестный SKU должен быть отклонён"
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/orders" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"items":[{"sku":"NOT-A-REAL-SKU","quantity":1}]}')
[ "$CODE" = "400" ] || fail "ожидался 400 для неизвестного SKU, получен $CODE"
echo "неизвестный SKU отклонён с кодом $CODE"

step "Запрос без токена должен возвращать JSON с 401"
BODY=$(curl -s "$BASE/api/orders" -H 'Content-Type: application/json' -d '{"items":[]}')
echo "$BODY" | jq -e '.error.code == "unauthorized"' >/dev/null || fail "401 вернулся не в едином формате: $BODY"
echo "формат ошибки корректный"

printf '\n\033[32mСквозной сценарий пройден. Заказ %s завершён.\033[0m\n' "$ORDER_ID"
