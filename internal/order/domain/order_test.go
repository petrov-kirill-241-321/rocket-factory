package domain

import (
	"errors"
	"testing"
)

// Прямой путь заказа: каждое следующее состояние должно применяться.
func TestShouldApplyAdvancesLifecycle(t *testing.T) {
	lifecycle := []string{
		StatusCreated,
		StatusInventoryReserved,
		StatusPaymentPending,
		StatusPaid,
		StatusProductionStarted,
		StatusCompleted,
	}

	for i := 0; i < len(lifecycle)-1; i++ {
		apply, err := ShouldApply(lifecycle[i], lifecycle[i+1])
		if err != nil {
			t.Fatalf("ShouldApply(%s, %s): %v", lifecycle[i], lifecycle[i+1], err)
		}
		if !apply {
			t.Fatalf("переход %s -> %s должен применяться", lifecycle[i], lifecycle[i+1])
		}
	}
}

// Ключевое свойство ранговой модели: событие, пришедшее раньше времени,
// применяется, а опоздавшее — отбрасывается. Именно это спасает заказ от
// зависания при доставке событий не по порядку.
func TestShouldApplyHandlesOutOfOrderDelivery(t *testing.T) {
	// payment_succeeded пришло раньше inventory_reserved.
	apply, err := ShouldApply(StatusCreated, StatusPaid)
	if err != nil {
		t.Fatalf("ShouldApply: %v", err)
	}
	if !apply {
		t.Fatal("опережающее событие должно применяться, иначе заказ застрянет")
	}

	// Запоздавшее inventory_reserved уже неактуально.
	apply, err = ShouldApply(StatusPaid, StatusInventoryReserved)
	if err != nil {
		t.Fatalf("ShouldApply: %v", err)
	}
	if apply {
		t.Fatal("устаревшее событие не должно откатывать состояние назад")
	}
}

// Повторная доставка того же события не должна ничего менять.
func TestShouldApplyIsIdempotent(t *testing.T) {
	for _, status := range []string{StatusCreated, StatusInventoryReserved, StatusPaid} {
		apply, err := ShouldApply(status, status)
		if err != nil {
			t.Fatalf("ShouldApply(%s, %s): %v", status, status, err)
		}
		if apply {
			t.Fatalf("повтор статуса %s не должен применяться", status)
		}
	}
}

func TestFinalStatusesAreNotOverwritten(t *testing.T) {
	finals := []string{StatusCompleted, StatusFailed, StatusInventoryFailed}
	incoming := []string{StatusPaid, StatusProductionStarted, StatusCompleted, StatusFailed}

	for _, final := range finals {
		if !IsFinal(final) {
			t.Fatalf("%s должен считаться терминальным", final)
		}
		for _, next := range incoming {
			apply, err := ShouldApply(final, next)
			if err != nil {
				t.Fatalf("ShouldApply(%s, %s): %v", final, next, err)
			}
			if apply {
				t.Fatalf("терминальный статус %s не должен меняться на %s", final, next)
			}
		}
	}
}

// Отказ перекрывает любой успешный прогресс.
func TestFailureOverridesProgress(t *testing.T) {
	apply, err := ShouldApply(StatusPaid, StatusFailed)
	if err != nil {
		t.Fatalf("ShouldApply: %v", err)
	}
	if !apply {
		t.Fatal("отказ должен применяться поверх успешного прогресса")
	}
}

func TestUnknownStatusIsRejected(t *testing.T) {
	if _, err := ShouldApply(StatusCreated, "teleported"); !errors.Is(err, ErrUnknownStatus) {
		t.Fatalf("err = %v, ожидалось ErrUnknownStatus", err)
	}
	if _, err := ShouldApply("teleported", StatusPaid); !errors.Is(err, ErrUnknownStatus) {
		t.Fatalf("err = %v, ожидалось ErrUnknownStatus", err)
	}
}

func TestEveryKnownStatusHasRank(t *testing.T) {
	statuses := []string{
		StatusCreated, StatusInventoryReserved, StatusInventoryFailed,
		StatusPaymentPending, StatusPaid, StatusProductionStarted,
		StatusCompleted, StatusFailed,
	}

	for _, status := range statuses {
		if _, ok := Rank(status); !ok {
			t.Fatalf("для статуса %s не задан ранг", status)
		}
	}
}
