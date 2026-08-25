// Package money хранит денежные суммы в минорных единицах (центах) как int64.
// float64 для денег непригоден: 0.1+0.2 != 0.3, а ParseFloat принимает "1e10" и "Inf".
package money

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrInvalidAmount  = errors.New("invalid monetary amount")
	ErrNegativeAmount = errors.New("monetary amount must be positive")
	ErrAmountOverflow = errors.New("monetary amount is too large")
)

// maxCents ограничивает сумму примерно 90 трлн — с запасом покрывает numeric(12,2)
// и гарантирует отсутствие переполнения при умножении на разумное количество позиций.
const maxCents int64 = 1_000_000_000_000

// Amount — сумма в минорных единицах.
type Amount int64

// Parse разбирает десятичную строку вида "1250.00" / "1250" / "1250.5" в центы
// без использования float. Допускается не более двух знаков после запятой.
func Parse(value string) (Amount, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, ErrInvalidAmount
	}
	if strings.HasPrefix(value, "+") {
		value = value[1:]
	}
	if strings.HasPrefix(value, "-") {
		return 0, ErrNegativeAmount
	}

	whole, frac, hasFrac := strings.Cut(value, ".")
	if whole == "" {
		whole = "0"
	}
	if !isDigits(whole) {
		return 0, ErrInvalidAmount
	}
	if hasFrac {
		if len(frac) == 0 || len(frac) > 2 || !isDigits(frac) {
			return 0, ErrInvalidAmount
		}
		if len(frac) == 1 {
			frac += "0"
		}
	} else {
		frac = "00"
	}

	wholePart, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, ErrAmountOverflow
	}
	if wholePart > maxCents/100 {
		return 0, ErrAmountOverflow
	}
	fracPart, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, ErrInvalidAmount
	}

	return Amount(wholePart*100 + fracPart), nil
}

// ParsePositive дополнительно требует, чтобы сумма была строго больше нуля.
func ParsePositive(value string) (Amount, error) {
	amount, err := Parse(value)
	if err != nil {
		return 0, err
	}
	if amount <= 0 {
		return 0, ErrNegativeAmount
	}
	return amount, nil
}

// MulQuantity умножает сумму на количество с проверкой переполнения.
func (a Amount) MulQuantity(quantity int) (Amount, error) {
	if quantity <= 0 {
		return 0, ErrInvalidAmount
	}
	if int64(a) > maxCents/int64(quantity) {
		return 0, ErrAmountOverflow
	}
	return a * Amount(quantity), nil
}

// Add складывает суммы с проверкой переполнения.
func (a Amount) Add(other Amount) (Amount, error) {
	sum := a + other
	if sum < a || int64(sum) > maxCents {
		return 0, ErrAmountOverflow
	}
	return sum, nil
}

// String возвращает каноническое представление с двумя знаками после запятой.
func (a Amount) String() string {
	return fmt.Sprintf("%d.%02d", int64(a)/100, int64(a)%100)
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
