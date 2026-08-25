package money

import (
	"errors"
	"testing"
)

func TestParseValidAmounts(t *testing.T) {
	cases := map[string]Amount{
		"0":       0,
		"0.01":    1,
		"1250":    125000,
		"1250.00": 125000,
		"1250.5":  125050,
		"1250.55": 125055,
		" 420.00": 42000,
		"+10.10":  1010,
	}

	for input, expected := range cases {
		got, err := Parse(input)
		if err != nil {
			t.Fatalf("Parse(%q) вернул ошибку: %v", input, err)
		}
		if got != expected {
			t.Fatalf("Parse(%q) = %d, ожидалось %d", input, got, expected)
		}
	}
}

// Именно эти значения проходили через strconv.ParseFloat в прежней реализации.
func TestParseRejectsInvalidAmounts(t *testing.T) {
	inputs := []string{
		"", "abc", "1e10", "Inf", "NaN", "1.005", "1.2.3",
		"1,5", "０.１", "0x10", "1 000", ".", "1.",
	}

	for _, input := range inputs {
		if _, err := Parse(input); err == nil {
			t.Fatalf("Parse(%q) должен был вернуть ошибку", input)
		}
	}
}

func TestParseRejectsNegative(t *testing.T) {
	if _, err := Parse("-1.00"); !errors.Is(err, ErrNegativeAmount) {
		t.Fatalf("err = %v, ожидалось ErrNegativeAmount", err)
	}
}

func TestParsePositiveRejectsZero(t *testing.T) {
	if _, err := ParsePositive("0.00"); !errors.Is(err, ErrNegativeAmount) {
		t.Fatalf("err = %v, ожидалось ErrNegativeAmount", err)
	}
}

func TestStringRoundTrip(t *testing.T) {
	for _, input := range []string{"0.00", "0.05", "10.00", "1250.55", "999999.99"} {
		amount, err := Parse(input)
		if err != nil {
			t.Fatalf("Parse(%q): %v", input, err)
		}
		if got := amount.String(); got != input {
			t.Fatalf("String() = %q, ожидалось %q", got, input)
		}
	}
}

// Проверка, что копеечные суммы складываются без ошибки округления,
// характерной для float64: 0.1 + 0.2 должно давать ровно 0.30.
func TestAddIsExact(t *testing.T) {
	a, _ := Parse("0.10")
	b, _ := Parse("0.20")

	sum, err := a.Add(b)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if sum.String() != "0.30" {
		t.Fatalf("сумма = %s, ожидалось 0.30", sum)
	}
}

func TestMulQuantity(t *testing.T) {
	price, _ := Parse("10.25")

	total, err := price.MulQuantity(2)
	if err != nil {
		t.Fatalf("MulQuantity: %v", err)
	}
	if total.String() != "20.50" {
		t.Fatalf("итог = %s, ожидалось 20.50", total)
	}

	if _, err := price.MulQuantity(0); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("err = %v, ожидалось ErrInvalidAmount", err)
	}
}

func TestOverflowIsDetected(t *testing.T) {
	if _, err := Parse("99999999999999"); !errors.Is(err, ErrAmountOverflow) {
		t.Fatalf("err = %v, ожидалось ErrAmountOverflow", err)
	}

	large, _ := Parse("1000000000")
	if _, err := large.MulQuantity(1000000); !errors.Is(err, ErrAmountOverflow) {
		t.Fatalf("err = %v, ожидалось ErrAmountOverflow", err)
	}
}
