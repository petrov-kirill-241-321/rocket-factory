package httpx

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type sampleRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

func decode(body string, dst any) error {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	return DecodeJSON(httptest.NewRecorder(), req, dst)
}

func TestDecodeJSONAcceptsValidBody(t *testing.T) {
	var req sampleRequest
	if err := decode(`{"sku":"ENGINE-X1","quantity":2}`, &req); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if req.SKU != "ENGINE-X1" || req.Quantity != 2 {
		t.Fatalf("разобрано неверно: %+v", req)
	}
}

// Опечатка в имени поля должна быть видимой ошибкой, а не молча
// проигнорированным значением.
func TestDecodeJSONRejectsUnknownFields(t *testing.T) {
	var req sampleRequest
	err := decode(`{"sku":"ENGINE-X1","quantity":1,"unit_price":"0.01"}`, &req)
	if err == nil {
		t.Fatal("неизвестное поле должно приводить к ошибке")
	}
	if !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("err = %v, ожидалось сообщение о неизвестном поле", err)
	}
}

func TestDecodeJSONRejectsTrailingContent(t *testing.T) {
	var req sampleRequest
	if err := decode(`{"sku":"A","quantity":1}{"sku":"B","quantity":2}`, &req); err == nil {
		t.Fatal("тело с двумя объектами должно отклоняться")
	}
}

// Регресс: без ограничения размера один запрос мог исчерпать память процесса.
func TestDecodeJSONEnforcesBodyLimit(t *testing.T) {
	huge := `{"sku":"` + strings.Repeat("A", int(MaxBodyBytes)+1024) + `","quantity":1}`

	var req sampleRequest
	err := decode(huge, &req)
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("err = %v, ожидалось ErrBodyTooLarge", err)
	}
}

func TestWriteDecodeErrorMapsStatuses(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{"слишком большое тело", `{"sku":"` + strings.Repeat("A", int(MaxBodyBytes)+1024) + `"}`, http.StatusRequestEntityTooLarge, CodeTooLarge},
		{"пустое тело", ``, http.StatusBadRequest, CodeValidation},
		{"битый json", `{нет`, http.StatusBadRequest, CodeValidation},
		{"неизвестное поле", `{"nope":1}`, http.StatusBadRequest, CodeValidation},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))

			var dst sampleRequest
			err := DecodeJSON(recorder, req, &dst)
			if err == nil {
				t.Fatal("ожидалась ошибка разбора")
			}

			out := httptest.NewRecorder()
			WriteDecodeError(out, err)

			if out.Code != tc.wantStatus {
				t.Fatalf("код = %d, ожидался %d", out.Code, tc.wantStatus)
			}

			var body ErrorResponse
			if err := json.Unmarshal(out.Body.Bytes(), &body); err != nil {
				t.Fatalf("тело не является JSON: %v", err)
			}
			if body.Error.Code != tc.wantCode {
				t.Fatalf("код ошибки = %q, ожидался %q", body.Error.Code, tc.wantCode)
			}
		})
	}
}

func TestDecodeOptionalJSONAllowsEmptyBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))

	var dst sampleRequest
	if err := DecodeOptionalJSON(httptest.NewRecorder(), req, &dst); err != nil {
		t.Fatalf("пустое тело должно допускаться: %v", err)
	}
}

func TestWriteErrorFormat(t *testing.T) {
	recorder := httptest.NewRecorder()
	WriteError(recorder, http.StatusConflict, CodeConflict, "order already has an active payment")

	if got := recorder.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := recorder.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q", got)
	}

	var body ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("тело не является JSON: %v", err)
	}
	if body.Error.Code != CodeConflict || body.Error.Message == "" {
		t.Fatalf("тело ошибки = %+v", body.Error)
	}
}
