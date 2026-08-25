// Package httpx содержит общие помощники HTTP-транспорта: единый формат ответа
// об ошибке, безопасное чтение тела и ограничения на размер запроса.
package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Машиночитаемые коды ошибок. Клиенту не нужен разбор текста сообщения.
const (
	CodeValidation   = "validation_error"
	CodeUnauthorized = "unauthorized"
	CodeForbidden    = "forbidden"
	CodeNotFound     = "not_found"
	CodeConflict     = "conflict"
	CodeTooLarge     = "payload_too_large"
	CodeUnavailable  = "service_unavailable"
	CodeInternal     = "internal_error"
)

// MaxBodyBytes ограничивает размер тела запроса.
// Без него один запрос может исчерпать память процесса.
const MaxBodyBytes int64 = 1 << 20 // 1 MiB

// ErrorResponse — единый формат ошибки для всех эндпоинтов и всех статусов,
// включая 401 и 500, которые раньше отдавались как text/plain.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func WriteJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func WriteError(w http.ResponseWriter, status int, code, message string) {
	WriteJSON(w, status, ErrorResponse{Error: ErrorBody{Code: code, Message: message}})
}

// ErrBodyTooLarge возвращается, когда тело запроса превысило лимит.
var ErrBodyTooLarge = errors.New("request body is too large")

// DecodeJSON читает тело запроса с ограничением размера и запретом неизвестных
// полей: опечатка в имени поля должна быть видимой ошибкой, а не тихо
// проигнорированным значением.
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if r.Body == nil {
		return io.EOF
	}
	defer func() { _ = r.Body.Close() }()

	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return ErrBodyTooLarge
		}
		return err
	}

	// В теле должен быть ровно один JSON-документ.
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request body must contain a single json object")
	}
	return nil
}

// DecodeOptionalJSON ведёт себя как DecodeJSON, но допускает пустое тело.
func DecodeOptionalJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	err := DecodeJSON(w, r, dst)
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// WriteDecodeError переводит ошибку разбора тела в корректный HTTP-ответ.
func WriteDecodeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrBodyTooLarge):
		WriteError(w, http.StatusRequestEntityTooLarge, CodeTooLarge,
			fmt.Sprintf("request body must not exceed %d bytes", MaxBodyBytes))
	case errors.Is(err, io.EOF):
		WriteError(w, http.StatusBadRequest, CodeValidation, "request body is required")
	default:
		message := err.Error()
		if strings.Contains(message, "unknown field") {
			WriteError(w, http.StatusBadRequest, CodeValidation, message)
			return
		}
		WriteError(w, http.StatusBadRequest, CodeValidation, "request body is not valid json")
	}
}
