// Command healthcheck — минимальная проверка живости для контейнеров.
//
// Образы на базе distroless не содержат shell, curl и wget, поэтому обычный
// healthcheck вида ["CMD-SHELL", "curl ..."] в них не работает. Отдельный
// статический бинарник решает это без внешних зависимостей.
//
// Использование: healthcheck [-addr host:port] [-path /health] [-timeout 3s]
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "адрес HTTP-сервера сервиса")
	path := flag.String("path", "/health", "путь проверки")
	timeout := flag.Duration("timeout", 3*time.Second, "таймаут запроса")
	flag.Parse()

	client := &http.Client{Timeout: *timeout}

	resp, err := client.Get("http://" + *addr + *path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: unexpected status %d\n", resp.StatusCode)
		os.Exit(1)
	}
}
