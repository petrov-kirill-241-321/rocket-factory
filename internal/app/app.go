// Package app содержит общий каркас запуска сервиса: сигналы, HTTP- и
// gRPC-серверы, фоновые задачи и корректное завершение.
//
// Раньше эти ~40 строк были скопированы в каждый из пяти main.go.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

const shutdownTimeout = 15 * time.Second

// Runner управляет жизненным циклом компонентов сервиса.
type Runner struct {
	logger *slog.Logger

	mu       sync.Mutex
	tasks    []task
	closers  []closer
	httpSrv  *http.Server
	grpcSrv  *grpc.Server
	grpcAddr string
}

type task struct {
	name string
	fn   func(context.Context) error
}

type closer struct {
	name string
	fn   func(context.Context) error
}

func NewRunner(logger *slog.Logger) *Runner {
	return &Runner{logger: logger}
}

// HTTP регистрирует HTTP-сервер.
func (r *Runner) HTTP(addr string, handler http.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.httpSrv = &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// GRPC регистрирует gRPC-сервер.
func (r *Runner) GRPC(addr string, server *grpc.Server) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.grpcAddr = addr
	r.grpcSrv = server
}

// Background добавляет фоновую задачу, живущую до отмены контекста.
func (r *Runner) Background(name string, fn func(context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.tasks = append(r.tasks, task{name: name, fn: fn})
}

// OnShutdown регистрирует освобождение ресурса при остановке.
// Замыкания вызываются в порядке, обратном регистрации.
func (r *Runner) OnShutdown(name string, fn func(context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closers = append(r.closers, closer{name: name, fn: fn})
}

// Run блокируется до сигнала завершения или фатальной ошибки компонента.
func (r *Runner) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	errCh := make(chan error, len(r.tasks)+2)
	var wg sync.WaitGroup

	if r.httpSrv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.logger.Info("http server starting", "addr", r.httpSrv.Addr)
			if err := r.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("http server: %w", err)
			}
		}()
	}

	var grpcListener net.Listener
	if r.grpcSrv != nil {
		listener, err := net.Listen("tcp", r.grpcAddr)
		if err != nil {
			return fmt.Errorf("listen grpc %s: %w", r.grpcAddr, err)
		}
		grpcListener = listener

		wg.Add(1)
		go func() {
			defer wg.Done()
			r.logger.Info("grpc server starting", "addr", r.grpcAddr)
			if err := r.grpcSrv.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
				errCh <- fmt.Errorf("grpc server: %w", err)
			}
		}()
	}

	for _, t := range r.tasks {
		wg.Add(1)
		go func(t task) {
			defer wg.Done()
			if err := t.fn(runCtx); err != nil && runCtx.Err() == nil {
				errCh <- fmt.Errorf("%s: %w", t.name, err)
			}
		}(t)
	}

	var runErr error
	select {
	case <-ctx.Done():
		r.logger.Info("shutdown requested")
	case runErr = <-errCh:
		r.logger.Error("component failed, shutting down", "error", runErr)
	}

	cancelRun()
	r.shutdown(&wg)
	return runErr
}

func (r *Runner) shutdown(wg *sync.WaitGroup) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Сначала перестаём принимать новые запросы, затем ждём фоновые задачи,
	// и только потом закрываем соединения с БД, Redis и Kafka.
	if r.httpSrv != nil {
		if err := r.httpSrv.Shutdown(shutdownCtx); err != nil {
			r.logger.Error("shutdown http server", "error", err)
		}
	}
	if r.grpcSrv != nil {
		stopped := make(chan struct{})
		go func() {
			r.grpcSrv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-shutdownCtx.Done():
			r.logger.Warn("grpc graceful stop timed out, forcing stop")
			r.grpcSrv.Stop()
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		r.logger.Warn("background tasks did not finish before timeout")
	}

	for i := len(r.closers) - 1; i >= 0; i-- {
		c := r.closers[i]
		if err := c.fn(shutdownCtx); err != nil {
			r.logger.Error("shutdown component", "component", c.name, "error", err)
		}
	}

	r.logger.Info("shutdown complete")
}
