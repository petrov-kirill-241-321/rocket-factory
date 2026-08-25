# Сборка выполняется внутри образа: проект собирается из чистого клона одной
# командой docker compose up --build, без предварительного локального go build.
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Слой с зависимостями кешируется отдельно от исходников.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO отключён, символы и пути вырезаны: бинарник компактный и воспроизводимый.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
        -o /out/auth-service ./cmd/auth-service

# В distroless нет shell и curl, поэтому проверку живости выполняет
# собственный статический бинарник.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
        -o /out/healthcheck ./cmd/healthcheck

# distroless вместо scratch: есть ca-certificates и tzdata, процесс
# запускается от непривилегированного пользователя.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/auth-service /auth-service
COPY --from=builder /out/healthcheck /healthcheck

USER nonroot:nonroot
EXPOSE 8080 9090

ENTRYPOINT ["/auth-service"]
