FROM golang:1.23 AS builder

RUN go env -w GOPROXY=https://package-mirror.liara.ir/repository/go/,https://proxy.golang.org,direct

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod tidy
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o logger-service ./cmd/api

FROM alpine:3.22

WORKDIR /app

COPY --from=builder /app/logger-service .

CMD ["./logger-service"]