FROM golang:1.26.2-alpine AS builder

WORKDIR /app

RUN apk add --no-cache gcc musl-dev ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o etl ./cmd/app


FROM alpine:3.23

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/etl /app/etl

RUN mkdir -p /db

CMD ["/app/etl"]
