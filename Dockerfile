FROM golang:1.25-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -ldflags "-s -w" -o /apr ./cmd/apr

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /apr /usr/local/bin/apr

ENTRYPOINT ["apr"]
