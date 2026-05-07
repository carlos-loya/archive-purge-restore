FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=1 go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/apr ./cmd/apr \
 && CGO_ENABLED=1 go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/apr-manager ./cmd/manager

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S apr -g 65532 && \
    adduser -S apr -G apr -u 65532

COPY --from=builder /out/apr /apr
COPY --from=builder /out/apr-manager /apr-manager

USER 65532:65532

# Default entrypoint is the CLI; the manager Deployment overrides command
# with /apr-manager.
ENTRYPOINT ["/apr"]
