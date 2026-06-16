# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.25.8

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /outboxer ./cmd/outboxer

FROM alpine:3.22

LABEL org.opencontainers.image.title="Outboxer" \
      org.opencontainers.image.description="Transactional outbox worker for Google Pub/Sub and AWS SQS" \
      org.opencontainers.image.source="https://github.com/fvdsn/outboxer" \
      org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache ca-certificates && \
    addgroup -S outboxer && \
    adduser -S -D -H -G outboxer outboxer

COPY --from=build /outboxer /outboxer

USER outboxer:outboxer
ENTRYPOINT ["/outboxer"]
