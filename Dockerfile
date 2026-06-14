FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /outboxer ./cmd/outboxer

FROM gcr.io/distroless/static-debian12
COPY --from=build /outboxer /outboxer
CMD ["/outboxer"]
