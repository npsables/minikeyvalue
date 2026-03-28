FROM golang:1.23 AS builder
ENV DEBIAN_FRONTEND noninteractive

WORKDIR /opt/build
COPY . .
RUN go mod download -x && \
    CGO_ENABLED=0 go build -o /tmp/mkv src/*.go

FROM golang:1.23-alpine
ENV DEBIAN_FRONTEND noninteractive
WORKDIR /mkv
RUN apk --no-cache add ca-certificates
COPY --from=builder /tmp/mkv .
