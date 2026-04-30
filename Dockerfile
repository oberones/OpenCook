# syntax=docker/dockerfile:1

ARG GO_IMAGE=golang:1.25-alpine
ARG RUNTIME_IMAGE=alpine:3.22

FROM ${GO_IMAGE} AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG BUILT_AT=unknown
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X github.com/oberones/OpenCook/internal/version.Version=${VERSION} -X github.com/oberones/OpenCook/internal/version.Commit=${COMMIT} -X github.com/oberones/OpenCook/internal/version.BuiltAt=${BUILT_AT}" \
    -o /out/opencook ./cmd/opencook

FROM ${RUNTIME_IMAGE} AS runtime
LABEL org.opencontainers.image.title="OpenCook" \
      org.opencontainers.image.description="Compatibility-first Go rewrite of Chef Infra Server" \
      org.opencontainers.image.source="https://github.com/oberones/OpenCook" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=build /out/opencook /usr/local/bin/opencook
COPY test/functional/fixtures/bootstrap_public.pem /etc/opencook/bootstrap_public.pem

ENV OPENCOOK_SERVICE_NAME=opencook \
    OPENCOOK_ENV=container \
    OPENCOOK_LISTEN_ADDRESS=:4000 \
    OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH=/etc/opencook/bootstrap_public.pem

EXPOSE 4000
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=24 \
    CMD wget -qO- http://127.0.0.1:4000/readyz >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/opencook"]

FROM ${GO_IMAGE} AS functional-tests
RUN apk add --no-cache bash curl ca-certificates postgresql-client

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENTRYPOINT ["bash", "/src/scripts/run-functional-tests-in-container.sh"]
