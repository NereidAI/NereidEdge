# Copyright (C) 2026 YIQISOFT
# SPDX-License-Identifier: Apache-2.0

# syntax=docker/dockerfile:1

# Compile on the build host architecture; only the runtime image varies by target.
FROM --platform=$BUILDPLATFORM public.ecr.aws/docker/library/golang:1.27.3-alpine3.24 AS builder

# Regional apk mirror and Go proxy are injected as build args; empty values
# keep the pristine upstream sources. The NereidLink device SDK is a local
# replace (./third_party/nereidlink/sdk/device/go) shipped inside the
# repository, so the image build needs no external credentials or extra
# build contexts.
ARG ALPINE_MIRROR=""
RUN if [ -n "$ALPINE_MIRROR" ]; then \
        sed -i "s|https*://dl-cdn.alpinelinux.org|$ALPINE_MIRROR|g" /etc/apk/repositories; \
    fi \
    && apk add --no-cache git ca-certificates

ARG GOPROXY=""
ENV GOPROXY=${GOPROXY} \
    GOSUMDB=sum.golang.org \
    GOWORK=off

WORKDIR /build

# Reviewed in-repo SDK mirror. Copied before go.mod so module resolution
# caches across application source edits.
COPY third_party/nereidlink/sdk/device/go ./third_party/nereidlink/sdk/device/go

# Resolve public modules first so the layer caches across source edits.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Default is the rolling dev channel; the release workflow injects bare
# SemVer (0.0.1) derived from the v-prefixed git tag. TARGETOS/TARGETARCH are
# BuildKit platform args, so the same Dockerfile builds every architecture
# with Go's native cross-compilation (no QEMU in the build stage).
ARG VERSION=0.0.0-dev
ARG GIT_SHA=dev
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags "-s -w -X main.Version=${VERSION} -X main.GitSHA=${GIT_SHA}" \
    -o nereid-edge ./cmd/nereid-edge \
    && mkdir -p /build/data/logs

# The runtime has no RUN instructions: foreign-architecture images therefore
# need no QEMU shell execution during cross-platform builds.
FROM public.ecr.aws/docker/library/alpine:3.24.4

# ARG scope is per stage: the metadata args must be re-declared after the
# final FROM even though VERSION/GIT_SHA are also consumed by the builder.
# version/revision/created come only from build args, never hardcoded values.
ARG VERSION=0.0.0-dev
ARG GIT_SHA=unknown
ARG BUILD_DATE=""

# Custom labels for traditional inspection tools plus the full OCI 1.0
# annotation set for the standard image toolchain.
LABEL license='SPDX-License-Identifier: Apache-2.0' \
      copyright='Copyright (c) 2026: YIQISOFT' \
      org.opencontainers.image.title='nereidedge' \
      org.opencontainers.image.description='EdgeX Foundry Application Service bridging EdgeX devices to the NereidLink MQTT gateway protocol' \
      org.opencontainers.image.vendor='YIQISOFT' \
      org.opencontainers.image.licenses='Apache-2.0' \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.source='https://github.com/NereidAI/NereidEdge' \
      org.opencontainers.image.url='https://github.com/NereidAI/NereidEdge' \
      org.opencontainers.image.documentation='https://github.com/NereidAI/NereidEdge#readme' \
      org.opencontainers.image.authors='NereidAI (https://github.com/NereidAI)'

WORKDIR /app

# Legal texts at the scanner-conventional /licenses path (Syft/Trivy). The
# extensionless files are unaffected by the .dockerignore *.md rule.
COPY LICENSE NOTICE /licenses/
COPY --from=builder /etc/ssl/cert.pem /etc/ssl/cert.pem
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /build/nereid-edge /app/nereid-edge
COPY res /app/res
COPY --chmod=0755 docker/entrypoint.sh /app/entrypoint.sh

# /app/data holds the encrypted Edge Secret store (ADR-0034) and the rotated
# agent log (ADR-0035 sys.logs_tail). Copy the prepared directory with runtime
# ownership so a named volume mounted over it inherits the expected UID/GID.
COPY --from=builder --chown=1000:1000 /build/data /app/data

USER 1000

EXPOSE 59750

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["-cp=keeper.http://edgex-core-keeper:59890", "--registry"]
