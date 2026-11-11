# Copyright (C) 2026 YIQISOFT
# SPDX-License-Identifier: Apache-2.0

# Optional local build acceleration: copy local.mk.example to local.mk
# (git-ignored) and set regional proxy/mirror endpoints there. A missing
# file means a pristine direct-internet build, as used by CI.
-include local.mk

.PHONY: build clean test vet lint verify sdk-mirror docker docker-builder docker-dev up logs dev-logs build-all run help

BINARY := nereid-edge
REGISTRY ?= ghcr.io/nereidai
# No VERSION file: the git tag is the single version source. On an exact v*
# tag checkout use its bare SemVer (v0.0.1 -> 0.0.1); any other checkout
# builds the rolling dev channel 0.0.0-dev. CI may inject VERSION.
VERSION ?= $(shell ref=$$(git describe --tags --exact-match 2>/dev/null) && echo "$${ref#v}" || echo 0.0.0-dev)
# Local and published images use the same full coordinates. A local tag does
# not push to GHCR; Make has no push target.
IMAGE_NAME := nereidedge
IMAGE := $(REGISTRY)/$(IMAGE_NAME)
DOCKER := $(shell docker info >/dev/null 2>&1 && echo docker || echo "sudo docker")
GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
# Three-tag scheme: VERSION plus UTC-timestamp and short-SHA traceability
# tags. No latest tag; Make only builds/packages, never pushes.
BUILD_TS := $(shell date -u +%Y%m%d-%H%M)
# RFC 3339 UTC build time for the OCI org.opencontainers.image.created label.
# Distinct from the compact BUILD_TS used in image tags; never mix the two.
BUILD_DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -ldflags "-X main.Version=$(VERSION) -X main.GitSHA=$(GIT_SHA)"

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY) ./cmd/nereid-edge

build-local:
	CGO_ENABLED=0 go build $(LDFLAGS) -o $(BINARY) ./cmd/nereid-edge

clean:
	rm -f $(BINARY) coverage.out
	rm -rf dist

test:
	go test -v ./...

test-cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

vet:
	go vet ./...

lint:
	golangci-lint run --timeout=120s

# verify is the local and release gate. Check formatting before the slower
# compilation and test steps so style-only failures return quickly.
verify:
	@unformatted="$$(gofmt -l cmd internal)"; \
	if [ -n "$$unformatted" ]; then \
		echo "The following files are not gofmt-clean:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	$(MAKE) vet
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) build

# Maintainer-only mirror refresh: NEREIDLINK_DIR points at a clean checkout
# of the still-private NereidLink repository. No build or test target depends
# on it; the in-repo third_party mirror is the single SDK source for local,
# CI and release builds. Once the SDK is published as a public Go module this
# target and the mirror can be replaced by a normal versioned require.
NEREIDLINK_DIR ?= ../NereidLink
SDK_MIRROR_DIR := third_party/nereidlink

# sdk-mirror refreshes the CI/release source copy from a clean upstream SDK.
# Refusing dirty inputs keeps REVISION sufficient to reproduce the exact copy.
sdk-mirror:
	@test "$(abspath $(NEREIDLINK_DIR))" != "$(abspath $(SDK_MIRROR_DIR))" || \
		{ echo "Mirror source and destination must differ."; exit 1; }
	@dirty="$$(git -C "$(NEREIDLINK_DIR)" status --porcelain -- sdk/device/go LICENSE NOTICE THIRD_PARTY_NOTICES)"; \
	if [ -n "$$dirty" ]; then \
		echo "NereidLink SDK mirror input is not clean:"; \
		echo "$$dirty"; \
		exit 1; \
	fi
	rm -rf "$(SDK_MIRROR_DIR)/sdk/device/go"
	mkdir -p "$(SDK_MIRROR_DIR)"
	@revision="$$(git -C "$(NEREIDLINK_DIR)" rev-parse HEAD)"; \
	git -C "$(NEREIDLINK_DIR)" archive "$$revision" \
		sdk/device/go LICENSE NOTICE THIRD_PARTY_NOTICES | \
		tar -x -C "$(SDK_MIRROR_DIR)"; \
	printf '%s\n' "$$revision" > "$(SDK_MIRROR_DIR)/REVISION"

PLATFORMS ?= linux/amd64,linux/arm64
BUILDX_BUILDER ?= default
BUILDX = $(DOCKER) buildx --builder $(BUILDX_BUILDER)
IMAGE_ARCHIVE ?= dist/images/nereidedge-$(VERSION)-multiarch.oci.tar
IMAGE_BUILD_ARGS = --build-arg GOPROXY=$(GOPROXY) \
	--build-arg ALPINE_MIRROR=$(ALPINE_MIRROR) \
	--build-arg VERSION=$(VERSION) \
	--build-arg GIT_SHA=$(GIT_SHA) \
	--build-arg BUILD_DATE=$(BUILD_DATE) \
	-t $(IMAGE):$(VERSION) \
	-t $(IMAGE):$(VERSION)-$(BUILD_TS) \
	-t $(IMAGE):$(VERSION)-$(GIT_SHA) .

# The default Docker BuildKit backend can export a multi-platform OCI archive.
# A dedicated container builder remains available through BUILDX_BUILDER when
# a remote cache or another BuildKit driver is needed.
docker-builder:
	@$(DOCKER) buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 || \
		$(DOCKER) buildx create --name $(BUILDX_BUILDER) --driver docker-container --bootstrap

# docker builds a multi-architecture OCI archive. Docker's local image store
# cannot load a multi-platform index; docker-dev remains the local-run path.
docker: docker-builder
	mkdir -p $(dir $(IMAGE_ARCHIVE))
	$(BUILDX) build --platform $(PLATFORMS) \
		--output type=oci,dest=$(IMAGE_ARCHIVE) $(IMAGE_BUILD_ARGS)
	@echo "Built OCI archive: $(IMAGE_ARCHIVE)"
	@echo "The archive is not loaded into the local Docker image store and is not pushed to GHCR."
	@echo "Use make docker-dev for a local runnable image; GitHub Actions owns GHCR publication."
	@echo "Image tags embedded in the archive:"
	@echo "  $(IMAGE):$(VERSION)"
	@echo "  $(IMAGE):$(VERSION)-$(BUILD_TS)"
	@echo "  $(IMAGE):$(VERSION)-$(GIT_SHA)"

# docker-dev builds the VERSION-tagged native image used by local Compose.
# The full GHCR coordinates are used as a local tag only; nothing is pushed.
docker-dev:
	$(DOCKER) build \
		--build-arg GOPROXY=$(GOPROXY) \
		--build-arg ALPINE_MIRROR=$(ALPINE_MIRROR) \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_SHA=$(GIT_SHA) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t $(IMAGE):$(VERSION) .
	@echo "Built local image tag only: $(IMAGE):$(VERSION)"
	@echo "Not pushed to GHCR; no Docker login or push is performed by Make."

# Operator values for Compose. Defaults to the conventional git-ignored
# .env in the repository root: copy .env.example to .env and fill in the
# broker and credential values. The --env-file flag is added only when the
# file exists, so a fresh clone works without any external deployment
# directory. Override ENV_FILE to select another operator profile.
ENV_FILE ?= .env
COMPOSE = $(DOCKER) compose
ifneq ($(wildcard $(ENV_FILE)),)
COMPOSE += --env-file "$(ENV_FILE)"
endif

# up starts the local docker compose stack using the VERSION-tagged native image.
up:
	$(COMPOSE) up -d

# logs follows the nereid-edge compose service logs.
logs:
	$(COMPOSE) logs -f --tail=100 nereid-edge

# Dev-mode log location, mirroring the workspace per-product convention.
DEV_LOGDIR ?= ../.tmp/NereidEdge/logs
DEV_LOG := $(DEV_LOGDIR)/backend.log

# dev-logs follows the compose logs and mirrors them into the product dev-log
# file so dev-mode runs are captured outside the repo tree.
dev-logs:
	mkdir -p $(DEV_LOGDIR)
	$(COMPOSE) logs -f --tail=100 nereid-edge | tee $(DEV_LOG)

# GHCR publication is owned exclusively by the GitHub Actions workflows with
# packages:write permission: publish-dev.yml for main and release.yml for v*
# tags. This Makefile has no login or push target and does not require GHCR
# write permission. To copy a local build into a separate registry, tag and
# push the image manually outside the repository.

build-all:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY)-linux-amd64 ./cmd/nereid-edge
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(BINARY)-linux-arm64 ./cmd/nereid-edge

run:
	./$(BINARY)

help:
	@echo "Available targets:"
	@echo "  build               Build for linux/amd64"
	@echo "  build-local         Build for current platform"
	@echo "  test                Run tests"
	@echo "  vet                 Run go vet"
	@echo "  lint                Run golangci-lint"
	@echo "  verify              Check formatting, vet, lint, test, and build"
	@echo "  sdk-mirror          Maintainer-only: refresh the in-repo NereidLink SDK mirror"
	@echo "  docker              Build $(PLATFORMS) OCI archive at $(IMAGE_ARCHIVE)"
	@echo "  docker-dev          Build local $(IMAGE):$(VERSION) image for compose"
	@echo "  up                  Start local docker compose stack"
	@echo "  logs                Follow nereid-edge compose logs"
	@echo "  build-all           Build multi-arch binaries"

# Build-network endpoints: empty by default, injected via the shell
# environment; empty values keep pristine upstream sources.
GOPROXY ?=
ALPINE_MIRROR ?=

mod-download:
	GOPROXY=$(GOPROXY) GOSUMDB=sum.golang.org go mod download
