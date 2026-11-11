<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->

# Contributing to NereidEdge

Thanks for taking the time to contribute. NereidEdge is an EdgeX Foundry
Application Service that bridges EdgeX device events to the NereidLink
gateway MQTT protocol. This guide covers the local toolchain, the commit
rules, and the repository-specific conventions.

## Development setup

- Go 1.27.3, matching the `go.mod` directive and the Dockerfile base image.
- golangci-lint v2.15.0 is required by `make lint` and `make verify`.
- The NereidLink device SDK is a reviewed in-repo source mirror under
  `third_party/nereidlink`. A fresh clone builds without any adjacent
  checkout or private module credentials.

## Build and test commands

Only commands that exist in the repository are listed below.

```bash
# Aggregate local gate: gofmt check, go vet, golangci-lint, the full test
# suite, and a linux/amd64 build. Run this before every submission.
make verify

# Individual gates.
make test          # Go unit tests via the project's test flags
make vet           # go vet
make lint          # golangci-lint
go test ./...      # Run the full test suite directly with the go tool

# Container images, built locally only. Make never performs a docker login
# or a push; GHCR publication is owned exclusively by the GitHub Actions
# release workflows.
make docker        # Multi-architecture OCI archive under dist/, not loaded
make docker-dev    # Native local image tagged with the full GHCR coordinates

# Maintainer-only target: refresh third_party/nereidlink from a clean
# upstream NereidLink checkout. No build or test target depends on it.
make sdk-mirror
```

## Developer Certificate of Origin

Every contribution must be signed under the Developer Certificate of Origin.
Create commits with `git commit -s`, which adds a `Signed-off-by` trailer
using your configured git identity:

```bash
git -c user.name='Your Name' -c user.email='you@example.com' commit -s
```

The `Signed-off-by` trailer must match the commit author exactly. The
repository's `DCO` GitHub Actions workflow (`.github/workflows/dco.yml`)
checks every non-merge commit in a pull request and fails the build when a
matching trailer is missing.

## SPDX license headers

Every hand-written source file starts with an SPDX header using the comment
form of its language:

- Go, shell, YAML, workflows, the Makefile, and dotfiles use the `//` or `#`
  comment form with `Copyright (C) 2026 YIQISOFT` and
  `SPDX-License-Identifier: Apache-2.0`.
- Markdown files use an HTML comment on the first line:
  `<!-- Copyright (C) 2026 YIQISOFT; SPDX-License-Identifier: Apache-2.0 -->`.
- Shell scripts place the header immediately after the shebang line.

Exceptions that intentionally carry no header: the `LICENSE` and `NOTICE`
legal texts, JSON files, `go.mod`/`go.sum` and other generated lock files,
and the entire `third_party/` mirror, which keeps its upstream headers.

## Language rules

- All code comments, committed documentation, commit messages, API response
  messages, and log messages are written in English.
- The bilingual `DisplayName`/`Description` catalogs are a delivered
  product feature, not documentation: keep both keys of each
  `map[string]string{"en": ..., "zh": ...}` entry, and keep the tests that
  assert both keys in sync. Do not remove a locale to "simplify" a catalog.

## The `third_party/nereidlink` mirror is read-only

Do not edit files under `third_party/nereidlink/` directly. It is a reviewed
source mirror of the upstream NereidLink device SDK and keeps upstream
license files and conventions. Bug fixes go to the upstream repository and
enter this tree through the maintainer-only `make sdk-mirror` refresh, which
also updates the recorded `REVISION`.

## Pull request checklist

- [ ] `make verify` passes locally.
- [ ] Every commit carries a matching `Signed-off-by` trailer.
- [ ] New source files carry the SPDX header in the correct comment form.
- [ ] Comments, docs, and user-facing strings are English-only, except the
      bilingual catalog entries described above.
- [ ] No secrets, credentials, internal hostnames, or private network
      addresses are committed; `.env.example` contains placeholders only.
- [ ] No files under `third_party/` were edited directly.
