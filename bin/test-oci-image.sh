#!/usr/bin/env bash
# Copyright (C) 2026 YIQISOFT
# SPDX-License-Identifier: Apache-2.0
#
# Verify that an exported OCI archive contains exactly the requested runnable
# platforms and that every platform image carries the required license and
# OCI annotation labels. Buildx provenance attestations use unknown/unknown
# and are ignored.

set -euo pipefail

archive=${1:?"usage: $0 <oci-archive> <comma-separated-platforms>"}
platforms=${2:?"usage: $0 <oci-archive> <comma-separated-platforms>"}

if [ ! -s "${archive}" ]; then
    echo "OCI archive is missing or empty: ${archive}" >&2
    exit 1
fi

root_index=$(tar -xOf "${archive}" index.json)
mapfile -t index_digests < <(
    jq -r '.manifests[] | select(.mediaType == "application/vnd.oci.image.index.v1+json") | .digest' \
        <<<"${root_index}" | sort -u
)
if [ "${#index_digests[@]}" -ne 1 ]; then
    echo "expected one unique multi-platform image index, found ${#index_digests[@]}" >&2
    exit 1
fi

digest=${index_digests[0]#sha256:}
image_index=$(tar -xOf "${archive}" "blobs/sha256/${digest}")
actual=$(
    jq -r '.manifests[] | select(.platform.os == "linux") |
        [.platform.os, .platform.architecture, (.platform.variant // "")] |
        join("/") | sub("/$"; "")' <<<"${image_index}" | sort -u
)
expected=$(printf '%s' "${platforms}" | tr ',' '\n' | sed '/^$/d' | sort -u)

if [ -z "${expected}" ]; then
    echo "at least one target platform is required" >&2
    exit 1
fi
if ! diff -u <(printf '%s\n' "${expected}") <(printf '%s\n' "${actual}"); then
    echo "OCI image platforms do not match the requested targets" >&2
    exit 1
fi

printf 'OCI image %s contains platforms:\n%s\n' "${archive}" "${actual}"

# Verify license and OCI annotation labels on every linux platform image.
mapfile -t manifest_digests < <(
    jq -r '.manifests[] | select(.platform.os == "linux") | .digest' <<<"${image_index}" | sort -u
)
if [ "${#manifest_digests[@]}" -eq 0 ]; then
    echo "no linux platform manifest found for label verification" >&2
    exit 1
fi

# label=expected-value pairs for exact-match assertions; an empty expected
# value means "key must exist and be non-empty".
required_labels=(
    "license=SPDX-License-Identifier: Apache-2.0"
    "copyright=Copyright (c) 2026: YIQISOFT"
    "org.opencontainers.image.title=nereidedge"
    "org.opencontainers.image.description="
    "org.opencontainers.image.vendor=YIQISOFT"
    "org.opencontainers.image.licenses=Apache-2.0"
    "org.opencontainers.image.version="
    "org.opencontainers.image.revision="
    "org.opencontainers.image.created="
    "org.opencontainers.image.source=https://github.com/NereidAI/NereidEdge"
    "org.opencontainers.image.url=https://github.com/NereidAI/NereidEdge"
    "org.opencontainers.image.documentation=https://github.com/NereidAI/NereidEdge#readme"
    "org.opencontainers.image.authors=NereidAI (https://github.com/NereidAI)"
)
rfc3339='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'

for manifest in "${manifest_digests[@]}"; do
    manifest_blob="${manifest#sha256:}"
    config_digest=$(tar -xOf "${archive}" "blobs/sha256/${manifest_blob}" \
        | jq -er '.config.digest')
    labels=$(tar -xOf "${archive}" "blobs/sha256/${config_digest#sha256:}" \
        | jq -ec '.config.Labels')
    for pair in "${required_labels[@]}"; do
        key=${pair%%=*}
        expected_value=${pair#*=}
        value=$(jq -er --arg key "${key}" '.[$key] // empty' <<<"${labels}")
        if [ -z "${value}" ]; then
            echo "missing or empty label ${key} on manifest ${manifest:7:12}" >&2
            exit 1
        fi
        if [ -n "${expected_value}" ] && [ "${value}" != "${expected_value}" ]; then
            echo "label ${key} = '${value}', want '${expected_value}'" >&2
            exit 1
        fi
    done
    created=$(jq -er '.["org.opencontainers.image.created"]' <<<"${labels}")
    if ! [[ ${created} =~ ${rfc3339} ]]; then
        echo "label org.opencontainers.image.created is not RFC 3339 UTC: ${created}" >&2
        exit 1
    fi
    revision=$(jq -er '.["org.opencontainers.image.revision"]' <<<"${labels}")
    if [ "${revision}" = "unknown" ]; then
        echo "label org.opencontainers.image.revision must not keep the unknown default" >&2
        exit 1
    fi
    printf 'labels verified for manifest %s\n' "${manifest:7:12}"
done
