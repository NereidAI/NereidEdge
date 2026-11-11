# NereidLink device SDK mirror

This directory is a reviewed source mirror of `NereidAI/NereidLink`; do not
edit the mirrored files directly. `REVISION` identifies the exact upstream git
commit. `LICENSE`, `NOTICE`, and `THIRD_PARTY_NOTICES` are copied from the same
checkout.

The mirror is the single SDK source for every build: the root `go.mod`
replaces the module with `./third_party/nereidlink/sdk/device/go`, and the
Dockerfile copies it from the repository context. No adjacent checkout,
named build context, or private-module credential is required to build,
test, or release NereidEdge.

Maintainers with access to the still-private upstream repository refresh the
mirror from a clean adjacent checkout, then verify the result:

```bash
make sdk-mirror
make verify
make docker
```

Once the SDK is published as a public Go module, this mirror and the
`sdk-mirror` target can be replaced by a normal versioned `require`.
