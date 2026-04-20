# enterprise/ — BUSL-1.1 licensed components

Everything under this directory ships in the same `objectio-gateway` binary
as the rest of the code, but is governed by the **Business Source License
1.1** (see [LICENSE](./LICENSE)) rather than the Apache-2.0 license that
covers the rest of the repo.

## What's here

- `crates/objectio-iceberg/` — Iceberg REST Catalog implementation
- `crates/objectio-delta-sharing/` — Delta Sharing protocol server

## What's not

- The **license signer** (`objectio-license-gen`) lives in a separate
  private repo. The Ed25519 **private signing key** stays off-repo
  entirely (stored outside any git tree by the maintainer).
- Runtime license verification code is in `crates/objectio-license/`
  (Apache-2.0) — that's open by design, since verifying a signature is
  inherently a public operation. The moat is the private key.

## License summary

BUSL-1.1 makes these files **source-available but not open source**:

- You can read, run, modify, and redistribute the code for any non-
  competitive purpose.
- You can run it in production internally — "additional use grant" covers
  operational use.
- You **cannot** offer it to third parties as a hosted / managed service
  that competes with ObjectIO's paid offerings without a commercial
  license.
- On **2030-04-18** (four years after first release), this directory
  automatically converts to Apache-2.0 under the BUSL change-license
  clause.

See the [LICENSE](./LICENSE) file for the full text, including the exact
definitions of "competitive offering" and "Product".

## Runtime gating

These features are also gated at runtime by a signed Ed25519 license
file (see `crates/objectio-license/`). Without a valid Enterprise
license installed, the `/iceberg/*`, `/delta-sharing/*`, and related
admin endpoints return a structured 403 with
`"error": "EnterpriseLicenseRequired"`. Install a license through the
console or `PUT /_admin/license` to unlock them.

The licensing model and code split are documented in `FEATURES.md`
(section 10, Licensing & Tiering) in the sibling `objectio-docs` repo.
