//! Baked-in Ed25519 public key used to verify license signatures.
//!
//! The **private** counterpart stays strictly off-repo — see
//! `docs/LICENSE.md` for the keygen procedure. A build-time all-zero
//! placeholder means "no licenses will ever verify" and every license load
//! will fail with [`crate::LicenseError::MissingPublicKey`].
//!
//! After running `cargo run -p objectio-license-gen -- keygen`, replace the
//! 32 zero bytes below with the generated public key.

pub const LICENSE_PUBLIC_KEY: [u8; 32] = [
    0x7c, 0xe6, 0xd3, 0x78, 0x9e, 0x8a, 0x84, 0xa4, 0xa8, 0x29, 0x71, 0x7d, 0x6c, 0xe3, 0x49, 0xf2,
    0x2f, 0x79, 0x8e, 0xcc, 0x98, 0x73, 0x15, 0x39, 0x16, 0x62, 0x45, 0x85, 0x8e, 0x31, 0xa1, 0x58,
];
