//! Minimal client for Neon's storage-broker publication contract.

pub(crate) mod proto {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("storage_broker");
}
