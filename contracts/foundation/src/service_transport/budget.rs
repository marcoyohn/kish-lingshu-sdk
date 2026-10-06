//! Pre-decoding accounting shared by network adapters. Authority is checked
//! separately; admission to a bounded queue never proves identity or custody.
use super::{MAX_KEY_BYTES, MAX_PROOF_BYTES};

/// Reserve space for the retained query object and small owned metadata too.
/// This is a logical reservation, not a claim about allocator or transport RSS.
pub const QUERY_RESERVATION_OVERHEAD_BYTES: usize = 4096;

/// Lingshu exact queries have no selector parameters. Router provenance may
/// occupy an attachment, independently bounded by the proof limit.
pub fn query_reservation_bytes(
    payload_bytes: Option<usize>,
    payload_limit: usize,
    key_bytes: usize,
    parameters_empty: bool,
    attachment_bytes: usize,
) -> Option<u32> {
    let payload = payload_bytes?;
    if payload == 0
        || payload > payload_limit
        || key_bytes == 0
        || key_bytes > MAX_KEY_BYTES
        || !parameters_empty
        || attachment_bytes > MAX_PROOF_BYTES
    {
        return None;
    }
    payload
        .checked_add(key_bytes)?
        .checked_add(attachment_bytes)?
        .checked_add(QUERY_RESERVATION_OVERHEAD_BYTES)?
        .try_into()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_includes_provenance_and_metadata_before_decode() {
        assert_eq!(
            query_reservation_bytes(Some(32), 32, 8, true, MAX_PROOF_BYTES),
            Some((32 + 8 + MAX_PROOF_BYTES + QUERY_RESERVATION_OVERHEAD_BYTES) as u32)
        );
        for input in [
            (None, 1, true, 0),
            (Some(0), 1, true, 0),
            (Some(33), 1, true, 0),
            (Some(1), 0, true, 0),
            (Some(1), MAX_KEY_BYTES + 1, true, 0),
            (Some(1), 1, false, 0),
            (Some(1), 1, true, MAX_PROOF_BYTES + 1),
        ] {
            assert_eq!(
                query_reservation_bytes(input.0, 32, input.1, input.2, input.3),
                None
            );
        }
        assert_eq!(
            query_reservation_bytes(Some(usize::MAX), usize::MAX, 1, true, 0),
            None
        );
    }
}
