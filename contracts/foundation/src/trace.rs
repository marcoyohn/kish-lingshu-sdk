//! Bounded W3C version-00 traceparent value. Observability, never authority.
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceParent {
    trace_id: String,
    span_id: String,
    flags: String,
}
impl TraceParent {
    pub fn parse(value: &str) -> Option<Self> {
        let bytes = value.as_bytes();
        if bytes.len() != 55 || &bytes[..3] != b"00-" || bytes[35] != b'-' || bytes[52] != b'-' {
            return None;
        }
        let hex = |value: &[u8]| {
            value
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        };
        if !hex(&bytes[3..35])
            || !hex(&bytes[36..52])
            || !hex(&bytes[53..55])
            || bytes[3..35].iter().all(|b| *b == b'0')
            || bytes[36..52].iter().all(|b| *b == b'0')
        {
            return None;
        }
        Some(Self {
            trace_id: value[3..35].into(),
            span_id: value[36..52].into(),
            flags: value[53..55].into(),
        })
    }
    pub fn root(trace_id: &str, span_id: &str) -> Option<Self> {
        Self::parse(&format!("00-{trace_id}-{span_id}-01"))
    }
    pub fn child(&self, span_id: &str) -> Option<Self> {
        Self::parse(&format!("00-{}-{span_id}-{}", self.trace_id, self.flags))
    }
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }
    pub fn span_id(&self) -> &str {
        &self.span_id
    }
    pub fn flags(&self) -> &str {
        &self.flags
    }
}
impl fmt::Display for TraceParent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "00-{}-{}-{}", self.trace_id, self.span_id, self.flags)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    const VALID: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    #[test]
    fn bounded_parent_and_child_preserve_trace_and_sampling_with_distinct_span_ids() {
        let parent = TraceParent::parse(VALID).unwrap();
        let child = parent.child("0123456789abcdef").unwrap();
        assert_eq!(parent.trace_id(), child.trace_id());
        assert_ne!(parent.span_id(), child.span_id());
        assert_eq!(child.flags(), "00");
        assert_eq!(parent.to_string(), VALID);
        assert!(parent.child("0000000000000000").is_none());
    }
    #[test]
    fn malformed_unsupported_zero_ids_uppercase_and_secret_metadata_are_ignored() {
        for value in [
            String::new(),
            VALID.replace("00-", "ff-"),
            VALID.replace(
                "4bf92f3577b34da6a3ce929d0e0e4736",
                "00000000000000000000000000000000",
            ),
            VALID.replace("00f067aa0ba902b7", "0000000000000000"),
            VALID.to_uppercase(),
            format!("{VALID}\r\nsecret=credential"),
            "secret".repeat(1000),
        ] {
            assert!(TraceParent::parse(&value).is_none());
        }
    }
}
