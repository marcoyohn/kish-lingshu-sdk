//! Canonical Workflow Event stream binding implementation.

use std::marker::PhantomData;

use kish_lingshu_runtime_contract::EventCursor;
use serde::de::DeserializeOwned;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SseParseError {
    #[error("SSE event is not valid UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("SSE data is not valid JSON: {source}; data={data}")]
    Json {
        data: String,
        #[source]
        source: serde_json::Error,
    },
}

pub(crate) enum SseItem<T> {
    Event(T),
    Done,
}

/// Incremental parser for JSON SSE data frames.
pub(crate) struct JsonSseParser<T> {
    buffer: Vec<u8>,
    marker: PhantomData<fn() -> T>,
}

impl<T> Default for JsonSseParser<T> {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            marker: PhantomData,
        }
    }
}

impl<T> JsonSseParser<T>
where
    T: DeserializeOwned,
{
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push(
        &mut self,
        chunk: impl AsRef<[u8]>,
    ) -> Vec<Result<SseItem<T>, SseParseError>> {
        self.buffer.extend_from_slice(chunk.as_ref());
        self.drain(false)
    }

    pub(crate) fn finish(&mut self) -> Vec<Result<SseItem<T>, SseParseError>> {
        self.drain(true)
    }

    fn drain(&mut self, flush: bool) -> Vec<Result<SseItem<T>, SseParseError>> {
        let mut events = Vec::new();
        while let Some((block_end, boundary_end)) = find_boundary(&self.buffer) {
            let block = self.buffer[..block_end].to_vec();
            self.buffer.drain(..boundary_end);
            if let Some(event) = parse_block(&block) {
                events.push(event);
            }
        }
        if flush && self.buffer.iter().any(|byte| !byte.is_ascii_whitespace()) {
            let block = std::mem::take(&mut self.buffer);
            if let Some(event) = parse_block(&block) {
                events.push(event);
            }
        } else if flush {
            self.buffer.clear();
        }
        events
    }
}

fn find_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    for index in 0..buffer.len() {
        if buffer[index] != b'\n' {
            continue;
        }
        let block_end = if index > 0 && buffer[index - 1] == b'\r' {
            index - 1
        } else {
            index
        };
        if buffer.get(index + 1) == Some(&b'\n') {
            return Some((block_end, index + 2));
        }
        if buffer.get(index + 1) == Some(&b'\r') && buffer.get(index + 2) == Some(&b'\n') {
            return Some((block_end, index + 3));
        }
    }
    None
}

fn parse_block<T>(block: &[u8]) -> Option<Result<SseItem<T>, SseParseError>>
where
    T: DeserializeOwned,
{
    let block = match std::str::from_utf8(block) {
        Ok(block) => block,
        Err(error) => return Some(Err(error.into())),
    };
    let data = block
        .lines()
        .filter_map(|line| {
            line.strip_suffix('\r')
                .unwrap_or(line)
                .strip_prefix("data:")
        })
        .map(|value| value.strip_prefix(' ').unwrap_or(value))
        .collect::<Vec<_>>()
        .join("\n");
    if data.is_empty() {
        return None;
    }
    if data.trim() == "[DONE]" {
        return Some(Ok(SseItem::Done));
    }
    Some(
        serde_json::from_str::<T>(&data)
            .map(SseItem::Event)
            .map_err(|source| SseParseError::Json { data, source }),
    )
}

/// Cursor state carried across SSE connections for one logical observation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SseReattachment {
    last_accepted_cursor: Option<EventCursor>,
    connection_attempt: u32,
}

impl SseReattachment {
    pub(crate) fn new(after: Option<EventCursor>) -> Self {
        Self {
            last_accepted_cursor: after,
            connection_attempt: 1,
        }
    }

    pub(crate) fn accept(&mut self, cursor: EventCursor) {
        self.last_accepted_cursor = Some(cursor);
    }

    pub(crate) fn reconnect(&mut self) {
        self.connection_attempt += 1;
    }

    pub(crate) fn after(&self) -> Option<&EventCursor> {
        self.last_accepted_cursor.as_ref()
    }

    pub(crate) fn connection_attempt(&self) -> u32 {
        self.connection_attempt
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::Value;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct TestEvent {
        #[serde(rename = "type")]
        kind: String,
        #[serde(default)]
        data: Option<Value>,
    }

    #[test]
    fn parser_handles_fragmented_frames_utf8_and_done_markers() {
        let payload = "data: {\"type\":\"message\",\"data\":{\"content\":\"你\"}}\n\n";
        let bytes = payload.as_bytes();
        let split = bytes
            .windows(3)
            .position(|window| window == "你".as_bytes())
            .unwrap()
            + 1;
        let mut parser = JsonSseParser::<TestEvent>::new();
        assert!(parser.push(&bytes[..split]).is_empty());
        let events = parser.push(&bytes[split..]);
        let SseItem::Event(event) = events.into_iter().next().unwrap().unwrap() else {
            panic!("expected event");
        };
        assert_eq!(event.kind, "message");
        assert_eq!(event.data.unwrap()["content"], "你");
        assert!(matches!(
            parser.push(b"data: [DONE]\r\n\r\n").as_slice(),
            [Ok(SseItem::Done)]
        ));
    }

    #[test]
    fn parser_reports_bad_event_and_continues() {
        let mut parser = JsonSseParser::<TestEvent>::new();
        let events = parser.push(b"data: nope\n\ndata: {\"type\":\"valid\"}\n\n");
        assert!(events[0].is_err());
        assert!(matches!(&events[1], Ok(SseItem::Event(event)) if event.kind == "valid"));
    }

    #[test]
    fn reattachment_starts_strictly_after_the_last_accepted_cursor() {
        let mut state = SseReattachment::new(Some(EventCursor::from("run-1:4")));
        assert_eq!(state.after().map(AsRef::as_ref), Some("run-1:4"));
        state.accept(EventCursor::from("run-1:5"));
        state.reconnect();
        assert_eq!(state.after().map(AsRef::as_ref), Some("run-1:5"));
        assert_eq!(state.connection_attempt(), 2);
    }
}
