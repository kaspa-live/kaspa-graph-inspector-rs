use async_stream::stream;
use axum::{
    body::Body,
    http::{Response, header},
    response::{
        IntoResponse,
        sse::{Event, Sse},
    },
};

use crate::{
    client::SseMessage,
    codec::sse::{CLIENT_REGISTRATION_EVENT, PUBLICATION_STATE_EVENT, PUBLICATION_WAKEUP_EVENT},
    service::SseConnection,
};

/// Frames one admitted connection's semantic messages as an SSE response.
pub(super) fn response(mut connection: SseConnection) -> Response<Body> {
    let cancellation = connection.cancellation();
    let messages = stream! {
        while let Some(message) = connection.next_message().await {
            match frame(message) {
                Ok(event) => yield Ok::<Event, serde_json::Error>(event),
                Err(error) => {
                    yield Err(error);
                    break;
                }
            }
        }
    };
    let mut response = Sse::new(messages).into_response();
    response.headers_mut().insert(header::CACHE_CONTROL, "no-store".parse().expect("static cache policy is valid"));
    super::connection::track_sse(&mut response, cancellation);
    response
}

fn frame(message: SseMessage) -> Result<Event, serde_json::Error> {
    let (event, data) = match message {
        SseMessage::ClientRegistration(payload) => (CLIENT_REGISTRATION_EVENT, payload.encode()?),
        SseMessage::PublicationWakeup(payload) => (PUBLICATION_WAKEUP_EVENT, payload.encode()?),
        SseMessage::PublicationState(payload) => (PUBLICATION_STATE_EVENT, payload.encode()?),
    };
    let data = String::from_utf8(data).expect("JSON encoding produces UTF-8");
    Ok(Event::default().event(event).data(data))
}
