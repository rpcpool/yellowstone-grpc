use std::time::Instant;

#[derive(Default)]
pub struct ClientShedder {
    first_throttle_hint: Option<Instant>,
}

impl ClientShedder {}
