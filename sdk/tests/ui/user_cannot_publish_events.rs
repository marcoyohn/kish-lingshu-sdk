use kish_lingshu_sdk::{Client, UserPrincipal};

fn publish(client: &Client<UserPrincipal>) {
    let _ = client.event_dispatch();
}

fn main() {}
