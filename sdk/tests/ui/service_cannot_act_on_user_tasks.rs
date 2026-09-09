use kish_lingshu_sdk::{Client, ServicePrincipal};

fn claim(client: &Client<ServicePrincipal>) {
    let tasks = client.user_tasks();
    let _ = tasks.claim();
}

fn main() {}
