use kish_lingshu_sdk::{Client, ServicePrincipal, UserPrincipal};

fn removed_service_api(client: &Client<ServicePrincipal>) {
    let _ = client.user_tasks();
}

fn removed_user_api(client: &Client<UserPrincipal>) {
    let _ = client.user_tasks();
}

fn removed_interaction_service_api(client: &Client<ServicePrincipal>) {
    let _ = client.interactions();
}

fn removed_interaction_user_api(client: &Client<UserPrincipal>) {
    let _ = client.interactions();
}

fn main() {}
