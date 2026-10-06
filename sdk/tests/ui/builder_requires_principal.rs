use kish_lingshu_sdk::{ClientBuilder, ClientConfig};

fn connect() {
    let _ = ClientBuilder::new(ClientConfig::new("https://lingshu.example")).connect();
}

fn main() {}
