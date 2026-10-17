use kish_lingshu_sdk::{ClientBuilder, ClientConfig, NoPrincipal};

fn connect() {
    let builder: ClientBuilder<NoPrincipal> =
        ClientBuilder::new(ClientConfig::new("https://lingshu.example"));
    let _ = builder.connect();
}

fn main() {}
