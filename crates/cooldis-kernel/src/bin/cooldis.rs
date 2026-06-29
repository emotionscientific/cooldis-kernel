#[tokio::main]
async fn main() {
    if let Err(err) = cooldis::cli::run().await {
        eprintln!("cooldis: {err}");
        std::process::exit(1);
    }
}
