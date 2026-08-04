#[tokio::main]
async fn main() {
    if let Err(err) = verlet::cli::run().await {
        eprintln!("verlet: {err}");
        std::process::exit(1);
    }
}
