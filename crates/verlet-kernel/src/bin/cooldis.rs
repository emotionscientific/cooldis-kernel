#[tokio::main]
async fn main() {
    eprintln!(
        "warning: {} is deprecated; use verlet (compatibility will be removed in v0.4.0)",
        concat!("cool", "dis")
    );
    if let Err(err) = verlet::cli::run().await {
        eprintln!("verlet: {err}");
        std::process::exit(1);
    }
}
