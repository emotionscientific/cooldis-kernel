/// Asserts `addr` becomes bindable within a bounded window.
///
/// A freed socket binds on the first try; the window only absorbs an
/// unrelated test briefly holding the port.
pub(crate) async fn assert_addr_released(addr: std::net::SocketAddr) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                drop(listener);
                return;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    panic!("address {addr} was not released within 10 seconds: {error}");
                }
                tokio::time::sleep(std::cmp::min(
                    std::time::Duration::from_millis(100),
                    deadline - now,
                ))
                .await;
            }
            Err(error) => panic!("failed to bind released address {addr}: {error}"),
        }
    }
}
