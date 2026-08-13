//! Contract tests for one active WAL transport over every assigned ring endpoint.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use verglas_ring_proxy::serve_tcp_pool;

#[tokio::test]
async fn client_sessions_rotate_across_all_ring_endpoints() {
    let mut endpoints = Vec::new();
    let counts = Arc::new((0..4).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    for member in 0..4 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind backend");
        endpoints.push(listener.local_addr().expect("address").to_string());
        let counts = Arc::clone(&counts);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                counts[member].fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let mut byte = [0_u8; 1];
                    if stream.read_exact(&mut byte).await.is_ok() {
                        let _ = stream.write_all(&[member as u8]).await;
                    }
                });
            }
        });
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pool");
    let address = listener.local_addr().expect("pool address");
    tokio::spawn(serve_tcp_pool(listener, endpoints));

    let mut observed = Vec::new();
    for _ in 0..8 {
        let mut client = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect pool");
        client.write_all(b"w").await.expect("write");
        let mut member = [0_u8; 1];
        client.read_exact(&mut member).await.expect("read member");
        observed.push(member[0]);
    }
    observed.sort_unstable();
    observed.dedup();
    assert_eq!(observed, vec![0, 1, 2, 3]);
    assert!(
        counts
            .iter()
            .all(|count| count.load(Ordering::Relaxed) >= 2)
    );
}

#[tokio::test]
async fn a_connection_failure_uses_another_ring_endpoint() {
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind dead endpoint");
    let dead_address = dead.local_addr().expect("dead address");
    drop(dead);
    let live = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind live endpoint");
    let live_address = live.local_addr().expect("live address");
    tokio::spawn(async move {
        let (mut stream, _) = live.accept().await.expect("accept live");
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await.expect("read");
        stream.write_all(b"l").await.expect("write");
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pool");
    let address = listener.local_addr().expect("pool address");
    tokio::spawn(serve_tcp_pool(
        listener,
        vec![dead_address.to_string(), live_address.to_string()],
    ));
    let mut client = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect pool");
    client.write_all(b"w").await.expect("write");
    let mut response = [0_u8; 1];
    client
        .read_exact(&mut response)
        .await
        .expect("failover response");
    assert_eq!(&response, b"l");
}
