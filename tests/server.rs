use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use ciwatch::server::{Limits, serve};

const LIMITS: Limits = Limits {
    header_read: Duration::from_secs(30),
    request: Duration::from_secs(30),
    max_connections: 16,
    drain: Duration::from_secs(30),
};

async fn start(limits: Limits) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    let app = Router::new().route("/", get(|| async { "ok" })).route(
        "/slow",
        get(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            "late"
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, app, limits, async {
        let _ = stop_rx.await;
    }));
    (addr, stop_tx, server)
}

async fn get_status(stream: &mut TcpStream, path: &str) -> String {
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nhost: x\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut buf = vec![0; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n])
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// Reads until the server closes the connection, failing after `within`.
async fn closed_within(stream: &mut TcpStream, within: Duration) {
    let mut buf = vec![0; 1024];
    let closed = tokio::time::timeout(within, async {
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "connection still open after {within:?}");
}

#[tokio::test]
async fn closes_connections_that_send_no_headers() {
    let (addr, _stop, _server) = start(Limits {
        header_read: Duration::from_millis(200),
        ..LIMITS
    })
    .await;
    let mut silent = TcpStream::connect(addr).await.unwrap();
    closed_within(&mut silent, Duration::from_secs(3)).await;
    let mut partial = TcpStream::connect(addr).await.unwrap();
    partial.write_all(b"GET / HTTP/1.1\r\nhost").await.unwrap();
    closed_within(&mut partial, Duration::from_secs(3)).await;
}

#[tokio::test]
async fn times_out_slow_requests() {
    let (addr, _stop, _server) = start(Limits {
        request: Duration::from_millis(100),
        ..LIMITS
    })
    .await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        get_status(&mut stream, "/slow").await,
        "HTTP/1.1 408 Request Timeout"
    );
    assert_eq!(get_status(&mut stream, "/").await, "HTTP/1.1 200 OK");
}

#[tokio::test]
async fn closes_connections_over_the_limit() {
    let (addr, _stop, _server) = start(Limits {
        max_connections: 1,
        ..LIMITS
    })
    .await;
    let mut first = TcpStream::connect(addr).await.unwrap();
    assert_eq!(get_status(&mut first, "/").await, "HTTP/1.1 200 OK");
    let mut second = TcpStream::connect(addr).await.unwrap();
    closed_within(&mut second, Duration::from_secs(3)).await;
    drop(first);
    // The permit returns once the first connection's task ends.
    let reopened = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let mut next = TcpStream::connect(addr).await.unwrap();
            if next.write_all(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n").await.is_ok() {
                let mut buf = vec![0; 64];
                if matches!(next.read(&mut buf).await, Ok(n) if n > 0) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(reopened.is_ok(), "no connection accepted after the first closed");
}

#[tokio::test]
async fn shutdown_drops_connections_after_the_drain() {
    let (addr, stop, server) = start(Limits {
        drain: Duration::from_millis(200),
        ..LIMITS
    })
    .await;
    let mut busy = TcpStream::connect(addr).await.unwrap();
    busy.write_all(b"GET /slow HTTP/1.1\r\nhost: x\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("serve returned after the drain")
        .unwrap();
    closed_within(&mut busy, Duration::from_secs(1)).await;
    assert!(TcpStream::connect(addr).await.is_err(), "listener still open");
}
