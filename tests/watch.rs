use resp::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use redis_rust::{AofOptions, Config, ReplicaType, Server, Store};

async fn setup_test_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let store = Store::from_path(&None, &None).unwrap();

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let config = Config::new(
                port,
                None,
                None,
                AofOptions::default(),
                store.clone(),
                ReplicaType::Leader,
                None,
            );
            tokio::spawn(async move {
                let _ = Server::new(config, stream).handle_conn().await;
            });
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    port
}

async fn connect(port: u16) -> TcpStream {
    TcpStream::connect(("127.0.0.1", port)).await.unwrap()
}

fn cmd(parts: &[&str]) -> Value {
    Value::Array(parts.iter().map(|s| Value::Bulk(s.to_string())).collect())
}

async fn send(stream: &mut TcpStream, command: &Value) {
    stream.write_all(&command.encode()).await.unwrap();
}

async fn read_all(stream: &mut TcpStream) -> String {
    tokio::time::sleep(tokio::time::Duration::from_millis(80)).await;
    let mut result = Vec::new();
    let mut buf = vec![0u8; 8192];
    loop {
        match tokio::time::timeout(
            tokio::time::Duration::from_millis(80),
            stream.read(&mut buf),
        )
        .await
        {
            Ok(Ok(n)) if n > 0 => {
                result.extend_from_slice(&buf[..n]);
                if n < buf.len() {
                    break;
                }
            }
            _ => break,
        }
    }
    String::from_utf8_lossy(&result).to_string()
}

#[tokio::test]
async fn exec_aborts_when_watched_key_is_modified_by_another_client() {
    let port = setup_test_server().await;
    let mut a = connect(port).await;
    let mut b = connect(port).await;

    // Seed the key.
    send(&mut a, &cmd(&["SET", "k", "1"])).await;
    let _ = read_all(&mut a).await;

    // Client A watches k, then enters MULTI and queues a SET.
    send(&mut a, &cmd(&["WATCH", "k"])).await;
    send(&mut a, &cmd(&["MULTI"])).await;
    send(&mut a, &cmd(&["SET", "k", "from-a"])).await;
    let _ = read_all(&mut a).await;

    // Client B modifies k — should dirty client A's watch.
    send(&mut b, &cmd(&["SET", "k", "from-b"])).await;
    let _ = read_all(&mut b).await;

    // Client A EXEC must return NullArray (*-1\r\n).
    send(&mut a, &cmd(&["EXEC"])).await;
    let resp = read_all(&mut a).await;
    assert!(
        resp.contains("*-1\r\n"),
        "expected null array, got {resp:?}"
    );

    // Confirm k was not overwritten by the aborted transaction.
    send(&mut a, &cmd(&["GET", "k"])).await;
    let resp = read_all(&mut a).await;
    assert!(
        resp.contains("from-b"),
        "expected value from client B, got {resp:?}"
    );
}

#[tokio::test]
async fn exec_commits_when_watched_key_is_unchanged() {
    let port = setup_test_server().await;
    let mut a = connect(port).await;

    send(&mut a, &cmd(&["SET", "k", "1"])).await;
    let _ = read_all(&mut a).await;

    send(&mut a, &cmd(&["WATCH", "k"])).await;
    send(&mut a, &cmd(&["MULTI"])).await;
    send(&mut a, &cmd(&["SET", "k", "committed"])).await;
    send(&mut a, &cmd(&["EXEC"])).await;
    let _ = read_all(&mut a).await;

    send(&mut a, &cmd(&["GET", "k"])).await;
    let resp = read_all(&mut a).await;
    assert!(
        resp.contains("committed"),
        "expected committed value, got {resp:?}"
    );
}

#[tokio::test]
async fn exec_aborts_when_watched_key_is_modified_by_blpop() {
    let port = setup_test_server().await;
    let mut a = connect(port).await;
    let mut b = connect(port).await;

    // Seed a list with one element.
    send(&mut a, &cmd(&["RPUSH", "q", "first"])).await;
    let _ = read_all(&mut a).await;

    send(&mut a, &cmd(&["WATCH", "q"])).await;
    send(&mut a, &cmd(&["MULTI"])).await;
    send(&mut a, &cmd(&["RPUSH", "q", "from-a"])).await;
    let _ = read_all(&mut a).await;

    // Client B BLPOPs the list — this mutates q and must fire watchers.
    send(&mut b, &cmd(&["BLPOP", "q", "0"])).await;
    let _ = read_all(&mut b).await;

    send(&mut a, &cmd(&["EXEC"])).await;
    let resp = read_all(&mut a).await;
    assert!(
        resp.contains("*-1\r\n"),
        "expected null array after blpop mutation, got {resp:?}"
    );
}
