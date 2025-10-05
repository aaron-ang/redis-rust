use std::sync::Arc;

use resp::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use codecrafters_redis::{Config, ReplicaType, Server, Store};

async fn setup_test_server() -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    let store = Arc::new(Store::from_path(&None, &None).unwrap());

    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let config = Config::new(port, None, None, store.clone(), ReplicaType::Leader, None);
            tokio::spawn(async move {
                let _ = Server::new(config, stream).handle_conn().await;
            });
        }
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    (port, handle)
}

fn build_command(parts: &[&str]) -> Value {
    Value::Array(parts.iter().map(|s| Value::Bulk(s.to_string())).collect())
}

async fn send_pipeline(stream: &mut TcpStream, commands: &[Value]) {
    for cmd in commands {
        stream.write_all(&cmd.encode()).await.unwrap();
    }
}

async fn send_raw(stream: &mut TcpStream, data: &str) {
    stream.write_all(data.as_bytes()).await.unwrap();
}

async fn read_response(stream: &mut TcpStream) -> String {
    // Wait a bit for responses to be written
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut result = Vec::new();
    let mut buf = vec![0u8; 65536];

    // Read in a loop until no more data is immediately available
    loop {
        match tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            stream.read(&mut buf),
        )
        .await
        {
            Ok(Ok(n)) if n > 0 => {
                result.extend_from_slice(&buf[..n]);
                // If we got a full buffer, there might be more
                if n < buf.len() {
                    // Short read, probably done
                    break;
                }
            }
            Ok(Ok(_)) => break,  // EOF
            Ok(Err(_)) => break, // Error
            Err(_) => break,     // Timeout - no more data available
        }
    }

    String::from_utf8_lossy(&result).to_string()
}

#[tokio::test]
async fn test_basic_pipelining() {
    let (port, _handle) = setup_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Send multiple SET commands in one batch
    let commands = vec![
        build_command(&["SET", "key1", "value1"]),
        build_command(&["SET", "key2", "value2"]),
        build_command(&["SET", "key3", "value3"]),
    ];

    send_pipeline(&mut stream, &commands).await;
    let response = read_response(&mut stream).await;

    // Should get 3 OK responses
    assert_eq!(response.matches("+OK\r\n").count(), 3);
}

#[tokio::test]
async fn test_pipelined_get_responses() {
    let (port, _handle) = setup_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // First SET some keys
    let setup = vec![
        build_command(&["SET", "foo", "bar"]),
        build_command(&["SET", "baz", "qux"]),
    ];
    send_pipeline(&mut stream, &setup).await;
    let _ = read_response(&mut stream).await;

    // Now pipeline GET commands
    let commands = vec![
        build_command(&["GET", "foo"]),
        build_command(&["GET", "baz"]),
        build_command(&["GET", "missing"]),
    ];

    send_pipeline(&mut stream, &commands).await;
    let response = read_response(&mut stream).await;

    // Should get: $3\r\nbar\r\n$3\r\nqux\r\n$-1\r\n
    assert!(response.contains("$3\r\nbar\r\n"));
    assert!(response.contains("$3\r\nqux\r\n"));
    assert!(response.contains("$-1\r\n")); // null for missing key
}

#[tokio::test]
async fn test_pipelining_preserves_order() {
    let (port, _handle) = setup_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Pipeline: SET, GET, INCR, GET
    let commands = vec![
        build_command(&["SET", "num", "10"]),
        build_command(&["GET", "num"]),
        build_command(&["INCR", "num"]),
        build_command(&["GET", "num"]),
    ];

    send_pipeline(&mut stream, &commands).await;
    let response = read_response(&mut stream).await;

    // Responses should be in order:
    // +OK\r\n (SET)
    // $2\r\n10\r\n (GET -> "10")
    // :11\r\n (INCR -> 11)
    // $2\r\n11\r\n (GET -> "11")

    let expected = "+OK\r\n$2\r\n10\r\n:11\r\n$2\r\n11\r\n";
    assert_eq!(response, expected);
}

#[tokio::test]
async fn test_partial_command_buffering() {
    let (port, _handle) = setup_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Send first part of a command (incomplete)
    let cmd = build_command(&["SET", "key", "value"]);
    let encoded = cmd.encode();
    let split_point = encoded.len() - 10; // Send all but last 10 bytes

    send_raw(
        &mut stream,
        &String::from_utf8_lossy(&encoded[..split_point]),
    )
    .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Send remaining part
    send_raw(
        &mut stream,
        &String::from_utf8_lossy(&encoded[split_point..]),
    )
    .await;

    let response = read_response(&mut stream).await;
    assert!(response.contains("+OK\r\n"));
}

#[tokio::test]
async fn test_mixed_commands_pipeline() {
    let (port, _handle) = setup_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Mix of different command types
    let commands = vec![
        build_command(&["PING"]),
        build_command(&["ECHO", "hello"]),
        build_command(&["SET", "x", "5"]),
        build_command(&["INCR", "x"]),
        build_command(&["PING"]),
    ];

    send_pipeline(&mut stream, &commands).await;
    let response = read_response(&mut stream).await;

    // Should get: PONG, hello, OK, 6, PONG
    assert!(response.contains("+PONG\r\n"));
    assert!(response.contains("$5\r\nhello\r\n"));
    assert!(response.contains("+OK\r\n"));
    assert!(response.contains(":6\r\n"));
}

#[tokio::test]
async fn test_large_pipeline() {
    let (port, _handle) = setup_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Send 100 SET commands in one pipeline
    let commands: Vec<Value> = (0..100)
        .map(|i| build_command(&["SET", &format!("key{i}"), &format!("value{i}")]))
        .collect();

    send_pipeline(&mut stream, &commands).await;

    // Wait for all responses
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let response = read_response(&mut stream).await;

    // Should get 100 OK responses
    assert_eq!(response.matches("+OK\r\n").count(), 100);
}

#[tokio::test]
async fn test_error_in_pipeline() {
    let (port, _handle) = setup_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Pipeline that exercises INCR
    let commands = vec![
        build_command(&["SET", "a", "1"]),
        build_command(&["INCR", "a"]),
        build_command(&["INCR", "a"]),
    ];

    send_pipeline(&mut stream, &commands).await;
    let response = read_response(&mut stream).await;

    // First SET succeeds
    assert!(response.contains("+OK\r\n"));
    // INCR on "1" succeeds -> 2
    assert!(response.contains(":2\r\n"));
    // Second INCR succeeds -> 3
    assert!(response.contains(":3\r\n"));
}
