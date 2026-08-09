use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use verglas_lakekeeper::VerglasCachePublisher;

#[tokio::test]
async fn committed_table_mutation_reaches_every_cache_with_bearer_auth() {
    let mut urls = Vec::new();
    let mut servers = Vec::new();
    for _ in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        urls.push(format!("http://{address}/admin/catalog/events"));
        servers.push(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = socket.read(&mut buffer).await.expect("read");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(13).any(|part| part == b"\"updateTable\"") {
                    break;
                }
            }
            socket
                .write_all(
                    b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("respond");
            String::from_utf8(request).expect("request text")
        }));
    }

    let publisher = VerglasCachePublisher::new(urls, "event-token".to_owned()).expect("publisher");
    let event = serde_json::json!({
        "specversion": "1.0",
        "id": "commit-1",
        "source": "lakekeeper",
        "type": "updateTable"
    });
    publisher
        .publish("updateTable", &event)
        .await
        .expect("publish");

    for server in servers {
        let request = server.await.expect("server").to_ascii_lowercase();
        assert!(request.starts_with("post /admin/catalog/events http/1.1"));
        assert!(request.contains("authorization: bearer event-token"));
        assert!(request.contains("content-type: application/cloudevents+json"));
    }
}

#[tokio::test]
async fn read_events_do_not_touch_cache_endpoints() {
    let publisher = VerglasCachePublisher::new(
        vec!["http://127.0.0.1:1/admin/catalog/events".to_owned()],
        "event-token".to_owned(),
    )
    .expect("publisher");
    publisher
        .publish("loadTable", &serde_json::json!({ "type": "loadTable" }))
        .await
        .expect("filtered");
}

#[test]
fn incomplete_configuration_fails_instead_of_disabling_push() {
    assert!(VerglasCachePublisher::new(Vec::new(), "token".to_owned()).is_err());
    assert!(VerglasCachePublisher::new(vec!["http://cache".to_owned()], String::new()).is_err());
}
