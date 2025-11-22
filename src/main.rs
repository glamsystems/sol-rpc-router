use axum::{
    body::{to_bytes, Body},
    extract::{Query, State},
    http::{Request, StatusCode, Uri},
    response::IntoResponse,
    routing::post,
    Router,
};
use axum::{
    extract::ConnectInfo,
    middleware::{self, Next},
    response::Response,
};
use dotenv::dotenv;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use serde::Deserialize;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber;

const MAX_BODY_SIZE: usize = 10 * 1024 * 1024; // 10 MB

#[derive(Clone)]
struct RpcMethod(String);

#[derive(Clone)]
struct AppState {
    client: Client<HttpsConnector<HttpConnector>, Body>,
    backend: String,
    api_keys: Vec<String>,
}

#[derive(Deserialize)]
struct Params {
    #[serde(rename = "api-key")]
    api_key: Option<String>,
}

pub async fn extract_rpc_method(mut req: Request<Body>, next: Next) -> Response {
    // Read body, extract "method" field, then reconstruct the request
    let (parts, body) = req.into_parts();
    let body_bytes = match to_bytes(body, MAX_BODY_SIZE).await {
        Ok(bytes) => bytes,
        Err(_) => {
            // If body read fails, pass empty body downstream
            return next.run(Request::from_parts(parts, Body::empty())).await;
        }
    };

    // Try to extract "method" from JSON
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
        if let Some(method) = json.get("method").and_then(|m| m.as_str()) {
            req = Request::from_parts(parts, Body::from(body_bytes.clone()));
            req.extensions_mut().insert(RpcMethod(method.to_string()));
            return next.run(req).await;
        }
    }

    // If no method found, reconstruct request with original body
    req = Request::from_parts(parts, Body::from(body_bytes));
    next.run(req).await
}

pub async fn log_requests(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let rpc_method = req.extensions().get::<RpcMethod>().cloned();

    let start = std::time::Instant::now();
    let response = next.run(req).await;
    let duration = start.elapsed();

    match rpc_method {
        Some(RpcMethod(m)) => info!(
            "{} {} {} {:?} rpc_method={}",
            method, path, addr, duration, m
        ),
        None => info!("{} {} {} {:?}", method, path, addr, duration),
    }

    response
}

async fn proxy(
    State(state): State<Arc<AppState>>,
    Query(params): Query<Params>,
    mut req: Request<Body>,
) -> impl IntoResponse {
    match params.api_key {
        Some(ref key) if state.api_keys.contains(key) => {}
        Some(ref key) => {
            info!("API key '{}' is invalid", key);
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
        None => {
            info!("No API key provided");
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    }

    // Rebuild URI (remove ?api-key=... from request, but preserve backend's api-key)
    let request_path_and_query = req
        .uri()
        .path_and_query()
        .map(|x| x.as_str())
        .unwrap_or("/");

    // Remove api-key from the incoming request's query parameters
    let cleaned_request_path = if let Some(pos) = request_path_and_query.find("?api-key=") {
        &request_path_and_query[..pos]
    } else {
        request_path_and_query
    };

    // Avoid double slashes and trailing slashes
    let uri_string = if cleaned_request_path == "/" {
        // For root path requests, don't add trailing slash
        state.backend.trim_end_matches('/').to_string()
    } else if state.backend.ends_with('/') && cleaned_request_path.starts_with('/') {
        // Avoid double slashes
        format!("{}{}", state.backend, &cleaned_request_path[1..])
    } else {
        format!("{}{}", state.backend, cleaned_request_path)
    };
    let parsed_uri = uri_string.parse::<Uri>().unwrap();

    // Update Host header to match the backend
    if let Some(host) = parsed_uri.host() {
        let host_value = if let Some(port) = parsed_uri.port_u16() {
            format!("{}:{}", host, port)
        } else {
            host.to_string()
        };
        req.headers_mut()
            .insert("host", host_value.parse().unwrap());
    }

    *req.uri_mut() = parsed_uri;

    // Forward request
    match state.client.request(req).await {
        Ok(resp) => resp.into_response(),
        Err(err) => {
            info!("Backend request failed: {} (error type: {:?})", err, err);
            (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", err)).into_response()
        }
    }
}

#[tokio::main]
async fn main() {
    // Load environment variables from .env file
    dotenv().ok();

    tracing_subscriber::fmt::init();

    // Read configuration from environment variables
    let backend = env::var("BACKEND_URL").expect("BACKEND_URL environment variable must be set");
    let api_keys_str = env::var("API_KEYS").expect("API_KEYS environment variable must be set");
    let api_keys: Vec<String> = api_keys_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if api_keys.is_empty() {
        panic!("API_KEYS must contain at least one valid API key");
    }

    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "28899".to_string())
        .parse()
        .expect("PORT must be a valid number");

    let https = HttpsConnector::new();
    let state = Arc::new(AppState {
        client: Client::builder(hyper_util::rt::TokioExecutor::new()).build(https),
        backend,
        api_keys,
    });

    let app = Router::new()
        .route("/", post(proxy))
        .route("/*path", post(proxy))
        .with_state(state)
        .layer(middleware::from_fn(log_requests))
        .layer(middleware::from_fn(extract_rpc_method));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening on http://{}", addr);

    axum::serve(
        tokio::net::TcpListener::bind(addr).await.unwrap(),
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}
