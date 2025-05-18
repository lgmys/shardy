enum Command {
    /// Handles searches coming in from the ApiGateway
    Searcher,
    /// Processess
    Indexer,
    ApiGateway,
}

/// Multitenancy is required and will be implemented through index prefix
/// Install axum and tokio,
/// Install tantivy,
/// Install minio and minio client
use axum::{
    Router,
    routing::{get, post},
};

#[tokio::main]
async fn main() {
    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/", get(root));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// basic handler that responds with a static string
async fn root() -> &'static str {
    "Hello, World!"
}
