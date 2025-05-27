use axum::response::IntoResponse;

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        todo!()
    }
}

pub struct AppError(pub anyhow::Error);
