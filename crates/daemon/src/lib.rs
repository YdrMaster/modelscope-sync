/// HTTP request handlers for the daemon REST API.
pub mod handlers;
/// Axum server bootstrap and graceful shutdown.
pub mod server;
/// Shared application state, configuration, and task tracking.
pub mod state;
/// Background task spawning and progress tracking.
pub mod tasks;
