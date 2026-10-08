#![forbid(unsafe_code)]

//! StorageService and validated database capabilities.

pub mod error;
pub mod generation;

#[allow(dead_code, reason = "wired into the permanent service in the next lifecycle point")]
mod database;
#[allow(dead_code, reason = "used through the database bootstrap lifecycle")]
mod migration;
#[allow(dead_code, reason = "used through the database bootstrap lifecycle")]
mod schema;
#[allow(dead_code, reason = "used through database bootstrap and recovery-session preparation")]
mod state;
