#![forbid(unsafe_code)]

//! StorageService and validated database capabilities.

pub mod error;
pub mod generation;
pub mod service;

mod database;
#[allow(dead_code, reason = "used through the database bootstrap lifecycle")]
mod migration;
mod runtime;
#[allow(dead_code, reason = "used through the database bootstrap lifecycle")]
mod schema;
#[allow(dead_code, reason = "used through database bootstrap and recovery-session preparation")]
mod state;
