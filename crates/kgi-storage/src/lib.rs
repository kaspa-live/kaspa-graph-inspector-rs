#![forbid(unsafe_code)]

//! StorageService and validated database capabilities.

pub mod error;
pub mod generation;
pub mod operation;
pub mod service;

mod cache;
mod database;
mod identity;
#[allow(dead_code, reason = "used through the database bootstrap lifecycle")]
mod migration;
#[allow(dead_code, reason = "replacement control is consumed by the pending rebuild transaction")]
mod replacement;
mod runtime;
#[allow(dead_code, reason = "used through the database bootstrap lifecycle")]
mod schema;
#[allow(dead_code, reason = "used through database bootstrap and recovery-session preparation")]
mod state;
mod transaction;
