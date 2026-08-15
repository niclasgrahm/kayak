//! The scripted transform.
//!
//! Split four ways, and the lines are the ones the rest of the crate already
//! draws: [`value`] is the bridge between a message and what a script sees,
//! [`source`] is where the text comes from, [`runner`] is the engine and the
//! sandbox, [`transform`] is the component. The declaration lives a crate away
//! in [`kayak_core::script`], for the reason the column mapping and the field
//! mapping do — it has to compile for wasm so the form can render it.
//!
//! [`runner`] is the one to read first: it is where the sandbox is, and every
//! part of that sandbox is load-bearing.

pub mod error;
pub mod runner;
pub mod source;
pub mod transform;
pub mod value;
