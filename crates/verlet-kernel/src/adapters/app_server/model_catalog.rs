//! Catalog seam between `model/list` and model-metadata sources (EMO-558).
//!
//! `model/list` composes its entries from this seam so the built-in snapshot
//! and models.dev refresh (EMO-561) can plug in behind it without touching
//! the RPC layer again.

// Seam is unwired until EMO-558 implements `model/list` over it; EMO-558
// removes this allow.
#![allow(dead_code)]

/// One selectable model as surfaced by `model/list`.
///
/// Auth status and the active flag are not part of the entry: the RPC layer
/// annotates them per request from the provider store and the live
/// [`super::ActiveModelSelection`].
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ModelCatalogEntry {
    pub(crate) provider_id: String,
    pub(crate) model_id: String,
    pub(crate) display_name: String,
    /// Total context window in tokens, when known.
    pub(crate) context_window: Option<u64>,
    /// Maximum output tokens per response, when known.
    pub(crate) max_output_tokens: Option<u64>,
}

/// Source of catalog entries consulted by `model/list`.
///
/// EMO-558 implements `model/list` over this trait with [`StaticModelCatalog`];
/// EMO-561 swaps the data behind it (built-in snapshot overlaid by the cached
/// models.dev refresh) without changing this surface.
pub(crate) trait ModelCatalogSource: Send + Sync {
    /// Every known model, in source order. The RPC layer owns ordering,
    /// dedup by (provider, model), and per-request annotation.
    fn entries(&self) -> Vec<ModelCatalogEntry>;
}

/// Fixed entries derived from the launch configuration and the provider
/// metadata store, until EMO-561 lands real catalog data.
pub(crate) struct StaticModelCatalog {
    entries: Vec<ModelCatalogEntry>,
}

impl StaticModelCatalog {
    pub(crate) fn new(entries: Vec<ModelCatalogEntry>) -> Self {
        Self { entries }
    }
}

impl ModelCatalogSource for StaticModelCatalog {
    fn entries(&self) -> Vec<ModelCatalogEntry> {
        self.entries.clone()
    }
}
