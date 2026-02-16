//! Application state management using Yew's use_reducer.

use std::rc::Rc;
use gloo_storage::Storage;
use yew::prelude::*;

use crate::schema::{InfrastructureTemplate, PangeaNamespace};

/// Global application state.
#[derive(Clone, PartialEq)]
pub struct AppState {
    /// API endpoint URL.
    pub endpoint: String,
    /// API authentication token.
    pub token: Option<String>,
    /// Currently selected namespace filter.
    pub selected_namespace: Option<String>,
    /// List of templates.
    pub templates: Vec<InfrastructureTemplate>,
    /// List of namespaces.
    pub namespaces: Vec<PangeaNamespace>,
    /// Currently viewing template.
    pub current_template: Option<InfrastructureTemplate>,
    /// Loading state.
    pub loading: bool,
    /// Error message.
    pub error: Option<String>,
}

impl Default for AppState {
    fn default() -> Self {
        let endpoint = gloo_storage::LocalStorage::get("pangea_endpoint")
            .unwrap_or_else(|_| "http://pangea.quero.local/graphql".to_string());
        let token = gloo_storage::LocalStorage::get("pangea_token").ok();

        Self {
            endpoint,
            token,
            selected_namespace: None,
            templates: Vec::new(),
            namespaces: Vec::new(),
            current_template: None,
            loading: false,
            error: None,
        }
    }
}

impl AppState {
    /// Save settings to browser storage.
    pub fn save_settings(&self) {
        let _ = gloo_storage::LocalStorage::set("pangea_endpoint", &self.endpoint);
        if let Some(ref token) = self.token {
            let _ = gloo_storage::LocalStorage::set("pangea_token", token);
        }
    }
}

/// Actions for modifying state.
pub enum Action {
    SetEndpoint(String),
    SetToken(Option<String>),
    SetNamespaceFilter(Option<String>),
    SetTemplates(Vec<InfrastructureTemplate>),
    SetNamespaces(Vec<PangeaNamespace>),
    SetCurrentTemplate(Option<InfrastructureTemplate>),
    SetLoading(bool),
    SetError(Option<String>),
    ClearError,
}

impl Reducible for AppState {
    type Action = Action;

    fn reduce(self: Rc<Self>, action: Self::Action) -> Rc<Self> {
        let mut state = (*self).clone();
        match action {
            Action::SetEndpoint(endpoint) => {
                state.endpoint = endpoint;
                state.save_settings();
            }
            Action::SetToken(token) => {
                state.token = token;
                state.save_settings();
            }
            Action::SetNamespaceFilter(ns) => {
                state.selected_namespace = ns;
            }
            Action::SetTemplates(templates) => {
                state.templates = templates;
                state.loading = false;
            }
            Action::SetNamespaces(namespaces) => {
                state.namespaces = namespaces;
            }
            Action::SetCurrentTemplate(template) => {
                state.current_template = template;
                state.loading = false;
            }
            Action::SetLoading(loading) => {
                state.loading = loading;
            }
            Action::SetError(error) => {
                state.error = error;
                state.loading = false;
            }
            Action::ClearError => {
                state.error = None;
            }
        }
        Rc::new(state)
    }
}

/// Context type for app state.
pub type AppStateContext = UseReducerHandle<AppState>;
