//! Namespaces page.

use yew::prelude::*;

use crate::client::PangeaClient;
use crate::components::error_banner::ErrorBanner;
use crate::components::loading::Loading;
use crate::state::{Action, AppStateContext};

/// Namespaces page component.
#[function_component(Namespaces)]
pub fn namespaces() -> Html {
    let state = use_context::<AppStateContext>().expect("AppStateContext not found");

    // Fetch namespaces
    {
        let state = state.clone();
        use_effect_with((), move |_| {
            let state = state.clone();
            wasm_bindgen_futures::spawn_local(async move {
                state.dispatch(Action::SetLoading(true));

                let client = PangeaClient::new(&state.endpoint, state.token.clone());

                match client.list_namespaces().await {
                    Ok(namespaces) => {
                        state.dispatch(Action::SetNamespaces(namespaces));
                        state.dispatch(Action::SetLoading(false));
                    }
                    Err(e) => {
                        state.dispatch(Action::SetError(Some(e)));
                    }
                }
            });
            || ()
        });
    }

    // Error dismissal
    let dismiss_error = {
        let state = state.clone();
        Callback::from(move |_| {
            state.dispatch(Action::ClearError);
        })
    };

    html! {
        <div class="namespaces-page">
            <h1>{ "Namespaces" }</h1>

            if let Some(ref error) = state.error {
                <ErrorBanner message={error.clone()} on_dismiss={dismiss_error} />
            }

            if state.loading {
                <Loading />
            } else if state.namespaces.is_empty() {
                <div class="empty-state">
                    <h2>{ "No Namespaces Found" }</h2>
                    <p>{ "No Pangea namespaces have been configured yet." }</p>
                </div>
            } else {
                <div class="namespaces-grid">
                    { for state.namespaces.iter().map(|ns| {
                        let name = ns.name.clone().unwrap_or_default();
                        let ready_class = if ns.is_ready { "ready" } else { "not-ready" };
                        html! {
                            <div class={classes!("namespace-card", ready_class)}>
                                <div class="namespace-header">
                                    <h3>{ &name }</h3>
                                    <span class={classes!("status-badge", ready_class)}>
                                        { if ns.is_ready { "Ready" } else { "Not Ready" } }
                                    </span>
                                </div>
                                <div class="namespace-body">
                                    <div class="namespace-info">
                                        <span class="label">{ "Backend:" }</span>
                                        <span class="value">{ &ns.backend_type }</span>
                                    </div>
                                    <div class="namespace-info">
                                        <span class="label">{ "Database:" }</span>
                                        <span class="value">
                                            { ns.database_host.clone().unwrap_or_else(|| "-".to_string()) }
                                        </span>
                                    </div>
                                    <div class="namespace-info">
                                        <span class="label">{ "Schema:" }</span>
                                        <span class="value">
                                            { ns.schema_name.clone().unwrap_or_else(|| "-".to_string()) }
                                        </span>
                                    </div>
                                    <div class="namespace-info">
                                        <span class="label">{ "Templates:" }</span>
                                        <span class="value">{ ns.template_count }</span>
                                    </div>
                                </div>
                            </div>
                        }
                    })}
                </div>
            }
        </div>
    }
}
