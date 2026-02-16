//! Settings page.

use yew::prelude::*;

use crate::state::{Action, AppStateContext};

/// Settings page component.
#[function_component(Settings)]
pub fn settings() -> Html {
    let state = use_context::<AppStateContext>().expect("AppStateContext not found");

    let endpoint_ref = use_node_ref();
    let token_ref = use_node_ref();

    // Save settings
    let on_save = {
        let state = state.clone();
        let endpoint_ref = endpoint_ref.clone();
        let token_ref = token_ref.clone();
        Callback::from(move |e: SubmitEvent| {
            e.prevent_default();

            if let Some(input) = endpoint_ref.cast::<web_sys::HtmlInputElement>() {
                state.dispatch(Action::SetEndpoint(input.value()));
            }

            if let Some(input) = token_ref.cast::<web_sys::HtmlInputElement>() {
                let value = input.value();
                let token = if value.is_empty() { None } else { Some(value) };
                state.dispatch(Action::SetToken(token));
            }

            log::info!("Settings saved");
        })
    };

    html! {
        <div class="settings-page">
            <h1>{ "Settings" }</h1>

            <form onsubmit={on_save} class="settings-form">
                <div class="form-group">
                    <label for="endpoint">{ "API Endpoint" }</label>
                    <input
                        type="url"
                        id="endpoint"
                        ref={endpoint_ref}
                        value={state.endpoint.clone()}
                        placeholder="http://pangea.quero.local/graphql"
                    />
                    <small>{ "The GraphQL API endpoint for the Pangea operator" }</small>
                </div>

                <div class="form-group">
                    <label for="token">{ "API Token" }</label>
                    <input
                        type="password"
                        id="token"
                        ref={token_ref}
                        value={state.token.clone().unwrap_or_default()}
                        placeholder="Enter API token"
                    />
                    <small>{ "Authentication token for API access" }</small>
                </div>

                <div class="form-actions">
                    <button type="submit" class="btn btn-primary">
                        { "Save Settings" }
                    </button>
                </div>
            </form>

            <div class="settings-info">
                <h2>{ "Current Configuration" }</h2>
                <dl>
                    <dt>{ "Endpoint" }</dt>
                    <dd>{ &state.endpoint }</dd>
                    <dt>{ "Token" }</dt>
                    <dd>{ if state.token.is_some() { "Set" } else { "Not set" } }</dd>
                </dl>
            </div>
        </div>
    }
}
