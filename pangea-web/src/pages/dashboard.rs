//! Dashboard page.

use yew::prelude::*;

use crate::client::PangeaClient;
use crate::components::error_banner::ErrorBanner;
use crate::components::loading::Loading;
use crate::components::template_card::TemplateCard;
use crate::schema::Phase;
use crate::state::{Action, AppStateContext};

/// Dashboard page component.
#[function_component(Dashboard)]
pub fn dashboard() -> Html {
    let state = use_context::<AppStateContext>().expect("AppStateContext not found");

    // Fetch data on mount
    {
        let state = state.clone();
        use_effect_with((), move |_| {
            let state = state.clone();
            wasm_bindgen_futures::spawn_local(async move {
                state.dispatch(Action::SetLoading(true));

                let client = PangeaClient::new(&state.endpoint, state.token.clone());

                match client.list_templates(None, None).await {
                    Ok(templates) => {
                        state.dispatch(Action::SetTemplates(templates));
                    }
                    Err(e) => {
                        state.dispatch(Action::SetError(Some(e)));
                    }
                }

                match client.list_namespaces().await {
                    Ok(namespaces) => {
                        state.dispatch(Action::SetNamespaces(namespaces));
                    }
                    Err(e) => {
                        log::warn!("Failed to fetch namespaces: {}", e);
                    }
                }
            });
            || ()
        });
    }

    // Calculate stats
    let total_templates = state.templates.len();
    let ready_count = state.templates.iter().filter(|t| t.phase == Phase::Ready).count();
    let failed_count = state.templates.iter().filter(|t| t.phase == Phase::Failed).count();
    let drifted_count = state.templates.iter().filter(|t| t.phase == Phase::Drifted).count();
    let in_progress = state.templates.iter().filter(|t| {
        matches!(t.phase, Phase::Compiling | Phase::Planning | Phase::Applying | Phase::Initializing)
    }).count();

    // Error dismissal
    let dismiss_error = {
        let state = state.clone();
        Callback::from(move |_| {
            state.dispatch(Action::ClearError);
        })
    };

    html! {
        <div class="dashboard">
            <h1>{ "Dashboard" }</h1>

            if let Some(ref error) = state.error {
                <ErrorBanner message={error.clone()} on_dismiss={dismiss_error} />
            }

            if state.loading {
                <Loading />
            } else {
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-value">{ total_templates }</div>
                        <div class="stat-label">{ "Total Templates" }</div>
                    </div>
                    <div class="stat-card stat-ready">
                        <div class="stat-value">{ ready_count }</div>
                        <div class="stat-label">{ "Ready" }</div>
                    </div>
                    <div class="stat-card stat-failed">
                        <div class="stat-value">{ failed_count }</div>
                        <div class="stat-label">{ "Failed" }</div>
                    </div>
                    <div class="stat-card stat-drifted">
                        <div class="stat-value">{ drifted_count }</div>
                        <div class="stat-label">{ "Drifted" }</div>
                    </div>
                    <div class="stat-card stat-progress">
                        <div class="stat-value">{ in_progress }</div>
                        <div class="stat-label">{ "In Progress" }</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">{ state.namespaces.len() }</div>
                        <div class="stat-label">{ "Namespaces" }</div>
                    </div>
                </div>

                <h2>{ "Recent Templates" }</h2>
                <div class="template-grid">
                    { for state.templates.iter().take(6).map(|t| {
                        html! { <TemplateCard template={t.clone()} /> }
                    })}
                </div>
            }
        </div>
    }
}
