//! Templates page.

use yew::prelude::*;

use crate::client::PangeaClient;
use crate::components::error_banner::ErrorBanner;
use crate::components::loading::Loading;
use crate::components::template_card::TemplateCard;
use crate::state::{Action, AppStateContext};

/// Props for Templates page.
#[derive(Properties, PartialEq, Default)]
pub struct TemplatesProps {
    #[prop_or_default]
    pub selected_namespace: String,
    #[prop_or_default]
    pub selected_name: String,
}

/// Templates page component.
#[function_component(Templates)]
pub fn templates(_props: &TemplatesProps) -> Html {
    let state = use_context::<AppStateContext>().expect("AppStateContext not found");

    // Fetch templates
    {
        let state = state.clone();
        use_effect_with((), move |_| {
            let state = state.clone();
            wasm_bindgen_futures::spawn_local(async move {
                state.dispatch(Action::SetLoading(true));

                let client = PangeaClient::new(&state.endpoint, state.token.clone());

                match client.list_templates(state.selected_namespace.clone(), None).await {
                    Ok(templates) => {
                        state.dispatch(Action::SetTemplates(templates));
                    }
                    Err(e) => {
                        state.dispatch(Action::SetError(Some(e)));
                    }
                }
            });
            || ()
        });
    }

    // Namespace filter change
    let on_namespace_change = {
        let state = state.clone();
        Callback::from(move |e: Event| {
            let target: web_sys::HtmlSelectElement = e.target_unchecked_into();
            let value = target.value();
            let ns = if value.is_empty() { None } else { Some(value) };
            state.dispatch(Action::SetNamespaceFilter(ns));
        })
    };

    // Error dismissal
    let dismiss_error = {
        let state = state.clone();
        Callback::from(move |_| {
            state.dispatch(Action::ClearError);
        })
    };

    html! {
        <div class="templates-page">
            <div class="page-header">
                <h1>{ "Templates" }</h1>
                <div class="filters">
                    <select onchange={on_namespace_change}>
                        <option value="">{ "All Namespaces" }</option>
                        { for state.namespaces.iter().map(|ns| {
                            let name = ns.name.clone().unwrap_or_default();
                            html! {
                                <option value={name.clone()}>{ &name }</option>
                            }
                        })}
                    </select>
                </div>
            </div>

            if let Some(ref error) = state.error {
                <ErrorBanner message={error.clone()} on_dismiss={dismiss_error} />
            }

            if state.loading {
                <Loading />
            } else if state.templates.is_empty() {
                <div class="empty-state">
                    <h2>{ "No Templates Found" }</h2>
                    <p>{ "No infrastructure templates match your current filters." }</p>
                </div>
            } else {
                <div class="template-grid">
                    { for state.templates.iter().map(|t| {
                        html! { <TemplateCard template={t.clone()} /> }
                    })}
                </div>
            }
        </div>
    }
}
