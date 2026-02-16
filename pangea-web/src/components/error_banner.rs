//! Error banner component.

use yew::prelude::*;

/// Props for ErrorBanner.
#[derive(Properties, PartialEq)]
pub struct ErrorBannerProps {
    pub message: String,
    #[prop_or_default]
    pub on_dismiss: Callback<()>,
}

/// Error banner component.
#[function_component(ErrorBanner)]
pub fn error_banner(props: &ErrorBannerProps) -> Html {
    let on_dismiss = props.on_dismiss.clone();
    let onclick = Callback::from(move |_| on_dismiss.emit(()));

    html! {
        <div class="error-banner">
            <span class="error-icon">{ "!" }</span>
            <span class="error-message">{ &props.message }</span>
            <button class="dismiss-button" {onclick}>{ "x" }</button>
        </div>
    }
}
