//! Loading indicator component.

use yew::prelude::*;

/// Loading indicator.
#[function_component(Loading)]
pub fn loading() -> Html {
    html! {
        <div class="loading">
            <div class="loading-spinner"></div>
            <span>{ "Loading..." }</span>
        </div>
    }
}
