//! Navigation bar component.

use yew::prelude::*;
use yew_router::prelude::*;

use crate::app::Route;

/// Navigation bar component.
#[function_component(NavBar)]
pub fn navbar() -> Html {
    html! {
        <nav class="navbar">
            <div class="navbar-brand">
                <Link<Route> to={Route::Dashboard} classes="navbar-logo">
                    { "Pangea" }
                </Link<Route>>
            </div>
            <div class="navbar-menu">
                <Link<Route> to={Route::Dashboard} classes="navbar-item">
                    { "Dashboard" }
                </Link<Route>>
                <Link<Route> to={Route::Templates} classes="navbar-item">
                    { "Templates" }
                </Link<Route>>
                <Link<Route> to={Route::Namespaces} classes="navbar-item">
                    { "Namespaces" }
                </Link<Route>>
                <Link<Route> to={Route::Settings} classes="navbar-item">
                    { "Settings" }
                </Link<Route>>
            </div>
        </nav>
    }
}
