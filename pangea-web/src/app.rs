//! Main application component with routing.

use yew::prelude::*;
use yew_router::prelude::*;

use crate::components::navbar::NavBar;
use crate::pages::{dashboard::Dashboard, namespaces::Namespaces, settings::Settings, templates::Templates};
use crate::state::{AppState, AppStateContext};

/// Application routes.
#[derive(Clone, Routable, PartialEq)]
pub enum Route {
    #[at("/")]
    Dashboard,
    #[at("/templates")]
    Templates,
    #[at("/templates/:namespace/:name")]
    TemplateDetail { namespace: String, name: String },
    #[at("/namespaces")]
    Namespaces,
    #[at("/settings")]
    Settings,
    #[not_found]
    #[at("/404")]
    NotFound,
}

/// Render the appropriate page for a route.
fn switch(routes: Route) -> Html {
    match routes {
        Route::Dashboard => html! { <Dashboard /> },
        Route::Templates => html! { <Templates /> },
        Route::TemplateDetail { namespace, name } => {
            html! { <Templates selected_namespace={namespace} selected_name={name} /> }
        }
        Route::Namespaces => html! { <Namespaces /> },
        Route::Settings => html! { <Settings /> },
        Route::NotFound => html! {
            <div class="not-found">
                <h1>{ "404 - Page Not Found" }</h1>
                <p>{ "The page you're looking for doesn't exist." }</p>
                <Link<Route> to={Route::Dashboard}>{ "Go to Dashboard" }</Link<Route>>
            </div>
        },
    }
}

/// Main application component.
#[function_component(App)]
pub fn app() -> Html {
    let state = use_reducer(AppState::default);

    html! {
        <ContextProvider<AppStateContext> context={state}>
            <BrowserRouter>
                <div class="app">
                    <NavBar />
                    <main class="main-content">
                        <Switch<Route> render={switch} />
                    </main>
                </div>
            </BrowserRouter>
        </ContextProvider<AppStateContext>>
    }
}
