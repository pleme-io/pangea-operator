//! Template card component.

use yew::prelude::*;

use crate::schema::{InfrastructureTemplate, Phase};

/// Props for TemplateCard.
#[derive(Properties, PartialEq)]
pub struct TemplateCardProps {
    pub template: InfrastructureTemplate,
    #[prop_or_default]
    pub on_click: Callback<()>,
}

/// Template card component showing template summary.
#[function_component(TemplateCard)]
pub fn template_card(props: &TemplateCardProps) -> Html {
    let template = &props.template;
    let on_click = props.on_click.clone();

    let onclick = Callback::from(move |_| on_click.emit(()));

    let phase_class = template.phase.css_class();
    let namespace = template.namespace.clone().unwrap_or_default();
    let name = template.name.clone().unwrap_or_default();

    html! {
        <div class={classes!("template-card", phase_class)} {onclick}>
            <div class="template-card-header">
                <h3 class="template-name">{ &name }</h3>
                <span class={classes!("phase-badge", phase_class)}>
                    { format!("{}", template.phase) }
                </span>
            </div>
            <div class="template-card-body">
                <div class="template-info">
                    <span class="label">{ "Namespace:" }</span>
                    <span class="value">{ &namespace }</span>
                </div>
                <div class="template-info">
                    <span class="label">{ "Pangea NS:" }</span>
                    <span class="value">{ &template.pangea_namespace }</span>
                </div>
                <div class="resource-counts">
                    <span class="count added">{ format!("+{}", template.resource_counts.added) }</span>
                    <span class="count changed">{ format!("~{}", template.resource_counts.changed) }</span>
                    <span class="count destroyed">{ format!("-{}", template.resource_counts.destroyed) }</span>
                    <span class="count total">{ format!("={}", template.resource_counts.total) }</span>
                </div>
            </div>
            if template.phase == Phase::Failed {
                if let Some(ref error) = template.last_error {
                    <div class="template-error">
                        { error }
                    </div>
                }
            }
        </div>
    }
}
