use crate::controller::template::secret_value::read_secret_key;
use crate::controller::ControllerState;
use crate::crd::InfrastructureTemplate;
use crate::error::Result;
use kube::ResourceExt;

pub async fn resolve_secret_variables(
    template: &InfrastructureTemplate,
    state: &ControllerState,
) -> Result<Vec<(String, serde_json::Value)>> {
    if template.spec.secret_variables.is_empty() {
        return Ok(Vec::new());
    }
    let namespace = template
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let mut out = Vec::with_capacity(template.spec.secret_variables.len());
    for v in &template.spec.secret_variables {
        let r = &v.value_from.secret_key_ref;
        let value = read_secret_key(
            &state.client,
            &namespace,
            &r.name,
            &r.key,
            &format!("secretVariables[{}]", v.name),
        )
        .await?;
        out.push((v.name.clone(), serde_json::Value::String(value)));
    }
    Ok(out)
}
