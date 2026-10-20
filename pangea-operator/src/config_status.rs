use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{Api, ObjectMeta, Patch, PatchParams};
use kube::Client;
use serde::{Deserialize, Serialize};

use crate::config::FileTier;
use crate::network::{NetworkResolution, ResolvedDevice, ResolvedService};

pub const STATUS_CONFIG_MAP: &str = "pangea-operator-config-status";
pub const STATUS_KEY: &str = "status.yaml";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum EntryStatus {
    Applied,
    Drifted { fields: Vec<String> },
    Refused { reason: String },
    Blind { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observation<T> {
    Seen(T),
    Blind(String),
}

pub trait NetworkObserver {
    fn observe_service(&self, desired: &ResolvedService) -> Observation<ResolvedService>;
    fn observe_device(&self, desired: &ResolvedDevice) -> Observation<ResolvedDevice>;
}

pub struct NoObserver;

impl NetworkObserver for NoObserver {
    fn observe_service(&self, _: &ResolvedService) -> Observation<ResolvedService> {
        Observation::Blind("no service observer on this operator".to_string())
    }

    fn observe_device(&self, _: &ResolvedDevice) -> Observation<ResolvedDevice> {
        Observation::Blind("no device observer on this operator".to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigStatus {
    pub file: FileTier,
    pub services: BTreeMap<String, EntryStatus>,
    pub devices: BTreeMap<String, BTreeMap<String, EntryStatus>>,
}

fn differing<T: Serialize>(desired: &T, observed: &T) -> Vec<String> {
    let (Ok(serde_json::Value::Object(d)), Ok(serde_json::Value::Object(o))) = (
        serde_json::to_value(desired),
        serde_json::to_value(observed),
    ) else {
        return vec!["<unserializable>".to_string()];
    };
    let mut fields: Vec<String> = d
        .iter()
        .filter(|(k, v)| o.get(*k) != Some(*v))
        .map(|(k, _)| k.clone())
        .collect();
    fields.sort();
    fields
}

fn judge<T: Serialize>(desired: &T, observation: Observation<T>) -> EntryStatus {
    match observation {
        Observation::Blind(reason) => EntryStatus::Blind { reason },
        Observation::Seen(observed) => {
            let fields = differing(desired, &observed);
            if fields.is_empty() {
                EntryStatus::Applied
            } else {
                EntryStatus::Drifted { fields }
            }
        }
    }
}

impl ConfigStatus {
    #[must_use]
    pub fn observe(
        file: FileTier,
        network: &NetworkResolution,
        observer: &dyn NetworkObserver,
    ) -> Self {
        let services = network
            .services
            .iter()
            .map(|(name, r)| {
                let status = match r {
                    Err(refusal) => EntryStatus::Refused {
                        reason: refusal.0.clone(),
                    },
                    Ok(desired) => judge(desired, observer.observe_service(desired)),
                };
                (name.clone(), status)
            })
            .collect();
        let mut devices: BTreeMap<String, BTreeMap<String, EntryStatus>> = BTreeMap::new();
        for ((site, name), r) in &network.devices {
            let status = match r {
                Err(refusal) => EntryStatus::Refused {
                    reason: refusal.0.clone(),
                },
                Ok(desired) => judge(desired, observer.observe_device(desired)),
            };
            devices
                .entry(site.clone())
                .or_default()
                .insert(name.clone(), status);
        }
        Self {
            file,
            services,
            devices,
        }
    }

    pub fn to_config_map(&self, namespace: &str) -> Result<ConfigMap, serde_yaml::Error> {
        let body = serde_yaml::to_string(self)?;
        let labels = BTreeMap::from([
            (
                "app.kubernetes.io/name".to_string(),
                "pangea-operator".to_string(),
            ),
            (
                "app.kubernetes.io/component".to_string(),
                "config-status".to_string(),
            ),
        ]);
        let annotations =
            BTreeMap::from([("pleme.io/config-tier".to_string(), "status".to_string())]);
        Ok(ConfigMap {
            metadata: ObjectMeta {
                name: Some(STATUS_CONFIG_MAP.to_string()),
                namespace: Some(namespace.to_string()),
                labels: Some(labels),
                annotations: Some(annotations),
                ..Default::default()
            },
            data: Some(BTreeMap::from([(STATUS_KEY.to_string(), body)])),
            ..Default::default()
        })
    }

    pub async fn publish(&self, client: Client, namespace: &str) -> Result<(), crate::Error> {
        let cm = self
            .to_config_map(namespace)
            .map_err(|e| crate::Error::Config(e.to_string()))?;
        let api: Api<ConfigMap> = Api::namespaced(client, namespace);
        match api.create(&Default::default(), &cm).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ae)) if ae.code == 409 => api
                .patch(
                    STATUS_CONFIG_MAP,
                    &PatchParams::apply("pangea-operator"),
                    &Patch::Merge(&cm),
                )
                .await
                .map(|_| ())
                .map_err(crate::Error::Kube),
            Err(e) => Err(crate::Error::Kube(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{DeviceEntry, NetworkConfig, Posture, ServiceEntry};

    struct Fake {
        drift_port: bool,
    }

    impl NetworkObserver for Fake {
        fn observe_service(&self, d: &ResolvedService) -> Observation<ResolvedService> {
            match d.name.as_str() {
                "dark" => Observation::Blind("unreachable".into()),
                _ if self.drift_port => {
                    let mut o = d.clone();
                    o.port += 1;
                    o.posture = Posture::PublicFunnel;
                    Observation::Seen(o)
                }
                _ => Observation::Seen(d.clone()),
            }
        }

        fn observe_device(&self, d: &ResolvedDevice) -> Observation<ResolvedDevice> {
            Observation::Seen(d.clone())
        }
    }

    fn network() -> NetworkConfig {
        let mut cfg = NetworkConfig::default();
        for (name, posture) in [("web", "tailnet"), ("dark", "lan"), ("bad", "nope")] {
            cfg.services.insert(
                name.into(),
                ServiceEntry {
                    node: Some("rio".into()),
                    posture: Some(posture.into()),
                    port: Some(8080),
                    ..Default::default()
                },
            );
        }
        cfg.devices.entry("house".into()).or_default().insert(
            "plug".into(),
            DeviceEntry {
                kind: Some("plug".into()),
                mac: Some("aa:bb:cc:dd:ee:ff".into()),
                address: None,
                posture: Some("lan".into()),
            },
        );
        cfg
    }

    fn file() -> FileTier {
        FileTier::Loaded {
            path: "/etc/pangea/config.yaml".into(),
        }
    }

    #[test]
    fn every_status_variant_is_reachable() {
        let r = network().resolve();
        let s = ConfigStatus::observe(file(), &r, &Fake { drift_port: false });
        assert_eq!(s.services["web"], EntryStatus::Applied);
        assert_eq!(
            s.services["dark"],
            EntryStatus::Blind {
                reason: "unreachable".into()
            }
        );
        assert!(matches!(s.services["bad"], EntryStatus::Refused { .. }));
        assert_eq!(s.devices["house"]["plug"], EntryStatus::Applied);

        let s = ConfigStatus::observe(file(), &r, &Fake { drift_port: true });
        assert_eq!(
            s.services["web"],
            EntryStatus::Drifted {
                fields: vec!["port".into(), "posture".into()]
            }
        );
    }

    #[test]
    fn without_an_observer_nothing_reads_as_applied() {
        let s = ConfigStatus::observe(file(), &network().resolve(), &NoObserver);
        assert!(matches!(s.services["web"], EntryStatus::Blind { .. }));
        assert!(matches!(s.services["bad"], EntryStatus::Refused { .. }));
        assert!(matches!(
            s.devices["house"]["plug"],
            EntryStatus::Blind { .. }
        ));
    }

    #[test]
    fn config_map_carries_the_status_as_yaml() {
        let s = ConfigStatus::observe(file(), &network().resolve(), &NoObserver);
        let cm = s.to_config_map("pangea-system").unwrap();
        assert_eq!(cm.metadata.name.as_deref(), Some(STATUS_CONFIG_MAP));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("pangea-system"));
        let body = &cm.data.unwrap()[STATUS_KEY];
        let back: ConfigStatus = serde_yaml::from_str(body).unwrap();
        assert_eq!(back, s);
        assert!(body.contains("state: refused"));
        assert!(body.contains("state: blind"));
    }
}
