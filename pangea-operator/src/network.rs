use std::collections::BTreeMap;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Posture {
    Host,
    Lan,
    Sites,
    TailnetTag,
    Tailnet,
    Shared,
    PublicFunnel,
    PublicDomain,
}

impl Posture {
    pub const ALL: [Posture; 8] = [
        Posture::Host,
        Posture::Lan,
        Posture::Sites,
        Posture::TailnetTag,
        Posture::Tailnet,
        Posture::Shared,
        Posture::PublicFunnel,
        Posture::PublicDomain,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Posture::Host => "host",
            Posture::Lan => "lan",
            Posture::Sites => "sites",
            Posture::TailnetTag => "tailnet-tag",
            Posture::Tailnet => "tailnet",
            Posture::Shared => "shared",
            Posture::PublicFunnel => "public-funnel",
            Posture::PublicDomain => "public-domain",
        }
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.as_str() == raw)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServicePatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posture: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub names: Option<Vec<String>>,
}

impl ServicePatch {
    fn overlay(&mut self, above: &ServicePatch) {
        if above.posture.is_some() {
            self.posture.clone_from(&above.posture);
        }
        if above.port.is_some() {
            self.port = above.port;
        }
        if above.upstream.is_some() {
            self.upstream.clone_from(&above.upstream);
        }
        if above.names.is_some() {
            self.names.clone_from(&above.names);
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkTier {
    #[serde(default)]
    pub defaults: ServicePatch,
    #[serde(default)]
    pub services: BTreeMap<String, ServicePatch>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeTier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
    #[serde(default)]
    pub defaults: ServicePatch,
    #[serde(default)]
    pub services: BTreeMap<String, ServicePatch>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posture: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub names: Option<Vec<String>>,
}

impl ServiceEntry {
    fn as_patch(&self) -> ServicePatch {
        ServicePatch {
            posture: self.posture.clone(),
            port: self.port,
            upstream: self.upstream.clone(),
            names: self.names.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mac: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posture: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    #[serde(default)]
    pub fleet: NetworkTier,
    #[serde(default)]
    pub sites: BTreeMap<String, NetworkTier>,
    #[serde(default)]
    pub nodes: BTreeMap<String, NodeTier>,
    #[serde(default)]
    pub services: BTreeMap<String, ServiceEntry>,
    #[serde(default)]
    pub devices: BTreeMap<String, BTreeMap<String, DeviceEntry>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedService {
    pub name: String,
    pub node: String,
    pub site: Option<String>,
    pub posture: Posture,
    pub port: u16,
    pub upstream: Option<String>,
    pub names: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedDevice {
    pub site: String,
    pub name: String,
    pub kind: String,
    pub mac: String,
    pub address: Option<IpAddr>,
    pub posture: Posture,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal(pub String);

impl Refusal {
    fn new(parts: &[&str]) -> Self {
        Self(parts.concat())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NetworkResolution {
    pub services: BTreeMap<String, Result<ResolvedService, Refusal>>,
    pub devices: BTreeMap<(String, String), Result<ResolvedDevice, Refusal>>,
}

fn parse_posture(raw: Option<&str>) -> Result<Posture, Refusal> {
    let raw = raw.ok_or_else(|| Refusal::new(&["no posture at any tier"]))?;
    Posture::parse(raw).ok_or_else(|| {
        Refusal::new(&[
            "unknown posture '",
            raw,
            "' (host|lan|sites|tailnet-tag|tailnet|shared|public-funnel|public-domain)",
        ])
    })
}

fn normalize_mac(raw: &str) -> Option<String> {
    let octets: Vec<&str> = raw.split([':', '-']).collect();
    let well_formed = octets.len() == 6
        && octets
            .iter()
            .all(|o| o.len() == 2 && o.chars().all(|c| c.is_ascii_hexdigit()));
    well_formed.then(|| octets.join(":").to_ascii_lowercase())
}

impl NetworkConfig {
    #[must_use]
    pub fn effective_patch(
        &self,
        name: &str,
        entry: &ServiceEntry,
        site: Option<&str>,
    ) -> ServicePatch {
        let mut patch = ServicePatch::default();
        patch.overlay(&self.fleet.defaults);
        if let Some(p) = self.fleet.services.get(name) {
            patch.overlay(p);
        }
        if let Some(tier) = site.and_then(|s| self.sites.get(s)) {
            patch.overlay(&tier.defaults);
            if let Some(p) = tier.services.get(name) {
                patch.overlay(p);
            }
        }
        if let Some(tier) = entry.node.as_deref().and_then(|n| self.nodes.get(n)) {
            patch.overlay(&tier.defaults);
            if let Some(p) = tier.services.get(name) {
                patch.overlay(p);
            }
        }
        patch.overlay(&entry.as_patch());
        patch
    }

    fn resolve_service(
        &self,
        name: &str,
        entry: &ServiceEntry,
    ) -> Result<ResolvedService, Refusal> {
        let node = entry
            .node
            .clone()
            .ok_or_else(|| Refusal::new(&["no node"]))?;
        let node_site = self.nodes.get(&node).and_then(|t| t.site.clone());
        let site = match (entry.site.clone(), node_site) {
            (Some(own), Some(of_node)) if own != of_node => {
                return Err(Refusal::new(&[
                    "site '",
                    &own,
                    "' disagrees with node '",
                    &node,
                    "' site '",
                    &of_node,
                    "'",
                ]))
            }
            (own, of_node) => own.or(of_node),
        };
        let patch = self.effective_patch(name, entry, site.as_deref());
        let posture = parse_posture(patch.posture.as_deref())?;
        let port = match patch.port {
            Some(p) if p > 0 => p,
            Some(_) => return Err(Refusal::new(&["port 0"])),
            None => return Err(Refusal::new(&["no port at any tier"])),
        };
        Ok(ResolvedService {
            name: name.to_string(),
            node,
            site,
            posture,
            port,
            upstream: patch.upstream,
            names: patch.names.unwrap_or_default(),
        })
    }

    fn resolve_device(
        site: &str,
        name: &str,
        entry: &DeviceEntry,
    ) -> Result<ResolvedDevice, Refusal> {
        let kind = entry
            .kind
            .clone()
            .ok_or_else(|| Refusal::new(&["no kind"]))?;
        let raw_mac = entry
            .mac
            .as_deref()
            .ok_or_else(|| Refusal::new(&["no mac"]))?;
        let mac = normalize_mac(raw_mac)
            .ok_or_else(|| Refusal::new(&["malformed mac '", raw_mac, "'"]))?;
        let address = match entry.address.as_deref() {
            None => None,
            Some(raw) => Some(
                raw.parse::<IpAddr>()
                    .map_err(|_| Refusal::new(&["malformed address '", raw, "'"]))?,
            ),
        };
        let posture = parse_posture(entry.posture.as_deref())?;
        Ok(ResolvedDevice {
            site: site.to_string(),
            name: name.to_string(),
            kind,
            mac,
            address,
            posture,
        })
    }

    #[must_use]
    pub fn resolve(&self) -> NetworkResolution {
        let mut services: BTreeMap<String, Result<ResolvedService, Refusal>> = self
            .services
            .iter()
            .map(|(name, entry)| (name.clone(), self.resolve_service(name, entry)))
            .collect();
        let mut claims: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for svc in services.values().flatten() {
            for n in &svc.names {
                claims.entry(n.clone()).or_default().push(svc.name.clone());
            }
        }
        for (claimed, owners) in claims.iter().filter(|(_, o)| o.len() > 1) {
            let others = owners.join(",");
            for owner in owners {
                services.insert(
                    owner.clone(),
                    Err(Refusal::new(&["name '", claimed, "' claimed by ", &others])),
                );
            }
        }

        let mut devices: BTreeMap<(String, String), Result<ResolvedDevice, Refusal>> =
            BTreeMap::new();
        for (site, entries) in &self.devices {
            for (name, entry) in entries {
                devices.insert(
                    (site.clone(), name.clone()),
                    Self::resolve_device(site, name, entry),
                );
            }
        }
        let mut macs: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
        for d in devices.values().flatten() {
            macs.entry((d.site.clone(), d.mac.clone()))
                .or_default()
                .push(d.name.clone());
        }
        for ((site, mac), owners) in macs.iter().filter(|(_, o)| o.len() > 1) {
            let others = owners.join(",");
            for owner in owners {
                devices.insert(
                    (site.clone(), owner.clone()),
                    Err(Refusal::new(&["mac ", mac, " shared with ", &others])),
                );
            }
        }
        NetworkResolution { services, devices }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patch(posture: Option<&str>, port: Option<u16>) -> ServicePatch {
        ServicePatch {
            posture: posture.map(str::to_string),
            port,
            upstream: None,
            names: None,
        }
    }

    fn tiered(mask: u8) -> (NetworkConfig, Option<&'static str>) {
        let labels = [
            "fleet",
            "fleet-svc",
            "site",
            "site-svc",
            "node",
            "node-svc",
            "service",
        ];
        let mut cfg = NetworkConfig::default();
        let mut entry = ServiceEntry {
            node: Some("plo".into()),
            port: Some(80),
            ..Default::default()
        };
        let mut expected = None;
        let mut site = NetworkTier::default();
        let mut node = NodeTier {
            site: Some("house".into()),
            ..Default::default()
        };
        for (i, label) in labels.iter().enumerate() {
            if mask & (1 << i) == 0 {
                continue;
            }
            let p = Posture::ALL[i].as_str();
            expected = Some(p);
            let one = patch(Some(p), None);
            match *label {
                "fleet" => cfg.fleet.defaults = one,
                "fleet-svc" => {
                    cfg.fleet.services.insert("web".into(), one);
                }
                "site" => site.defaults = one,
                "site-svc" => {
                    site.services.insert("web".into(), one);
                }
                "node" => node.defaults = one,
                "node-svc" => {
                    node.services.insert("web".into(), one);
                }
                _ => entry.posture = Some(p.to_string()),
            }
        }
        cfg.sites.insert("house".into(), site);
        cfg.nodes.insert("plo".into(), node);
        cfg.services.insert("web".into(), entry);
        (cfg, expected)
    }

    #[test]
    fn highest_setting_tier_wins_for_every_tier_subset() {
        for mask in 0u8..(1 << 7) {
            let (cfg, expected) = tiered(mask);
            let got = cfg.resolve().services.remove("web").unwrap();
            match expected {
                Some(p) => assert_eq!(got.unwrap().posture.as_str(), p, "mask {mask:07b}"),
                None => assert_eq!(got.unwrap_err().0, "no posture at any tier"),
            }
        }
    }

    #[test]
    fn fields_merge_independently_across_tiers() {
        let mut cfg = NetworkConfig::default();
        cfg.fleet.defaults = patch(Some("tailnet"), Some(443));
        cfg.sites.insert(
            "house".into(),
            NetworkTier {
                defaults: ServicePatch {
                    upstream: Some("http://127.0.0.1:8123".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        cfg.services.insert(
            "ha".into(),
            ServiceEntry {
                node: Some("plo".into()),
                site: Some("house".into()),
                posture: Some("lan".into()),
                names: Some(vec!["ha.pleme.internal".into()]),
                ..Default::default()
            },
        );
        let got = cfg.resolve().services.remove("ha").unwrap().unwrap();
        assert_eq!(got.posture, Posture::Lan);
        assert_eq!(got.port, 443);
        assert_eq!(got.upstream.as_deref(), Some("http://127.0.0.1:8123"));
        assert_eq!(got.site.as_deref(), Some("house"));
        assert_eq!(got.names, vec!["ha.pleme.internal".to_string()]);
    }

    #[test]
    fn a_bad_entry_is_refused_and_its_siblings_resolve() {
        let mut cfg = NetworkConfig::default();
        cfg.fleet.defaults = patch(None, Some(80));
        cfg.services.insert(
            "good".into(),
            ServiceEntry {
                node: Some("rio".into()),
                posture: Some("tailnet".into()),
                ..Default::default()
            },
        );
        cfg.services.insert(
            "typo".into(),
            ServiceEntry {
                node: Some("rio".into()),
                posture: Some("public".into()),
                ..Default::default()
            },
        );
        cfg.services
            .insert("homeless".into(), ServiceEntry::default());
        let r = cfg.resolve();
        assert!(r.services["good"].is_ok());
        assert!(r.services["typo"]
            .as_ref()
            .unwrap_err()
            .0
            .contains("unknown posture 'public'"));
        assert_eq!(r.services["homeless"].as_ref().unwrap_err().0, "no node");
    }

    #[test]
    fn site_disagreeing_with_node_is_refused() {
        let mut cfg = NetworkConfig::default();
        cfg.nodes.insert(
            "plo".into(),
            NodeTier {
                site: Some("house".into()),
                ..Default::default()
            },
        );
        cfg.services.insert(
            "x".into(),
            ServiceEntry {
                node: Some("plo".into()),
                site: Some("office".into()),
                posture: Some("lan".into()),
                port: Some(1),
                ..Default::default()
            },
        );
        assert!(cfg.resolve().services["x"]
            .as_ref()
            .unwrap_err()
            .0
            .contains("disagrees"));
    }

    #[test]
    fn a_name_claimed_twice_refuses_both_claimants_only() {
        let mut cfg = NetworkConfig::default();
        cfg.fleet.defaults = patch(Some("tailnet"), Some(80));
        for (svc, name) in [
            ("a", "x.pleme.internal"),
            ("b", "x.pleme.internal"),
            ("c", "c.pleme.internal"),
        ] {
            cfg.services.insert(
                svc.into(),
                ServiceEntry {
                    node: Some("rio".into()),
                    names: Some(vec![name.into()]),
                    ..Default::default()
                },
            );
        }
        let r = cfg.resolve();
        assert!(r.services["a"].is_err());
        assert!(r.services["b"].is_err());
        assert!(r.services["c"].is_ok());
    }

    #[test]
    fn devices_validate_per_entry() {
        let mut cfg = NetworkConfig::default();
        let site = cfg.devices.entry("house".into()).or_default();
        let dev = |mac: &str, addr: Option<&str>| DeviceEntry {
            kind: Some("sensor".into()),
            mac: Some(mac.into()),
            address: addr.map(str::to_string),
            posture: Some("lan".into()),
        };
        site.insert("ok".into(), dev("AA-BB-CC-DD-EE-01", Some("192.168.50.10")));
        site.insert("badmac".into(), dev("aa:bb", None));
        site.insert("badip".into(), dev("aa:bb:cc:dd:ee:02", Some("999.1.1.1")));
        site.insert("dup1".into(), dev("aa:bb:cc:dd:ee:03", None));
        site.insert("dup2".into(), dev("AA:BB:CC:DD:EE:03", None));
        let r = cfg.resolve();
        let get = |n: &str| r.devices[&("house".to_string(), n.to_string())].clone();
        assert_eq!(get("ok").unwrap().mac, "aa:bb:cc:dd:ee:01");
        assert!(get("badmac").unwrap_err().0.contains("malformed mac"));
        assert!(get("badip").unwrap_err().0.contains("malformed address"));
        assert!(get("dup1").unwrap_err().0.contains("shared with"));
        assert!(get("dup2").unwrap_err().0.contains("shared with"));
    }

    #[test]
    fn posture_names_roundtrip_through_serde() {
        for p in Posture::ALL {
            let y = serde_yaml::to_string(&p).unwrap();
            assert_eq!(y.trim(), p.as_str());
            assert_eq!(Posture::parse(p.as_str()), Some(p));
        }
    }
}
