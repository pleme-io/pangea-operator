use base64::Engine;
use chrono::{DateTime, Utc};
use ring::rand::SystemRandom;
use ring::signature::{self, EcdsaKeyPair, RsaKeyPair};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Material(String),
    #[error("{0}")]
    Request(String),
    #[error("rolesanywhere CreateSession refused ({status}): {body}")]
    Refused { status: u16, body: String },
    #[error("{0}")]
    Response(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRequest {
    pub trust_anchor_arn: String,
    pub profile_arn: String,
    pub role_arn: String,
    pub duration_seconds: Option<u32>,
}

impl SessionRequest {
    pub fn endpoint_region(&self) -> Option<&str> {
        let mut parts = self.trust_anchor_arn.splitn(6, ':');
        let (arn, _, service, region, _, resource) = (
            parts.next()?,
            parts.next()?,
            parts.next()?,
            parts.next()?,
            parts.next()?,
            parts.next()?,
        );
        (arn == "arn"
            && service == "rolesanywhere"
            && !region.is_empty()
            && resource.starts_with("trust-anchor/"))
        .then_some(region)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SessionCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expiration: Option<String>,
}

impl std::fmt::Debug for SessionCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("expiration", &self.expiration)
            .finish_non_exhaustive()
    }
}

const SERVICE: &str = "rolesanywhere";
const DEFAULT_DURATION_SECONDS: u32 = 3600;

pub enum SigningKey {
    Rsa(RsaKeyPair),
    EcdsaP256(EcdsaKeyPair),
}

impl SigningKey {
    pub fn from_pem(pem_text: &[u8]) -> Result<Self> {
        let blocks = pem_blocks(pem_text)?;
        let (label, der) = blocks
            .into_iter()
            .find(|(l, _)| l.ends_with("PRIVATE KEY"))
            .ok_or_else(|| Error::Material("tls.key holds no PEM private key".into()))?;
        match label.as_str() {
            "RSA PRIVATE KEY" => RsaKeyPair::from_der(&der)
                .map(SigningKey::Rsa)
                .map_err(|e| Error::Material(format!("tls.key is not a usable RSA key: {e}"))),
            "PRIVATE KEY" => {
                if let Ok(k) = RsaKeyPair::from_pkcs8(&der) {
                    return Ok(SigningKey::Rsa(k));
                }
                EcdsaKeyPair::from_pkcs8(
                    &signature::ECDSA_P256_SHA256_ASN1_SIGNING,
                    &der,
                    &SystemRandom::new(),
                )
                .map(SigningKey::EcdsaP256)
                .map_err(|e| {
                    Error::Material(format!(
                        "tls.key PKCS#8 is neither RSA nor ECDSA P-256: {e}"
                    ))
                })
            }
            "EC PRIVATE KEY" => Err(Error::Material(
                "tls.key is a SEC1 EC key; issue it with PKCS#8 encoding".into(),
            )),
            other => Err(Error::Material(format!(
                "tls.key PEM label {other:?} is not supported"
            ))),
        }
    }

    pub fn algorithm(&self) -> &'static str {
        match self {
            SigningKey::Rsa(_) => "AWS4-X509-RSA-SHA256",
            SigningKey::EcdsaP256(_) => "AWS4-X509-ECDSA-SHA256",
        }
    }

    fn sign(&self, msg: &[u8]) -> Result<Vec<u8>> {
        let rng = SystemRandom::new();
        match self {
            SigningKey::Rsa(k) => {
                let mut sig = vec![0u8; k.public().modulus_len()];
                k.sign(&signature::RSA_PKCS1_SHA256, &rng, msg, &mut sig)
                    .map_err(|e| Error::Material(format!("RSA signing failed: {e}")))?;
                Ok(sig)
            }
            SigningKey::EcdsaP256(k) => k
                .sign(&rng, msg)
                .map(|s| s.as_ref().to_vec())
                .map_err(|e| Error::Material(format!("ECDSA signing failed: {e}"))),
        }
    }
}

pub struct CertificateChain {
    pub leaf: Vec<u8>,
    pub intermediates: Vec<Vec<u8>>,
}

impl CertificateChain {
    pub fn from_pem(pem_text: &[u8]) -> Result<Self> {
        let mut certs = pem_blocks(pem_text)?
            .into_iter()
            .filter(|(l, _)| l == "CERTIFICATE")
            .map(|(_, d)| d);
        let leaf = certs
            .next()
            .ok_or_else(|| Error::Material("tls.crt holds no PEM certificate".into()))?;
        Ok(CertificateChain {
            leaf,
            intermediates: certs.collect(),
        })
    }

    pub fn serial_decimal(&self) -> Result<String> {
        certificate_serial(&self.leaf)
            .map(|bytes| big_endian_to_decimal(&bytes))
            .ok_or_else(|| Error::Material("tls.crt leaf certificate DER is malformed".into()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignedRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
    pub string_to_sign: String,
    pub signature: Vec<u8>,
}

pub fn sign_create_session(
    ra: &SessionRequest,
    chain: &CertificateChain,
    key: &SigningKey,
    now: DateTime<Utc>,
) -> Result<SignedRequest> {
    let region = ra.endpoint_region().ok_or_else(|| {
        Error::Material(format!(
            "trustAnchorArn {:?} names no region",
            ra.trust_anchor_arn
        ))
    })?;
    let host = format!("{SERVICE}.{region}.amazonaws.com");
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let b64 = base64::engine::general_purpose::STANDARD;

    let body = serde_json::json!({
        "durationSeconds": ra.duration_seconds.unwrap_or(DEFAULT_DURATION_SECONDS),
        "profileArn": ra.profile_arn,
        "roleArn": ra.role_arn,
        "trustAnchorArn": ra.trust_anchor_arn,
    })
    .to_string();

    let mut headers: Vec<(String, String)> = vec![
        ("content-type".into(), "application/json".into()),
        ("host".into(), host.clone()),
        ("x-amz-date".into(), amz_date.clone()),
        ("x-amz-x509".into(), b64.encode(&chain.leaf)),
    ];
    if !chain.intermediates.is_empty() {
        let joined = chain
            .intermediates
            .iter()
            .map(|c| b64.encode(c))
            .collect::<Vec<_>>()
            .join(",");
        headers.push(("x-amz-x509-chain".into(), joined));
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers: String = headers
        .iter()
        .map(|(k, v)| format!("{k}:{}\n", v.trim()))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "POST\n/sessions\n\n{canonical_headers}\n{signed_headers}\n{}",
        sha256_hex(body.as_bytes())
    );

    let scope = format!("{date}/{region}/{SERVICE}/aws4_request");
    let string_to_sign = format!(
        "{}\n{amz_date}\n{scope}\n{}",
        key.algorithm(),
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = key.sign(string_to_sign.as_bytes())?;
    let authorization = format!(
        "{} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={}",
        key.algorithm(),
        chain.serial_decimal()?,
        hex_lower(&signature)
    );
    headers.push(("authorization".into(), authorization));

    Ok(SignedRequest {
        url: format!("https://{host}/sessions"),
        headers,
        body,
        string_to_sign,
        signature,
    })
}

pub fn parse_create_session_response(body: &[u8]) -> Result<SessionCredentials> {
    let v: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| Error::Response(format!("CreateSession response is not JSON: {e}")))?;
    let creds = v
        .get("credentialSet")
        .and_then(|s| s.get(0))
        .and_then(|e| e.get("credentials"))
        .ok_or_else(|| Error::Response("CreateSession response has no credentialSet".into()))?;
    let field = |name: &str| -> Result<String> {
        creds
            .get(name)
            .and_then(|x| x.as_str())
            .map(str::to_string)
            .ok_or_else(|| Error::Response(format!("CreateSession credentials lack {name}")))
    };
    let [id, key, token] = ["accessKeyId", "secretAccessKey", "sessionToken"].map(&field);
    Ok(SessionCredentials {
        access_key_id: id?,
        secret_access_key: key?,
        session_token: token?,
        expiration: creds
            .get("expiration")
            .and_then(|x| x.as_str())
            .map(str::to_string),
    })
}

pub async fn issue_session(
    ra: &SessionRequest,
    tls_crt: &[u8],
    tls_key: &[u8],
) -> Result<SessionCredentials> {
    let chain = CertificateChain::from_pem(tls_crt)?;
    let key = SigningKey::from_pem(tls_key)?;
    let signed = sign_create_session(ra, &chain, &key, Utc::now())?;
    let client = reqwest::Client::new();
    let mut req = client.post(&signed.url).body(signed.body.clone());
    for (k, v) in signed.headers.iter().filter(|(k, _)| k != "host") {
        req = req.header(k.as_str(), v.as_str());
    }
    let resp = req
        .send()
        .await
        .map_err(|e| Error::Request(format!("rolesanywhere CreateSession request failed: {e}")))?;
    let status = resp.status();
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| Error::Request(format!("rolesanywhere CreateSession body unreadable: {e}")))?;
    if !status.is_success() {
        return Err(Error::Refused {
            status: status.as_u16(),
            body: String::from_utf8_lossy(&bytes).into_owned(),
        });
    }
    parse_create_session_response(&bytes)
}

fn pem_blocks(text: &[u8]) -> Result<Vec<(String, Vec<u8>)>> {
    let text = std::str::from_utf8(text)
        .map_err(|_| Error::Material("PEM material is not UTF-8".into()))?;
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut out = Vec::new();
    let mut current: Option<(String, String)> = None;
    for line in text.lines().map(str::trim) {
        if let Some(label) = line
            .strip_prefix("-----BEGIN ")
            .and_then(|l| l.strip_suffix("-----"))
        {
            current = Some((label.to_string(), String::new()));
        } else if let Some(label) = line
            .strip_prefix("-----END ")
            .and_then(|l| l.strip_suffix("-----"))
        {
            let (open, body) = current
                .take()
                .ok_or_else(|| Error::Material("PEM END without BEGIN".into()))?;
            if open != label {
                return Err(Error::Material(format!(
                    "PEM block BEGIN {open} closed by END {label}"
                )));
            }
            let der = b64
                .decode(body.as_bytes())
                .map_err(|e| Error::Material(format!("PEM {label} body is not base64: {e}")))?;
            out.push((label.to_string(), der));
        } else if let Some((_, body)) = current.as_mut() {
            if !line.contains(':') {
                body.push_str(line);
            }
        }
    }
    Ok(out)
}

fn der_tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (first as usize, rest)
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |acc, b| (acc << 8) | *b as usize);
        (len, &rest[n..])
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

fn certificate_serial(der: &[u8]) -> Option<Vec<u8>> {
    let (tag, cert, _) = der_tlv(der)?;
    if tag != 0x30 {
        return None;
    }
    let (tag, tbs, _) = der_tlv(cert)?;
    if tag != 0x30 {
        return None;
    }
    let (mut tag, mut value, mut rest) = der_tlv(tbs)?;
    if tag == 0xa0 {
        (tag, value, rest) = der_tlv(rest)?;
    }
    let _ = rest;
    (tag == 0x02).then(|| value.to_vec())
}

fn big_endian_to_decimal(bytes: &[u8]) -> String {
    let mut digits: Vec<u8> = vec![0];
    for &byte in bytes {
        let mut carry = byte as u32;
        for d in digits.iter_mut() {
            let v = (*d as u32) * 256 + carry;
            *d = (v % 10) as u8;
            carry = v / 10;
        }
        while carry > 0 {
            digits.push((carry % 10) as u8);
            carry /= 10;
        }
    }
    digits.iter().rev().map(|d| (b'0' + d) as char).collect()
}

fn sha256_hex(data: &[u8]) -> String {
    hex_lower(ring::digest::digest(&ring::digest::SHA256, data).as_ref())
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use ring::signature::KeyPair;

    const FIXTURE: &str = "fixture-value";

    fn ra() -> SessionRequest {
        SessionRequest {
            trust_anchor_arn: "arn:aws:rolesanywhere:us-east-2:376129857990:trust-anchor/ta-1"
                .into(),
            profile_arn: "arn:aws:rolesanywhere:us-east-2:376129857990:profile/p-1".into(),
            role_arn: "arn:aws:iam::376129857990:role/pangea-operator-ryn".into(),
            duration_seconds: None,
        }
    }

    fn fake_cert(serial: &[u8]) -> Vec<u8> {
        let mut int = vec![0x02, serial.len() as u8];
        int.extend_from_slice(serial);
        let mut tbs_body = vec![0xa0, 0x03, 0x02, 0x01, 0x02];
        tbs_body.extend(int);
        tbs_body.extend([0x05, 0x00]);
        let mut tbs = vec![0x30, tbs_body.len() as u8];
        tbs.extend(tbs_body);
        let mut cert = vec![0x30, tbs.len() as u8];
        cert.extend(tbs);
        cert
    }

    fn pem(label: &str, der: &[u8]) -> String {
        let b = base64::engine::general_purpose::STANDARD.encode(der);
        let lines: Vec<&str> = b
            .as_bytes()
            .chunks(64)
            .map(|c| std::str::from_utf8(c).unwrap())
            .collect();
        format!(
            "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
            lines.join("\n")
        )
    }

    fn p256_pkcs8() -> Vec<u8> {
        EcdsaKeyPair::generate_pkcs8(
            &signature::ECDSA_P256_SHA256_ASN1_SIGNING,
            &SystemRandom::new(),
        )
        .unwrap()
        .as_ref()
        .to_vec()
    }

    #[test]
    fn endpoint_region_comes_from_the_trust_anchor() {
        assert_eq!(ra().endpoint_region(), Some("us-east-2"));
        let mut bad = ra();
        bad.trust_anchor_arn = "arn:aws:rolesanywhere:us-east-2:1:profile/p".into();
        assert_eq!(bad.endpoint_region(), None);
    }

    #[test]
    fn serial_converts_to_decimal() {
        assert_eq!(big_endian_to_decimal(&[0x01, 0x00]), "256");
        assert_eq!(big_endian_to_decimal(&[0x00, 0xff]), "255");
        assert_eq!(
            big_endian_to_decimal(&[0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11]),
            "335812727670730321937"
        );
        let chain = CertificateChain {
            leaf: fake_cert(&[0x01, 0x00]),
            intermediates: vec![],
        };
        assert_eq!(chain.serial_decimal().unwrap(), "256");
    }

    #[test]
    fn pem_chain_splits_leaf_and_intermediates() {
        let text = format!(
            "{}{}",
            pem("CERTIFICATE", &fake_cert(&[7])),
            pem("CERTIFICATE", &fake_cert(&[8]))
        );
        let chain = CertificateChain::from_pem(text.as_bytes()).unwrap();
        assert_eq!(chain.serial_decimal().unwrap(), "7");
        assert_eq!(chain.intermediates.len(), 1);
    }

    #[test]
    fn sec1_ec_key_is_refused_with_the_fix_named() {
        let text = pem("EC PRIVATE KEY", &[1, 2, 3]);
        let err = SigningKey::from_pem(text.as_bytes())
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("PKCS#8"), "{err}");
    }

    #[test]
    fn signed_request_verifies_and_carries_the_x509_scheme() {
        let pkcs8 = p256_pkcs8();
        let key = SigningKey::from_pem(pem("PRIVATE KEY", &pkcs8).as_bytes()).unwrap();
        assert_eq!(key.algorithm(), "AWS4-X509-ECDSA-SHA256");
        let chain = CertificateChain {
            leaf: fake_cert(&[0x30, 0x39]),
            intermediates: vec![fake_cert(&[2])],
        };
        let now = Utc.with_ymd_and_hms(2026, 10, 16, 12, 0, 0).unwrap();
        let req = sign_create_session(&ra(), &chain, &key, now).unwrap();

        assert_eq!(
            req.url,
            "https://rolesanywhere.us-east-2.amazonaws.com/sessions"
        );
        let header = |n: &str| {
            req.headers
                .iter()
                .find(|(k, _)| k == n)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(header("x-amz-date").as_deref(), Some("20261016T120000Z"));
        assert!(header("x-amz-x509-chain").is_some());
        let auth = header("authorization").unwrap();
        assert!(auth.starts_with(
            "AWS4-X509-ECDSA-SHA256 Credential=12345/20261016/us-east-2/rolesanywhere/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-x509;x-amz-x509-chain, Signature="
        ), "{auth}");

        let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
        assert_eq!(body["durationSeconds"], 3600);
        assert_eq!(
            body["roleArn"],
            "arn:aws:iam::376129857990:role/pangea-operator-ryn"
        );

        let SigningKey::EcdsaP256(kp) = &key else {
            unreachable!()
        };
        signature::UnparsedPublicKey::new(
            &signature::ECDSA_P256_SHA256_ASN1,
            kp.public_key().as_ref(),
        )
        .verify(req.string_to_sign.as_bytes(), &req.signature)
        .expect("signature verifies against the leaf key");
        assert!(req.string_to_sign.starts_with("AWS4-X509-ECDSA-SHA256\n20261016T120000Z\n20261016/us-east-2/rolesanywhere/aws4_request\n"));
    }

    #[test]
    fn create_session_response_maps_to_session_credentials() {
        let body = serde_json::json!({"credentialSet": [{"credentials": {
            "accessKeyId": "ASIA1",
            "secretAccessKey": FIXTURE,
            "sessionToken": "tok",
            "expiration": "2026-10-16T13:00:00Z"
        }}]})
        .to_string();
        let body = body.as_bytes();
        let c = parse_create_session_response(body).unwrap();
        assert_eq!(c.access_key_id, "ASIA1");
        assert_eq!(c.secret_access_key, FIXTURE);
        assert_eq!(c.session_token, "tok");
        assert_eq!(c.expiration.as_deref(), Some("2026-10-16T13:00:00Z"));
        assert!(!format!("{c:?}").contains(FIXTURE));
        assert!(parse_create_session_response(br#"{"credentialSet":[]}"#).is_err());
    }
}
