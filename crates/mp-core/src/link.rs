use std::fmt;
use std::str::FromStr;

use cid::Cid;
use url::Url;

use crate::{MpError, Result, parse_file_cid};

/// Parsed canonical `mp://` file link.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareLink {
    cid: Cid,
    filename: Option<String>,
}

impl ShareLink {
    /// Create a link from a validated file CID and optional display filename.
    pub fn new(cid: Cid, filename: Option<String>) -> Result<Self> {
        parse_file_cid(&cid.to_string())?;
        let filename = normalize_filename(filename)?;
        Ok(Self { cid, filename })
    }

    /// Return the content CID.
    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    /// Return the advisory display filename.
    pub fn filename(&self) -> Option<&str> {
        self.filename.as_deref()
    }
}

impl fmt::Display for ShareLink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut url = Url::parse(&format!("mp://{}", self.cid)).map_err(|_| fmt::Error)?;
        if let Some(filename) = &self.filename {
            url.query_pairs_mut().append_pair("filename", filename);
        }
        formatter.write_str(url.as_str())
    }
}

impl FromStr for ShareLink {
    type Err = MpError;

    fn from_str(value: &str) -> Result<Self> {
        let url = Url::parse(value)
            .map_err(|error| MpError::InvalidLink(format!("URL parse failed: {error}")))?;
        if url.scheme() != "mp" {
            return Err(MpError::InvalidLink("scheme must be mp".to_string()));
        }
        if !url.username().is_empty() || url.password().is_some() || url.port().is_some() {
            return Err(MpError::InvalidLink(
                "credentials and ports are not allowed".to_string(),
            ));
        }
        if !url.path().is_empty() && url.path() != "/" {
            return Err(MpError::InvalidLink("path must be empty".to_string()));
        }
        if url.fragment().is_some() {
            return Err(MpError::InvalidLink("fragment is not allowed".to_string()));
        }

        let host = url
            .host_str()
            .ok_or_else(|| MpError::InvalidLink("CID host is missing".to_string()))?;
        let cid = parse_file_cid(host)?;
        let mut filename = None;
        for (key, value) in url.query_pairs() {
            if key != "filename" {
                return Err(MpError::InvalidLink(format!(
                    "unknown query parameter: {key}"
                )));
            }
            if filename.is_some() {
                return Err(MpError::InvalidLink(
                    "filename may appear only once".to_string(),
                ));
            }
            filename = Some(value.into_owned());
        }

        Self::new(cid, filename)
    }
}

fn normalize_filename(filename: Option<String>) -> Result<Option<String>> {
    match filename {
        None => Ok(None),
        Some(value) => {
            let value = value.trim();
            if value.is_empty() {
                return Err(MpError::InvalidLink(
                    "filename must not be empty".to_string(),
                ));
            }
            if value.len() > 255 {
                return Err(MpError::InvalidLink(
                    "filename exceeds 255 bytes".to_string(),
                ));
            }
            Ok(Some(value.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calculate_bytes_cid;

    #[test]
    fn link_round_trip_preserves_unicode_filename() {
        let link = ShareLink::new(
            calculate_bytes_cid(b"hello").unwrap(),
            Some("report 你好.txt".to_string()),
        )
        .unwrap();
        let encoded = link.to_string();
        let decoded: ShareLink = encoded.parse().unwrap();
        assert_eq!(decoded, link);
        assert!(encoded.contains("filename=report+%E4%BD%A0%E5%A5%BD.txt"));
    }

    #[test]
    fn rejects_unknown_query_parameter() {
        let cid = calculate_bytes_cid(b"hello").unwrap();
        let error = format!("mp://{cid}?filename=a&tracker=x")
            .parse::<ShareLink>()
            .unwrap_err();
        assert!(error.to_string().contains("unknown query parameter"));
    }
}
