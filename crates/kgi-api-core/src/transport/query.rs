use std::{borrow::Cow, collections::BTreeSet};

use axum::http::{HeaderMap, header};
use kgi_api_model::projection::{GraphWindowAnchor, MAX_WINDOW_DEPTH};
use kgi_model::block::{BlockHash, MAX_DAA_SCORE};
use percent_encoding::percent_decode;

use crate::{client::ClientIdentifier, codec::HeadEtag};

use super::ParsedHeadCondition;

const MAX_HEAD_LEVELS_PER_REQUEST: usize = 64;

/// A query that accepts no parameters.
pub(super) struct EmptyQuery;

impl EmptyQuery {
    pub(super) fn parse(query: Option<&str>) -> Option<Self> {
        decode_query(query)?.is_empty().then_some(Self)
    }
}

/// The validated `/graph/head` query.
pub(super) struct HeadSnapshotQuery {
    pub(super) target_depth: u32,
}

impl HeadSnapshotQuery {
    pub(super) fn parse(query: Option<&str>) -> Option<Self> {
        let mut target_depth = None;
        for (name, value) in decode_query(query)? {
            match name.as_str() {
                "target_depth" => set_once(&mut target_depth, parse_u32(&value)?)?,
                _ => return None,
            }
        }
        let target_depth = target_depth?;
        (1..=MAX_WINDOW_DEPTH).contains(&target_depth).then_some(Self { target_depth })
    }
}

/// The validated `/graph/deltas` query.
pub(super) struct CanonicalDeltaQuery {
    pub(super) publication_id: u64,
    pub(super) from_revision_id: u64,
    pub(super) client: Option<ClientIdentifier>,
}

/// Proof that `/graph/wakeups` supplied one complete, valid cursor.
///
/// The cursor values are validation-only because registration always binds to
/// the coherent current publication and its current Head.
pub(super) struct PublicationWakeupQuery;

impl PublicationWakeupQuery {
    pub(super) fn parse(query: Option<&str>) -> Option<Self> {
        let mut publication_id = None;
        let mut from_revision_id = None;
        for (name, value) in decode_query(query)? {
            match name.as_str() {
                "publication_id" => set_once(&mut publication_id, parse_u64(&value)?)?,
                "from_revision_id" => set_once(&mut from_revision_id, parse_u64(&value)?)?,
                _ => return None,
            }
        }
        publication_id?;
        from_revision_id?;
        Some(Self)
    }
}

impl CanonicalDeltaQuery {
    pub(super) fn parse(query: Option<&str>) -> Option<Self> {
        let mut publication_id = None;
        let mut from_revision_id = None;
        let mut client = None;
        for (name, value) in decode_query(query)? {
            match name.as_str() {
                "publication_id" => set_once(&mut publication_id, parse_u64(&value)?)?,
                "from_revision_id" => set_once(&mut from_revision_id, parse_u64(&value)?)?,
                "client_id" => set_once(&mut client, ClientIdentifier::parse(&value)?)?,
                _ => return None,
            }
        }
        Some(Self { publication_id: publication_id?, from_revision_id: from_revision_id?, client })
    }
}

/// The validated `/graph/levels` query.
pub(super) struct HeadLevelsQuery {
    pub(super) publication_id: u64,
    pub(super) levels: Vec<u32>,
}

impl HeadLevelsQuery {
    pub(super) fn parse(query: Option<&str>) -> Option<Self> {
        let mut publication_id = None;
        let mut levels = BTreeSet::new();
        for (name, value) in decode_query(query)? {
            match name.as_str() {
                "publication_id" => set_once(&mut publication_id, parse_u64(&value)?)?,
                "level" => {
                    let level = parse_u32(&value)?;
                    if level == 0 {
                        return None;
                    }
                    levels.insert(level);
                }
                _ => return None,
            }
        }
        if levels.is_empty() || levels.len() > MAX_HEAD_LEVELS_PER_REQUEST {
            return None;
        }
        Some(Self { publication_id: publication_id?, levels: levels.into_iter().collect() })
    }
}

/// The validated `/graph/window` query.
pub(super) struct AnchoredWindowQuery {
    pub(super) anchor: GraphWindowAnchor,
    pub(super) max_depth: u32,
}

impl AnchoredWindowQuery {
    pub(super) fn parse(query: Option<&str>) -> Option<Self> {
        let mut max_depth = None;
        let mut anchor = None;
        for (name, value) in decode_query(query)? {
            match name.as_str() {
                "max_depth" => set_once(&mut max_depth, parse_u32(&value)?)?,
                "level" => {
                    let level = parse_u32(&value)?;
                    if level == 0 {
                        return None;
                    }
                    set_once(&mut anchor, GraphWindowAnchor::Level(level))?;
                }
                "block_hash" => {
                    let hash = value.parse::<BlockHash>().ok()?;
                    if hash.to_string() != value {
                        return None;
                    }
                    set_once(&mut anchor, GraphWindowAnchor::BlockHash(hash))?;
                }
                "daa_score" => {
                    let score = parse_u64(&value)?;
                    if score > MAX_DAA_SCORE {
                        return None;
                    }
                    set_once(&mut anchor, GraphWindowAnchor::DaaScore(score))?;
                }
                _ => return None,
            }
        }
        let max_depth = max_depth?;
        if max_depth == 0 {
            return None;
        }
        Some(Self { anchor: anchor?, max_depth: max_depth.min(MAX_WINDOW_DEPTH) })
    }
}

fn decode_query(query: Option<&str>) -> Option<Vec<(String, String)>> {
    let Some(query) = query else { return Some(Vec::new()) };
    if query.is_empty() {
        return Some(Vec::new());
    }

    query
        .split('&')
        .map(|pair| {
            if pair.is_empty() {
                return None;
            }
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            Some((decode_component(name)?, decode_component(value)?))
        })
        .collect()
}

fn set_once<T>(target: &mut Option<T>, value: T) -> Option<()> {
    if target.is_some() {
        return None;
    }
    *target = Some(value);
    Some(())
}

fn parse_u32(value: &str) -> Option<u32> {
    (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())).then(|| value.parse().ok()).flatten()
}

fn parse_u64(value: &str) -> Option<u64> {
    (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())).then(|| value.parse().ok()).flatten()
}

fn decode_component(component: &str) -> Option<String> {
    let replaced =
        if component.as_bytes().contains(&b'+') { Cow::Owned(component.replace('+', " ")) } else { Cow::Borrowed(component) };
    percent_decode(replaced.as_bytes()).decode_utf8().ok().map(Cow::into_owned)
}

pub(super) fn accepts_gzip(headers: &HeaderMap) -> bool {
    let mut explicit_gzip = None;
    let mut wildcard = None;

    for value in headers.get_all(header::ACCEPT_ENCODING) {
        let Ok(value) = value.to_str() else { return false };
        for item in value.split(',') {
            let mut parts = item.trim().split(';');
            let coding = parts.next().unwrap_or_default().trim();
            if coding.is_empty() {
                return false;
            }
            let mut quality = None;
            for parameter in parts {
                let Some((name, value)) = parameter.trim().split_once('=') else { return false };
                if !name.trim().eq_ignore_ascii_case("q") || quality.is_some() {
                    return false;
                }
                quality = parse_quality(value.trim());
                if quality.is_none() {
                    return false;
                }
            }
            let accepted = quality.unwrap_or(true);
            if coding.eq_ignore_ascii_case("gzip") {
                explicit_gzip = Some(explicit_gzip.unwrap_or(false) || accepted);
            } else if coding == "*" {
                wildcard = Some(wildcard.unwrap_or(false) || accepted);
            }
        }
    }

    explicit_gzip.or(wildcard).unwrap_or(false)
}

fn parse_quality(value: &str) -> Option<bool> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    match whole {
        "0" => Some(fraction.bytes().any(|byte| byte != b'0')),
        "1" if fraction.bytes().all(|byte| byte == b'0') => Some(true),
        _ => None,
    }
}

impl ParsedHeadCondition {
    /// Parses all `If-None-Match` field values into a Head request condition.
    ///
    /// An absent header produces an unconditional condition. Malformed field
    /// values, entity tags, or lists return `None`.
    pub(super) fn parse(headers: &HeaderMap) -> Option<Self> {
        let mut saw_field = false;
        let mut wildcard = false;
        let mut tags = Vec::new();
        for value in headers.get_all(header::IF_NONE_MATCH) {
            saw_field = true;
            let value = value.to_str().ok()?;
            if value.trim() == "*" {
                if wildcard || !tags.is_empty() {
                    return None;
                }
                wildcard = true;
            } else {
                if wildcard {
                    return None;
                }
                parse_entity_tag_list(value, &mut tags)?;
            }
        }
        if !saw_field {
            return Some(Self::Unconditional);
        }
        if wildcard {
            return Some(Self::Any);
        }
        (!tags.is_empty()).then_some(Self::Tags(tags))
    }
}

fn parse_entity_tag_list(value: &str, tags: &mut Vec<HeadEtag>) -> Option<()> {
    let bytes = value.as_bytes();
    let mut offset = 0;
    let initial_tag_count = tags.len();
    let skip_ascii_whitespace = |offset: &mut usize| {
        while bytes.get(*offset).is_some_and(u8::is_ascii_whitespace) {
            *offset += 1;
        }
    };
    while offset < bytes.len() {
        skip_ascii_whitespace(&mut offset);
        if bytes.get(offset..offset + 2) == Some(b"W/") {
            offset += 2;
        }
        if bytes.get(offset) != Some(&b'"') {
            return None;
        }
        offset += 1;
        let start = offset;
        loop {
            let &byte = bytes.get(offset)?;
            if byte == b'"' {
                break;
            }
            if byte < 0x21 || byte == 0x7f {
                return None;
            }
            offset += 1;
        }
        let opaque = std::str::from_utf8(&bytes[start..offset]).ok()?;
        tags.push(HeadEtag::from_opaque(opaque));
        offset += 1;
        skip_ascii_whitespace(&mut offset);
        if offset == bytes.len() {
            break;
        }
        if bytes[offset] != b',' {
            return None;
        }
        offset += 1;
        if offset == bytes.len() {
            return None;
        }
    }

    (tags.len() > initial_tag_count).then_some(())
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};

    use super::{
        CanonicalDeltaQuery, EmptyQuery, HeadLevelsQuery, HeadSnapshotQuery, PublicationWakeupQuery, accepts_gzip, decode_query,
    };
    use crate::transport::ParsedHeadCondition;

    #[test]
    fn query_parser_decodes_components_and_rejects_invalid_utf8() {
        assert_eq!(
            decode_query(Some("target_depth=%32%30&name=hello+world")),
            Some(vec![("target_depth".into(), "20".into()), ("name".into(), "hello world".into())])
        );
        assert_eq!(decode_query(Some("value=%FF")), None);
    }

    #[test]
    fn endpoint_queries_reject_unknown_and_duplicate_scalars() {
        assert!(EmptyQuery::parse(None).is_some());
        assert!(EmptyQuery::parse(Some("unknown=1")).is_none());
        assert!(HeadSnapshotQuery::parse(Some("target_depth=20")).is_some());
        assert!(HeadSnapshotQuery::parse(Some("target_depth=20&target_depth=30")).is_none());
        assert!(CanonicalDeltaQuery::parse(Some("publication_id=1&from_revision_id=2&unknown=3")).is_none());
        assert!(PublicationWakeupQuery::parse(Some("publication_id=1&from_revision_id=2")).is_some());
        assert!(PublicationWakeupQuery::parse(Some("publication_id=1&from_revision_id=2&client_id=3")).is_none());
    }

    #[test]
    fn level_query_deduplicates_and_orders_repeated_levels() {
        let query = HeadLevelsQuery::parse(Some("publication_id=7&level=3&level=1&level=3")).expect("valid repeated levels parse");
        assert_eq!(query.publication_id, 7);
        assert_eq!(query.levels, vec![1, 3]);
    }

    #[test]
    fn gzip_acceptance_obeys_explicit_and_wildcard_quality() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_static("br, *;q=0.5"));
        assert!(accepts_gzip(&headers));
        headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_static("*;q=1, gzip;q=0"));
        assert!(!accepts_gzip(&headers));
        headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_static("gzip;q=0.001"));
        assert!(accepts_gzip(&headers));
    }

    #[test]
    fn conditional_parser_accepts_lists_and_rejects_malformed_values() {
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("W/\"one\", \"two\""));
        let Some(ParsedHeadCondition::Tags(tags)) = ParsedHeadCondition::parse(&headers) else {
            panic!("valid entity-tag list parses");
        };
        assert_eq!(tags.len(), 2);

        headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("W/one"));
        assert!(ParsedHeadCondition::parse(&headers).is_none());
    }

    #[test]
    fn conditional_parser_accumulates_repeated_fields_without_combining_them() {
        let mut headers = HeaderMap::new();
        headers.append(header::IF_NONE_MATCH, HeaderValue::from_static("\"one,two\""));
        headers.append(header::IF_NONE_MATCH, HeaderValue::from_static("W/\"three\""));
        let Some(ParsedHeadCondition::Tags(tags)) = ParsedHeadCondition::parse(&headers) else {
            panic!("repeated entity-tag fields parse");
        };
        assert_eq!(tags.len(), 2);

        headers.append(header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        assert!(ParsedHeadCondition::parse(&headers).is_none());
    }
}
