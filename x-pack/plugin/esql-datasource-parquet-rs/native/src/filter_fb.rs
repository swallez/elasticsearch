/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

//! Decodes the FlatBuffers `FilterExpr` IPC representation
//! (see `native/schema/filter_expr.fbs`) into the runtime [`FilterExpr`] enum
//! defined in [`crate::filter`].
//!
//! The Java side serialises a `FilterExpr` table; the verifier in
//! [`flatbuffers::root`] enforces that all `(required)` fields are present, so
//! the converter only needs to handle the union discriminant and optional
//! fields.

use crate::filter::FilterExpr;
use flatbuffers::InvalidFlatbuffer;
use crate::generated::fbs;

#[derive(Debug)]
pub enum FbDecodeError {
    InvalidBuffer(InvalidFlatbuffer),
    /// Union discriminant set but the corresponding payload table is absent.
    MissingPayload(&'static str),
    /// Union discriminant is `NONE` or a tag the runtime does not recognise.
    UnknownVariant,
}

impl From<InvalidFlatbuffer> for FbDecodeError {
    fn from(e: InvalidFlatbuffer) -> Self {
        FbDecodeError::InvalidBuffer(e)
    }
}

impl std::fmt::Display for FbDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FbDecodeError::InvalidBuffer(e) => write!(f, "invalid flatbuffer: {e}"),
            FbDecodeError::MissingPayload(v) => write!(f, "missing payload for variant {v}"),
            FbDecodeError::UnknownVariant => write!(f, "unknown FilterExprValue variant"),
        }
    }
}

impl std::error::Error for FbDecodeError {}

/// Verifies and decodes a `FilterExpr` flatbuffer into the runtime enum.
pub fn decode(buf: &[u8]) -> Result<FilterExpr, FbDecodeError> {
    convert(fbs ::root_as_filter_expr(buf)?)
}

fn convert(fe: fbs::FilterExpr<'_>) -> Result<FilterExpr, FbDecodeError> {
    use fbs::FilterExprValue as V;
    match fe.expr_type() {
        V::Column => {
            let t = fe
                .expr_as_column()
                .ok_or(FbDecodeError::MissingPayload("Column"))?;
            Ok(FilterExpr::Column(t.name().to_string()))
        }
        V::LiteralInt => {
            let t = fe
                .expr_as_literal_int()
                .ok_or(FbDecodeError::MissingPayload("LiteralInt"))?;
            Ok(FilterExpr::LiteralInt(t.value()))
        }
        V::LiteralLong => {
            let t = fe
                .expr_as_literal_long()
                .ok_or(FbDecodeError::MissingPayload("LiteralLong"))?;
            Ok(FilterExpr::LiteralLong(t.value()))
        }
        V::LiteralDouble => {
            let t = fe
                .expr_as_literal_double()
                .ok_or(FbDecodeError::MissingPayload("LiteralDouble"))?;
            Ok(FilterExpr::LiteralDouble(t.value()))
        }
        V::LiteralBool => {
            let t = fe
                .expr_as_literal_bool()
                .ok_or(FbDecodeError::MissingPayload("LiteralBool"))?;
            Ok(FilterExpr::LiteralBool(t.value()))
        }
        V::LiteralString => {
            let t = fe
                .expr_as_literal_string()
                .ok_or(FbDecodeError::MissingPayload("LiteralString"))?;
            Ok(FilterExpr::LiteralString(t.value().to_string()))
        }
        V::LiteralTimestampMillis => {
            let t = fe
                .expr_as_literal_timestamp_millis()
                .ok_or(FbDecodeError::MissingPayload("LiteralTimestampMillis"))?;
            Ok(FilterExpr::LiteralTimestampMillis(t.value()))
        }
        V::Eq => {
            let t = fe.expr_as_eq().ok_or(FbDecodeError::MissingPayload("Eq"))?;
            Ok(FilterExpr::Eq(
                Box::new(convert(t.left())?),
                Box::new(convert(t.right())?),
            ))
        }
        V::NotEq => {
            let t = fe
                .expr_as_not_eq()
                .ok_or(FbDecodeError::MissingPayload("NotEq"))?;
            Ok(FilterExpr::NotEq(
                Box::new(convert(t.left())?),
                Box::new(convert(t.right())?),
            ))
        }
        V::Gt => {
            let t = fe.expr_as_gt().ok_or(FbDecodeError::MissingPayload("Gt"))?;
            Ok(FilterExpr::Gt(
                Box::new(convert(t.left())?),
                Box::new(convert(t.right())?),
            ))
        }
        V::GtEq => {
            let t = fe
                .expr_as_gt_eq()
                .ok_or(FbDecodeError::MissingPayload("GtEq"))?;
            Ok(FilterExpr::GtEq(
                Box::new(convert(t.left())?),
                Box::new(convert(t.right())?),
            ))
        }
        V::Lt => {
            let t = fe.expr_as_lt().ok_or(FbDecodeError::MissingPayload("Lt"))?;
            Ok(FilterExpr::Lt(
                Box::new(convert(t.left())?),
                Box::new(convert(t.right())?),
            ))
        }
        V::LtEq => {
            let t = fe
                .expr_as_lt_eq()
                .ok_or(FbDecodeError::MissingPayload("LtEq"))?;
            Ok(FilterExpr::LtEq(
                Box::new(convert(t.left())?),
                Box::new(convert(t.right())?),
            ))
        }
        V::And => {
            let t = fe.expr_as_and().ok_or(FbDecodeError::MissingPayload("And"))?;
            Ok(FilterExpr::And(
                Box::new(convert(t.left())?),
                Box::new(convert(t.right())?),
            ))
        }
        V::Or => {
            let t = fe.expr_as_or().ok_or(FbDecodeError::MissingPayload("Or"))?;
            Ok(FilterExpr::Or(
                Box::new(convert(t.left())?),
                Box::new(convert(t.right())?),
            ))
        }
        V::Not => {
            let t = fe.expr_as_not().ok_or(FbDecodeError::MissingPayload("Not"))?;
            Ok(FilterExpr::Not(Box::new(convert(t.child())?)))
        }
        V::IsNull => {
            let t = fe
                .expr_as_is_null()
                .ok_or(FbDecodeError::MissingPayload("IsNull"))?;
            Ok(FilterExpr::IsNull(Box::new(convert(t.child())?)))
        }
        V::IsNotNull => {
            let t = fe
                .expr_as_is_not_null()
                .ok_or(FbDecodeError::MissingPayload("IsNotNull"))?;
            Ok(FilterExpr::IsNotNull(Box::new(convert(t.child())?)))
        }
        V::InList => {
            let t = fe
                .expr_as_in_list()
                .ok_or(FbDecodeError::MissingPayload("InList"))?;
            let inner = convert(t.expr())?;
            let items = match t.items() {
                Some(v) => v.iter().map(convert).collect::<Result<Vec<_>, _>>()?,
                None => Vec::new(),
            };
            Ok(FilterExpr::InList(Box::new(inner), items))
        }
        V::Like => {
            let t = fe.expr_as_like().ok_or(FbDecodeError::MissingPayload("Like"))?;
            Ok(FilterExpr::Like(
                Box::new(convert(t.expr())?),
                t.pattern().to_string(),
            ))
        }
        V::NotLike => {
            let t = fe
                .expr_as_not_like()
                .ok_or(FbDecodeError::MissingPayload("NotLike"))?;
            Ok(FilterExpr::NotLike(
                Box::new(convert(t.expr())?),
                t.pattern().to_string(),
            ))
        }
        V::StartsWith => {
            let t = fe
                .expr_as_starts_with()
                .ok_or(FbDecodeError::MissingPayload("StartsWith"))?;
            Ok(FilterExpr::StartsWith(
                Box::new(convert(t.expr())?),
                t.prefix().to_string(),
                t.upper_bound().map(str::to_string),
            ))
        }
        _ => Err(FbDecodeError::UnknownVariant),
    }
}
