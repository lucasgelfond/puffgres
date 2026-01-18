//! Neon bindings for puffgres.
//!
//! Exposes key Rust functions to Node.js via Neon.

use neon::prelude::*;

mod config;
mod runtime;

/// Parse and validate a puffgres.toml config file.
fn parse_config(mut cx: FunctionContext) -> JsResult<JsObject> {
    let toml_str = cx.argument::<JsString>(0)?.value(&mut cx);

    // Parse the TOML config
    let config: toml::Value = match toml::from_str(&toml_str) {
        Ok(v) => v,
        Err(e) => return cx.throw_error(format!("Failed to parse config: {}", e)),
    };

    // Convert to JS object
    let result = value_to_js(&mut cx, &serde_json::to_value(&config).unwrap())?;

    Ok(result.downcast_or_throw(&mut cx)?)
}

/// Parse a migration TOML file.
fn parse_migration(mut cx: FunctionContext) -> JsResult<JsObject> {
    let toml_str = cx.argument::<JsString>(0)?.value(&mut cx);

    // Parse using puffgres-config
    let config = match puffgres_config::MigrationConfig::parse(&toml_str) {
        Ok(c) => c,
        Err(e) => return cx.throw_error(format!("Failed to parse migration: {}", e)),
    };

    // Convert to JS object
    let json = serde_json::to_value(&config).unwrap();
    let result = value_to_js(&mut cx, &json)?;

    Ok(result.downcast_or_throw(&mut cx)?)
}

/// Compute content hash for a migration file.
fn compute_content_hash(mut cx: FunctionContext) -> JsResult<JsString> {
    let content = cx.argument::<JsString>(0)?.value(&mut cx);
    let hash = puffgres_pg::compute_content_hash(&content);
    Ok(cx.string(hash))
}

/// Format LSN to string representation.
fn format_lsn(mut cx: FunctionContext) -> JsResult<JsString> {
    let lsn = cx.argument::<JsNumber>(0)?.value(&mut cx) as u64;
    let formatted = puffgres_pg::format_lsn(lsn);
    Ok(cx.string(formatted))
}

/// Parse LSN from string representation.
fn parse_lsn(mut cx: FunctionContext) -> JsResult<JsNumber> {
    let lsn_str = cx.argument::<JsString>(0)?.value(&mut cx);
    match puffgres_pg::parse_lsn(&lsn_str) {
        Ok(lsn) => Ok(cx.number(lsn as f64)),
        Err(e) => cx.throw_error(format!("Failed to parse LSN: {}", e)),
    }
}

/// Convert a serde_json::Value to a Neon JS value.
fn value_to_js<'a>(cx: &mut FunctionContext<'a>, value: &serde_json::Value) -> JsResult<'a, JsValue> {
    match value {
        serde_json::Value::Null => Ok(cx.null().upcast()),
        serde_json::Value::Bool(b) => Ok(cx.boolean(*b).upcast()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(cx.number(i as f64).upcast())
            } else if let Some(f) = n.as_f64() {
                Ok(cx.number(f).upcast())
            } else {
                Ok(cx.null().upcast())
            }
        }
        serde_json::Value::String(s) => Ok(cx.string(s).upcast()),
        serde_json::Value::Array(arr) => {
            let js_arr = cx.empty_array();
            for (i, v) in arr.iter().enumerate() {
                let js_val = value_to_js(cx, v)?;
                js_arr.set(cx, i as u32, js_val)?;
            }
            Ok(js_arr.upcast())
        }
        serde_json::Value::Object(obj) => {
            let js_obj = cx.empty_object();
            for (k, v) in obj {
                let js_val = value_to_js(cx, v)?;
                js_obj.set(cx, k.as_str(), js_val)?;
            }
            Ok(js_obj.upcast())
        }
    }
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    // Configuration parsing
    cx.export_function("parseConfig", parse_config)?;
    cx.export_function("parseMigration", parse_migration)?;
    cx.export_function("computeContentHash", compute_content_hash)?;

    // LSN utilities
    cx.export_function("formatLsn", format_lsn)?;
    cx.export_function("parseLsn", parse_lsn)?;

    Ok(())
}
