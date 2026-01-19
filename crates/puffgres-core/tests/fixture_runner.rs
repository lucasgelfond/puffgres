use std::collections::HashMap;
use std::fs;
use std::path::Path;

use puffgres_core::{
    extract_id, Action, DocumentId, IdType, IdentityTransformer, Mapping, MembershipConfig,
    Operation, Predicate, Router, RowEvent, Transformer, Value,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Fixture {
    name: String,
    description: String,
    mapping: MappingDef,
    events: Vec<EventDef>,
    expected_actions: Vec<ExpectedAction>,
}

#[derive(Debug, Deserialize)]
struct MappingDef {
    name: String,
    namespace: String,
    source: SourceDef,
    id: IdDef,
    columns: Vec<String>,
    #[serde(default)]
    membership: Option<MembershipDef>,
}

#[derive(Debug, Deserialize)]
struct SourceDef {
    schema: String,
    table: String,
}

#[derive(Debug, Deserialize)]
struct IdDef {
    column: String,
    #[serde(rename = "type")]
    id_type: String,
}

#[derive(Debug, Deserialize)]
struct MembershipDef {
    mode: String,
    predicate: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventDef {
    op: String,
    schema: String,
    table: String,
    lsn: u64,
    #[serde(default)]
    new: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    old: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct ExpectedAction {
    #[serde(rename = "type")]
    action_type: String,
    id: serde_json::Value,
    #[serde(default)]
    doc: Option<HashMap<String, serde_json::Value>>,
}

fn parse_id_type(s: &str) -> IdType {
    match s {
        "uint" => IdType::Uint,
        "int" => IdType::Int,
        "uuid" => IdType::Uuid,
        "string" => IdType::String,
        _ => panic!("Unknown id type: {}", s),
    }
}

fn parse_operation(s: &str) -> Operation {
    match s {
        "insert" => Operation::Insert,
        "update" => Operation::Update,
        "delete" => Operation::Delete,
        _ => panic!("Unknown operation: {}", s),
    }
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        serde_json::Value::Object(obj) => Value::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

fn json_map_to_row(map: &HashMap<String, serde_json::Value>) -> HashMap<String, Value> {
    map.iter()
        .map(|(k, v)| (k.clone(), json_to_value(v)))
        .collect()
}

fn build_mapping(def: &MappingDef) -> Mapping {
    let id_type = parse_id_type(&def.id.id_type);

    let membership = match &def.membership {
        None => MembershipConfig::All,
        Some(m) => match m.mode.as_str() {
            "all" => MembershipConfig::All,
            "view" => MembershipConfig::View,
            "dsl" => {
                let pred = Predicate::parse(m.predicate.as_ref().unwrap()).unwrap();
                MembershipConfig::Dsl(pred)
            }
            _ => panic!("Unknown membership mode: {}", m.mode),
        },
    };

    Mapping::builder(&def.name)
        .namespace(&def.namespace)
        .source(&def.source.schema, &def.source.table)
        .id(&def.id.column, id_type)
        .columns(def.columns.clone())
        .membership(membership)
        .build()
        .unwrap()
}

fn build_event(def: &EventDef) -> RowEvent {
    RowEvent {
        op: parse_operation(&def.op),
        schema: def.schema.clone(),
        table: def.table.clone(),
        new: def.new.as_ref().map(json_map_to_row),
        old: def.old.as_ref().map(json_map_to_row),
        lsn: def.lsn,
        txid: None,
        timestamp: None,
    }
}

fn run_fixture(fixture: &Fixture) -> Vec<Action> {
    let mapping = build_mapping(&fixture.mapping);
    let router = Router::new(vec![mapping.clone()]);
    let transformer = IdentityTransformer::new(fixture.mapping.columns.clone());

    let mut actions = Vec::new();

    for event_def in &fixture.events {
        let event = build_event(event_def);
        let matched = router.route(&event);

        for m in matched {
            let id = extract_id(&event, &m.id.column, m.id.id_type).unwrap();
            let action = transformer.transform(&event, id).unwrap();
            if action.requires_write() {
                actions.push(action);
            }
        }
    }

    actions
}

fn compare_actions(actual: &[Action], expected: &[ExpectedAction]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "Action count mismatch: got {}, expected {}",
        actual.len(),
        expected.len()
    );

    for (i, (act, exp)) in actual.iter().zip(expected.iter()).enumerate() {
        match (act, exp.action_type.as_str()) {
            (Action::Upsert { id, doc, .. }, "upsert") => {
                // Compare ID
                let expected_id = match &exp.id {
                    serde_json::Value::Number(n) => {
                        if let Some(u) = n.as_u64() {
                            DocumentId::Uint(u)
                        } else if let Some(i) = n.as_i64() {
                            DocumentId::Int(i)
                        } else {
                            panic!("Invalid ID number in expected action");
                        }
                    }
                    serde_json::Value::String(s) => DocumentId::String(s.clone()),
                    _ => panic!("Invalid ID type in expected action"),
                };
                assert_eq!(
                    *id, expected_id,
                    "Action {} ID mismatch: got {:?}, expected {:?}",
                    i, id, expected_id
                );

                // Compare document if specified
                if let Some(expected_doc) = &exp.doc {
                    for (key, expected_val) in expected_doc {
                        let actual_val = doc.get(key);
                        let expected_core_val = json_to_value(expected_val);
                        assert_eq!(
                            actual_val,
                            Some(&expected_core_val),
                            "Action {} doc field '{}' mismatch",
                            i,
                            key
                        );
                    }
                }
            }
            (Action::Delete { id }, "delete") => {
                let expected_id = match &exp.id {
                    serde_json::Value::Number(n) => {
                        if let Some(u) = n.as_u64() {
                            DocumentId::Uint(u)
                        } else if let Some(i) = n.as_i64() {
                            DocumentId::Int(i)
                        } else {
                            panic!("Invalid ID number in expected action");
                        }
                    }
                    serde_json::Value::String(s) => DocumentId::String(s.clone()),
                    _ => panic!("Invalid ID type in expected action"),
                };
                assert_eq!(
                    *id, expected_id,
                    "Action {} ID mismatch: got {:?}, expected {:?}",
                    i, id, expected_id
                );
            }
            _ => panic!(
                "Action {} type mismatch: got {:?}, expected {}",
                i, act, exp.action_type
            ),
        }
    }
}

fn load_and_run_fixture(path: &Path) {
    let content = fs::read_to_string(path).expect("Failed to read fixture file");
    let fixture: Fixture = serde_json::from_str(&content).expect("Failed to parse fixture");

    println!(
        "Running fixture: {} - {}",
        fixture.name, fixture.description
    );

    let actions = run_fixture(&fixture);
    compare_actions(&actions, &fixture.expected_actions);

    println!("  PASSED");
}

#[test]
fn test_all_fixtures() {
    let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");

    if !fixtures_dir.exists() {
        println!("No fixtures directory found at {:?}", fixtures_dir);
        return;
    }

    let mut fixture_count = 0;

    for entry in fs::read_dir(&fixtures_dir).expect("Failed to read fixtures directory") {
        let entry = entry.expect("Failed to read directory entry");
        let path = entry.path();

        if path.extension().map_or(false, |ext| ext == "json") {
            load_and_run_fixture(&path);
            fixture_count += 1;
        }
    }

    assert!(
        fixture_count > 0,
        "No fixture files found in {:?}",
        fixtures_dir
    );
    println!("Ran {} fixture(s) successfully", fixture_count);
}

#[test]
fn test_basic_insert_fixture() {
    let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let path = fixtures_dir.join("basic_insert.json");
    load_and_run_fixture(&path);
}

#[test]
fn test_membership_filter_fixture() {
    let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let path = fixtures_dir.join("membership_filter.json");
    load_and_run_fixture(&path);
}
