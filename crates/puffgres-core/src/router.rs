use crate::mapping::{Mapping, MembershipConfig};
use crate::types::RowEvent;

/// Routes events to their matching mappings.
pub struct Router {
    mappings: Vec<Mapping>,
}

impl Router {
    pub fn new(mappings: Vec<Mapping>) -> Self {
        Self { mappings }
    }

    /// Find all mappings that match a given event.
    /// Returns references to matching mappings.
    pub fn route<'a>(&'a self, event: &RowEvent) -> Vec<&'a Mapping> {
        self.mappings
            .iter()
            .filter(|m| self.matches(m, event))
            .collect()
    }

    /// Check if a mapping matches an event.
    fn matches(&self, mapping: &Mapping, event: &RowEvent) -> bool {
        // First check source relation
        if !mapping.source.matches(&event.schema, &event.table) {
            return false;
        }

        // Then evaluate membership predicate
        self.evaluate_membership(&mapping.membership, event)
    }

    /// Evaluate membership predicate against an event.
    fn evaluate_membership(&self, membership: &MembershipConfig, event: &RowEvent) -> bool {
        match membership {
            MembershipConfig::All | MembershipConfig::View => true,
            MembershipConfig::Dsl(predicate) => {
                // For inserts/updates, check new row
                // For deletes, check old row (was the row a member before deletion?)
                if let Some(row) = event.row() {
                    predicate.evaluate(row)
                } else {
                    false
                }
            }
        }
    }
}

/// A routed event with its matching mapping.
#[derive(Debug)]
pub struct RoutedEvent<'a> {
    pub event: &'a RowEvent,
    pub mapping: &'a Mapping,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::MembershipConfig;
    use crate::predicate::Predicate;
    use crate::transform::IdType;
    use crate::types::{Operation, Value};
    use std::collections::HashMap;

    fn make_mapping(
        name: &str,
        schema: &str,
        table: &str,
        membership: MembershipConfig,
    ) -> Mapping {
        Mapping::builder(name)
            .namespace(name)
            .source(schema, table)
            .id("id", IdType::Uint)
            .membership(membership)
            .build()
            .unwrap()
    }

    fn make_event(schema: &str, table: &str, new: HashMap<String, Value>) -> RowEvent {
        RowEvent {
            op: Operation::Insert,
            schema: schema.into(),
            table: table.into(),
            new: Some(new),
            old: None,
            lsn: 100,
            txid: None,
            timestamp: None,
        }
    }

    #[test]
    fn test_router_source_matching() {
        let mappings = vec![
            make_mapping("users", "public", "users", MembershipConfig::All),
            make_mapping("posts", "public", "posts", MembershipConfig::All),
        ];
        let router = Router::new(mappings);

        let event = make_event(
            "public",
            "users",
            [("id".into(), Value::Int(1))].into_iter().collect(),
        );

        let matches = router.route(&event);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].name, "users");
    }

    #[test]
    fn test_router_no_match() {
        let mappings = vec![make_mapping(
            "users",
            "public",
            "users",
            MembershipConfig::All,
        )];
        let router = Router::new(mappings);

        let event = make_event(
            "public",
            "posts",
            [("id".into(), Value::Int(1))].into_iter().collect(),
        );

        let matches = router.route(&event);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_router_membership_predicate() {
        let predicate = Predicate::parse("status = 'active'").unwrap();
        let mappings = vec![make_mapping(
            "active_users",
            "public",
            "users",
            MembershipConfig::Dsl(predicate),
        )];
        let router = Router::new(mappings);

        // Active user should match
        let event = make_event(
            "public",
            "users",
            [
                ("id".into(), Value::Int(1)),
                ("status".into(), Value::String("active".into())),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(router.route(&event).len(), 1);

        // Inactive user should not match
        let event = make_event(
            "public",
            "users",
            [
                ("id".into(), Value::Int(2)),
                ("status".into(), Value::String("inactive".into())),
            ]
            .into_iter()
            .collect(),
        );
        assert!(router.route(&event).is_empty());
    }

    #[test]
    fn test_router_multiple_mappings_same_source() {
        let active_pred = Predicate::parse("status = 'active'").unwrap();
        let deleted_pred = Predicate::parse("deleted_at IS NOT NULL").unwrap();

        let mappings = vec![
            make_mapping(
                "active_users",
                "public",
                "users",
                MembershipConfig::Dsl(active_pred),
            ),
            make_mapping(
                "deleted_users",
                "public",
                "users",
                MembershipConfig::Dsl(deleted_pred),
            ),
        ];
        let router = Router::new(mappings);

        // Active user with deleted_at should match deleted_users only
        let event = make_event(
            "public",
            "users",
            [
                ("id".into(), Value::Int(1)),
                ("status".into(), Value::String("inactive".into())),
                ("deleted_at".into(), Value::String("2024-01-01".into())),
            ]
            .into_iter()
            .collect(),
        );

        let matches = router.route(&event);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].name, "deleted_users");
    }
}
