use crate::error::{Error, Result};
use crate::types::{RowMap, Value};

/// A parsed predicate expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Always true.
    True,
    /// Always false.
    False,
    /// Column equals a literal value.
    Eq(String, Literal),
    /// Column does not equal a literal value.
    NotEq(String, Literal),
    /// Column is null.
    IsNull(String),
    /// Column is not null.
    IsNotNull(String),
    /// Logical AND of predicates.
    And(Box<Predicate>, Box<Predicate>),
    /// Logical OR of predicates.
    Or(Box<Predicate>, Box<Predicate>),
    /// Logical NOT of a predicate.
    Not(Box<Predicate>),
}

/// A literal value in a predicate.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl Literal {
    fn matches(&self, value: &Value) -> bool {
        match (self, value) {
            (Literal::Null, Value::Null) => true,
            (Literal::Bool(a), Value::Bool(b)) => a == b,
            (Literal::Int(a), Value::Int(b)) => a == b,
            (Literal::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
            (Literal::Int(a), Value::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
            (Literal::Float(a), Value::Int(b)) => (a - *b as f64).abs() < f64::EPSILON,
            (Literal::String(a), Value::String(b)) => a == b,
            _ => false,
        }
    }
}

impl Predicate {
    /// Evaluate the predicate against a row.
    pub fn evaluate(&self, row: &RowMap) -> bool {
        match self {
            Predicate::True => true,
            Predicate::False => false,
            Predicate::Eq(col, lit) => row.get(col).map(|v| lit.matches(v)).unwrap_or(false),
            Predicate::NotEq(col, lit) => row.get(col).map(|v| !lit.matches(v)).unwrap_or(true),
            Predicate::IsNull(col) => row.get(col).map(|v| v.is_null()).unwrap_or(true),
            Predicate::IsNotNull(col) => row.get(col).map(|v| !v.is_null()).unwrap_or(false),
            Predicate::And(a, b) => a.evaluate(row) && b.evaluate(row),
            Predicate::Or(a, b) => a.evaluate(row) || b.evaluate(row),
            Predicate::Not(p) => !p.evaluate(row),
        }
    }

    /// Parse a predicate from a DSL string.
    pub fn parse(input: &str) -> Result<Self> {
        let mut parser = Parser::new(input);
        parser.parse_expression()
    }
}

/// Token types for the predicate DSL.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    String(String),
    Int(i64),
    Float(f64),
    True,
    False,
    Null,
    Eq,
    NotEq,
    Is,
    Not,
    And,
    Or,
    LParen,
    RParen,
    Eof,
}

struct Lexer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn advance(&mut self) {
        if let Some(c) = self.peek_char() {
            self.pos += c.len_utf8();
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_string()
    }

    fn read_number(&mut self) -> Token {
        let start = self.pos;
        let mut has_dot = false;

        if self.peek_char() == Some('-') {
            self.advance();
        }

        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() {
                self.advance();
            } else if c == '.' && !has_dot {
                has_dot = true;
                self.advance();
            } else {
                break;
            }
        }

        let s = &self.input[start..self.pos];
        if has_dot {
            Token::Float(s.parse().unwrap_or(0.0))
        } else {
            Token::Int(s.parse().unwrap_or(0))
        }
    }

    fn read_string(&mut self) -> String {
        self.advance(); // skip opening quote
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c == '\'' {
                break;
            }
            self.advance();
        }
        let s = self.input[start..self.pos].to_string();
        self.advance(); // skip closing quote
        s
    }

    fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        let Some(c) = self.peek_char() else {
            return Token::Eof;
        };

        match c {
            '(' => {
                self.advance();
                Token::LParen
            }
            ')' => {
                self.advance();
                Token::RParen
            }
            '=' => {
                self.advance();
                Token::Eq
            }
            '!' => {
                self.advance();
                if self.peek_char() == Some('=') {
                    self.advance();
                    Token::NotEq
                } else {
                    Token::Not
                }
            }
            '\'' => Token::String(self.read_string()),
            c if c.is_ascii_digit() || c == '-' => self.read_number(),
            c if c.is_alphabetic() || c == '_' => {
                let ident = self.read_ident();
                match ident.to_uppercase().as_str() {
                    "TRUE" => Token::True,
                    "FALSE" => Token::False,
                    "NULL" => Token::Null,
                    "IS" => Token::Is,
                    "NOT" => Token::Not,
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    _ => Token::Ident(ident),
                }
            }
            _ => {
                self.advance();
                self.next_token()
            }
        }
    }
}

struct Parser<'a> {
    lexer: Lexer<'a>,
    current: Token,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        let mut lexer = Lexer::new(input);
        let current = lexer.next_token();
        Self { lexer, current }
    }

    fn advance(&mut self) {
        self.current = self.lexer.next_token();
    }

    fn parse_expression(&mut self) -> Result<Predicate> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Predicate> {
        let mut left = self.parse_and()?;

        while self.current == Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = Predicate::Or(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Predicate> {
        let mut left = self.parse_not()?;

        while self.current == Token::And {
            self.advance();
            let right = self.parse_not()?;
            left = Predicate::And(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Predicate> {
        if self.current == Token::Not {
            self.advance();
            let inner = self.parse_not()?;
            Ok(Predicate::Not(Box::new(inner)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<Predicate> {
        match &self.current {
            Token::LParen => {
                self.advance();
                let expr = self.parse_expression()?;
                if self.current != Token::RParen {
                    return Err(Error::PredicateError("expected ')'".into()));
                }
                self.advance();
                Ok(expr)
            }
            Token::True => {
                self.advance();
                Ok(Predicate::True)
            }
            Token::False => {
                self.advance();
                Ok(Predicate::False)
            }
            Token::Ident(name) => {
                let name = name.clone();
                self.advance();
                self.parse_comparison(name)
            }
            _ => Err(Error::PredicateError(format!(
                "unexpected token: {:?}",
                self.current
            ))),
        }
    }

    fn parse_comparison(&mut self, column: String) -> Result<Predicate> {
        match &self.current {
            Token::Eq => {
                self.advance();
                let lit = self.parse_literal()?;
                Ok(Predicate::Eq(column, lit))
            }
            Token::NotEq => {
                self.advance();
                let lit = self.parse_literal()?;
                Ok(Predicate::NotEq(column, lit))
            }
            Token::Is => {
                self.advance();
                if self.current == Token::Not {
                    self.advance();
                    if self.current != Token::Null {
                        return Err(Error::PredicateError("expected NULL after IS NOT".into()));
                    }
                    self.advance();
                    Ok(Predicate::IsNotNull(column))
                } else if self.current == Token::Null {
                    self.advance();
                    Ok(Predicate::IsNull(column))
                } else {
                    Err(Error::PredicateError(
                        "expected NULL or NOT after IS".into(),
                    ))
                }
            }
            _ => Err(Error::PredicateError(format!(
                "expected comparison operator, got {:?}",
                self.current
            ))),
        }
    }

    fn parse_literal(&mut self) -> Result<Literal> {
        let lit = match &self.current {
            Token::Null => Literal::Null,
            Token::True => Literal::Bool(true),
            Token::False => Literal::Bool(false),
            Token::Int(i) => Literal::Int(*i),
            Token::Float(f) => Literal::Float(*f),
            Token::String(s) => Literal::String(s.clone()),
            _ => {
                return Err(Error::PredicateError(format!(
                    "expected literal, got {:?}",
                    self.current
                )))
            }
        };
        self.advance();
        Ok(lit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, Value)]) -> RowMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn test_predicate_true_false() {
        let row = row(&[]);
        assert!(Predicate::True.evaluate(&row));
        assert!(!Predicate::False.evaluate(&row));
    }

    #[test]
    fn test_predicate_eq() {
        let row = row(&[
            ("status", Value::String("active".into())),
            ("count", Value::Int(42)),
        ]);

        let p = Predicate::parse("status = 'active'").unwrap();
        assert!(p.evaluate(&row));

        let p = Predicate::parse("status = 'inactive'").unwrap();
        assert!(!p.evaluate(&row));

        let p = Predicate::parse("count = 42").unwrap();
        assert!(p.evaluate(&row));
    }

    #[test]
    fn test_predicate_not_eq() {
        let row = row(&[("status", Value::String("active".into()))]);

        let p = Predicate::parse("status != 'inactive'").unwrap();
        assert!(p.evaluate(&row));

        let p = Predicate::parse("status != 'active'").unwrap();
        assert!(!p.evaluate(&row));
    }

    #[test]
    fn test_predicate_is_null() {
        let row = row(&[
            ("name", Value::String("test".into())),
            ("deleted_at", Value::Null),
        ]);

        let p = Predicate::parse("deleted_at IS NULL").unwrap();
        assert!(p.evaluate(&row));

        let p = Predicate::parse("name IS NULL").unwrap();
        assert!(!p.evaluate(&row));

        let p = Predicate::parse("name IS NOT NULL").unwrap();
        assert!(p.evaluate(&row));

        let p = Predicate::parse("deleted_at IS NOT NULL").unwrap();
        assert!(!p.evaluate(&row));
    }

    #[test]
    fn test_predicate_and() {
        let row = row(&[
            ("status", Value::String("active".into())),
            ("public", Value::Bool(true)),
        ]);

        let p = Predicate::parse("status = 'active' AND public = true").unwrap();
        assert!(p.evaluate(&row));

        let p = Predicate::parse("status = 'inactive' AND public = true").unwrap();
        assert!(!p.evaluate(&row));
    }

    #[test]
    fn test_predicate_or() {
        let row = row(&[("status", Value::String("active".into()))]);

        let p = Predicate::parse("status = 'active' OR status = 'pending'").unwrap();
        assert!(p.evaluate(&row));

        let p = Predicate::parse("status = 'inactive' OR status = 'pending'").unwrap();
        assert!(!p.evaluate(&row));
    }

    #[test]
    fn test_predicate_not() {
        let row = row(&[("status", Value::String("active".into()))]);

        let p = Predicate::parse("NOT status = 'inactive'").unwrap();
        assert!(p.evaluate(&row));

        let p = Predicate::parse("NOT status = 'active'").unwrap();
        assert!(!p.evaluate(&row));
    }

    #[test]
    fn test_predicate_parentheses() {
        let row = row(&[
            ("a", Value::Int(1)),
            ("b", Value::Int(2)),
            ("c", Value::Int(3)),
        ]);

        // Without parens: a=1 OR (b=2 AND c=4) - should be true
        let p = Predicate::parse("a = 1 OR b = 2 AND c = 4").unwrap();
        assert!(p.evaluate(&row));

        // With parens: (a=1 OR b=2) AND c=4 - should be false
        let p = Predicate::parse("(a = 1 OR b = 2) AND c = 4").unwrap();
        assert!(!p.evaluate(&row));
    }

    #[test]
    fn test_predicate_complex() {
        let row = row(&[
            ("status", Value::String("published".into())),
            ("deleted_at", Value::Null),
            ("view_count", Value::Int(100)),
        ]);

        let p =
            Predicate::parse("status = 'published' AND deleted_at IS NULL AND view_count = 100")
                .unwrap();
        assert!(p.evaluate(&row));
    }

    #[test]
    fn test_predicate_missing_column() {
        let row = row(&[("a", Value::Int(1))]);

        // Missing column treated as NULL for IS NULL
        let p = Predicate::parse("missing IS NULL").unwrap();
        assert!(p.evaluate(&row));

        // Missing column: equality check returns false
        let p = Predicate::parse("missing = 1").unwrap();
        assert!(!p.evaluate(&row));

        // Missing column: inequality returns true (NULL != any value)
        let p = Predicate::parse("missing != 1").unwrap();
        assert!(p.evaluate(&row));
    }

    #[test]
    fn test_predicate_bool_literals() {
        let row = row(&[("active", Value::Bool(true))]);

        let p = Predicate::parse("active = true").unwrap();
        assert!(p.evaluate(&row));

        let p = Predicate::parse("active = false").unwrap();
        assert!(!p.evaluate(&row));
    }
}
