# Agent Rules for Puffgres

Guidelines for coding agents contributing to this project.

## General Principles

1. **Small PRs**: Each change should be focused and reviewable
2. **Tests first**: Write or update tests before implementing features
3. **No refactors**: Don't refactor unrelated code in the same PR
4. **Incremental**: Add dependencies and code only as needed

## Code Style

- Follow Rust conventions (`cargo fmt`, `cargo clippy`)
- Prefer explicit types over inference in public APIs
- Use `thiserror` for library errors, `anyhow` for CLI errors (when added)
- Keep functions small and focused

## Testing

- Unit tests live alongside code in `mod tests`
- Integration tests go in `tests/`
- Fixture-based tests use JSON files in `tests/fixtures/`
- Use descriptive test names: `test_predicate_evaluates_null_check`

## Workspace Structure

```
crates/
  puffgres-cli/     # CLI binary
  puffgres-config/  # Config parsing (add when needed)
  puffgres-core/    # Core engine (add when needed)
  puffgres-state/   # State storage (add when needed)
  puffgres-tp/      # Turbopuffer client (add when needed)
  puffgres-pg/      # Postgres adapters (add when needed)
  puffgres-js/      # JS transforms (add when needed)
tests/
  fixtures/         # JSON test fixtures
```

## Commit Messages

- Use imperative mood: "Add feature" not "Added feature"
- First line: brief summary (50 chars)
- Body: explain why, not what

## Dependencies

- Add to `[workspace.dependencies]` in root `Cargo.toml`
- Reference with `{ workspace = true }` in crate `Cargo.toml`
- Only add dependencies when actually needed
