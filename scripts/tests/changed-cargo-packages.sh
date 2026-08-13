#!/usr/bin/env bash
# Tests changed-cargo-packages.sh against a temporary two-package workspace.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
selector="$repo_root/scripts/changed-cargo-packages.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

assert_packages() {
  local expected="$1"
  local base="$2"
  local head="$3"
  local actual

  actual="$(cd "$fixture" && "$selector" "$base" "$head")"
  if [[ "$actual" != "$expected" ]]; then
    echo "expected $expected, got $actual" >&2
    return 1
  fi
}

mkdir -p "$fixture/crates/alpha/src" "$fixture/crates/beta/src"
printf '%s\n' \
  '[workspace]' \
  'resolver = "2"' \
  'members = ["crates/alpha", "crates/beta"]' \
  > "$fixture/Cargo.toml"
printf '%s\n' '[package]' 'name = "alpha"' 'version = "0.1.0"' 'edition = "2024"' \
  > "$fixture/crates/alpha/Cargo.toml"
printf '%s\n' 'pub fn alpha() {}' > "$fixture/crates/alpha/src/lib.rs"
printf '%s\n' '[package]' 'name = "beta"' 'version = "0.1.0"' 'edition = "2024"' \
  > "$fixture/crates/beta/Cargo.toml"
printf '%s\n' 'pub fn beta() {}' > "$fixture/crates/beta/src/lib.rs"

git -C "$fixture" init -q
git -C "$fixture" checkout -qb fixture
git -C "$fixture" config user.name "CI selector test"
git -C "$fixture" config user.email "ci-selector@example.invalid"
(cd "$fixture" && cargo metadata --format-version 1 --no-deps >/dev/null)
git -C "$fixture" add .
git -C "$fixture" commit -qm "base"
base="$(git -C "$fixture" rev-parse HEAD)"

printf '%s\n' 'pub fn alpha() { println!("changed"); }' > "$fixture/crates/alpha/src/lib.rs"
git -C "$fixture" add crates/alpha/src/lib.rs
git -C "$fixture" commit -qm "change alpha"
head="$(git -C "$fixture" rev-parse HEAD)"
assert_packages '["alpha"]' "$base" "$head"

base="$head"
mkdir -p "$fixture/crates/alpha/tests"
printf '%s\n' '#[test]' 'fn alpha_works() {}' > "$fixture/crates/alpha/tests/alpha.rs"
printf '%s\n' 'pub fn beta() { println!("changed"); }' > "$fixture/crates/beta/src/lib.rs"
git -C "$fixture" add crates/alpha/tests/alpha.rs crates/beta/src/lib.rs
git -C "$fixture" commit -qm "change two packages"
head="$(git -C "$fixture" rev-parse HEAD)"
assert_packages '["alpha","beta"]' "$base" "$head"

base="$head"
printf '\n' >> "$fixture/Cargo.lock"
git -C "$fixture" add Cargo.lock
git -C "$fixture" commit -qm "change workspace lockfile"
head="$(git -C "$fixture" rev-parse HEAD)"
assert_packages '["alpha","beta"]' "$base" "$head"

base="$head"
printf '%s\n' '# Documentation only' > "$fixture/crates/alpha/README.md"
git -C "$fixture" add crates/alpha/README.md
git -C "$fixture" commit -qm "change package documentation"
head="$(git -C "$fixture" rev-parse HEAD)"
assert_packages '[]' "$base" "$head"

echo "changed Cargo package selection passed"
