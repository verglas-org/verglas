#!/usr/bin/env bash
# Emits the Cargo package names whose code changed between two revisions.

set -euo pipefail

usage() {
  echo "usage: $0 <base-revision> <head-revision> | --all" >&2
  exit 2
}

if [[ $# -eq 1 && "$1" == "--all" ]]; then
  cargo metadata --format-version 1 --no-deps \
    | jq -c '[.workspace_members[] as $id | .packages[] | select(.id == $id) | .name] | sort'
  exit 0
fi

[[ $# -eq 2 ]] || usage
base_revision="$1"
head_revision="$2"
repo_root="$(git rev-parse --show-toplevel)"
metadata="$(cargo metadata --format-version 1 --no-deps)"

changed_paths=("")
while IFS= read -r -d '' path; do
  changed_paths+=("$path")
done < <(git diff --name-only -z --diff-filter=ACMRD "$base_revision...$head_revision")

for path in "${changed_paths[@]}"; do
  case "$path" in
    Cargo.toml|Cargo.lock|rust-toolchain|rust-toolchain.toml|.cargo/*)
      jq -c '[.workspace_members[] as $id | .packages[] | select(.id == $id) | .name] | sort' \
        <<< "$metadata"
      exit 0
      ;;
  esac
done

selected=("")
while IFS=$'\t' read -r package_name manifest_path; do
  package_dir="${manifest_path%/Cargo.toml}"
  package_dir="${package_dir#"$repo_root"/}"

  for path in "${changed_paths[@]}"; do
    if [[ "$path" == "$package_dir/Cargo.toml" ]]; then
      selected+=("$package_name")
      break
    fi

    case "$path" in
      "$package_dir"/build.rs|"$package_dir"/src/*|"$package_dir"/tests/*|"$package_dir"/benches/*|"$package_dir"/examples/*)
        selected+=("$package_name")
        break
        ;;
    esac
  done
done < <(
  jq -r '.workspace_members[] as $id | .packages[] | select(.id == $id) | [.name, .manifest_path] | @tsv' \
    <<< "$metadata"
)

printf '%s\n' "${selected[@]}" \
  | jq -Rsc 'split("\n") | map(select(length > 0)) | unique | sort'
