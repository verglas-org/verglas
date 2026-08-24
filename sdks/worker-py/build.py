"""Build a Python Durable Object project into a Verglas component.

The builder accepts the small Wrangler manifest used by this prototype, creates
one temporary entry module that installs the public Python shim, and delegates
component generation to the pinned componentize-py executable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


COMPONENTIZE_PY_VERSION = "0.25.0"
_PACKAGE_DIR = Path(__file__).resolve().parent
_REPOSITORY_DIR = _PACKAGE_DIR.parents[1]
_WIT_DIR = _REPOSITORY_DIR / "crates" / "verglas-do-wasm" / "wit"
_COMPONENTIZE_PY = _PACKAGE_DIR / ".venv" / "bin" / "componentize-py"


class ManifestError(ValueError):
    """Reports a manifest outside the supported Wrangler subset."""


class BuildError(RuntimeError):
    """Reports a failure before a component can be written."""


@dataclass(frozen=True)
class Manifest:
    """Contains the validated project fields consumed by the builder."""

    name: str
    main: Path
    bindings: list[dict[str, str]]


@dataclass(frozen=True)
class BuildResult:
    """Describes the content-addressed component and output manifest."""

    name: str
    component_digest: str
    component_path: Path
    manifest_path: Path
    component_bytes: bytes
    bindings: list[dict[str, str]]


def _strip_jsonc_comments(source: str) -> str:
    """Remove JSONC comments while preserving quoted strings and newlines."""
    output: list[str] = []
    in_string = False
    escaped = False
    in_line_comment = False
    in_block_comment = False
    index = 0

    while index < len(source):
        character = source[index]
        next_character = source[index + 1] if index + 1 < len(source) else ""

        if in_line_comment:
            if character == "\n":
                in_line_comment = False
                output.append(character)
            else:
                output.append(" ")
            index += 1
            continue

        if in_block_comment:
            if character == "*" and next_character == "/":
                in_block_comment = False
                output.append("  ")
                index += 2
            else:
                output.append("\n" if character == "\n" else " ")
                index += 1
            continue

        if in_string:
            output.append(character)
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            index += 1
            continue

        if character == '"':
            in_string = True
            output.append(character)
            index += 1
        elif character == "/" and next_character == "/":
            in_line_comment = True
            output.append("  ")
            index += 2
        elif character == "/" and next_character == "*":
            in_block_comment = True
            output.append("  ")
            index += 2
        else:
            output.append(character)
            index += 1

    if in_block_comment:
        raise ManifestError("unterminated block comment in wrangler.jsonc")
    return "".join(output)


def _strip_trailing_commas(source: str) -> str:
    """Remove commas before object or array terminators outside strings."""
    output: list[str] = []
    in_string = False
    escaped = False
    index = 0

    while index < len(source):
        character = source[index]
        if in_string:
            output.append(character)
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            index += 1
            continue

        if character == '"':
            in_string = True
            output.append(character)
            index += 1
            continue

        if character == ",":
            lookahead = index + 1
            while lookahead < len(source) and source[lookahead].isspace():
                lookahead += 1
            if lookahead < len(source) and source[lookahead] in "}]":
                index += 1
                continue

        output.append(character)
        index += 1

    return "".join(output)


def parse_jsonc(source: str) -> Any:
    """Parse one JSONC document with comments and trailing commas."""
    try:
        cleaned = _strip_trailing_commas(_strip_jsonc_comments(source))
        return json.loads(cleaned)
    except ManifestError:
        raise
    except json.JSONDecodeError as error:
        raise ManifestError(f"invalid wrangler.jsonc: {error}") from error


def _reject_unknown_keys(object_value: dict[str, Any], allowed: set[str], path: str) -> None:
    """Reject keys outside one explicitly supported manifest object shape."""
    for key in object_value:
        if key not in allowed:
            raise ManifestError(f"unknown {path} key: {key}")


def _required_string(object_value: dict[str, Any], field: str, path: str) -> str:
    """Read one required, non-empty string field from a manifest object."""
    value = object_value.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ManifestError(f"{path}.{field} is required and must be a non-empty string")
    return value


def _parse_manifest_data(raw: Any) -> tuple[str, str, list[dict[str, str]]]:
    """Validate the supported Wrangler fields without resolving project paths."""
    if not isinstance(raw, dict):
        raise ManifestError("wrangler.jsonc must contain a JSON object")
    _reject_unknown_keys(raw, {"name", "main", "durable_objects"}, "top-level")

    name = _required_string(raw, "name", "manifest")
    main = _required_string(raw, "main", "manifest")
    durable_objects = raw.get("durable_objects")
    bindings: list[dict[str, str]] = []

    if "durable_objects" in raw:
        if not isinstance(durable_objects, dict):
            raise ManifestError("manifest.durable_objects must be an object")
        _reject_unknown_keys(durable_objects, {"bindings"}, "durable_objects")
        raw_bindings = durable_objects.get("bindings")
        if not isinstance(raw_bindings, list):
            raise ManifestError(
                "manifest.durable_objects.bindings is required and must be an array"
            )
        for index, raw_binding in enumerate(raw_bindings):
            path = f"manifest.durable_objects.bindings[{index}]"
            if not isinstance(raw_binding, dict):
                raise ManifestError(f"{path} must be an object")
            _reject_unknown_keys(raw_binding, {"name", "class_name"}, path)
            bindings.append(
                {
                    "name": _required_string(raw_binding, "name", path),
                    "class_name": _required_string(raw_binding, "class_name", path),
                }
            )

    names: set[str] = set()
    for binding in bindings:
        if binding["name"] in names:
            raise ManifestError(
                f"duplicate durable object binding name: {binding['name']}"
            )
        names.add(binding["name"])

    return name, main, bindings


def load_manifest(project_dir: str | os.PathLike[str]) -> Manifest:
    """Read and validate a project's ``wrangler.jsonc`` and Python main file."""
    project = Path(os.path.abspath(os.fspath(project_dir)))
    manifest_path = project / "wrangler.jsonc"
    try:
        source = manifest_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ManifestError(f"cannot read {manifest_path}: {error}") from error

    name, main_name, bindings = _parse_manifest_data(parse_jsonc(source))
    main = Path(os.path.abspath(os.path.join(os.fspath(project), main_name)))
    if not main.resolve().is_relative_to(project.resolve()):
        raise ManifestError("manifest.main must name a file inside the project directory")
    if main.suffix != ".py":
        raise ManifestError("manifest.main must name a .py module")
    if not main.is_file():
        raise ManifestError(f"manifest.main does not exist: {main_name}")

    relative = main.relative_to(project)
    if relative.name == "__init__.py":
        raise ManifestError("manifest.main must name a Python module, not __init__.py")
    module_parts = relative.with_suffix("").parts
    if any(not part.isidentifier() for part in module_parts):
        raise ManifestError(
            "manifest.main path components must be valid Python identifiers"
        )

    return Manifest(name=name, main=main, bindings=bindings)


def _main_module_name(manifest: Manifest, project_dir: Path) -> str:
    """Convert a validated main path into its importable Python module name."""
    project = Path(os.path.abspath(os.fspath(project_dir)))
    relative = manifest.main.relative_to(project)
    return ".".join(relative.with_suffix("").parts)


def _componentize_executable() -> Path:
    """Return the repository-local pinned componentize-py executable."""
    if not _COMPONENTIZE_PY.is_file():
        raise BuildError(
            f"missing {_COMPONENTIZE_PY}; create the local venv and install "
            "requirements.txt before building"
        )
    try:
        probe = subprocess.run(
            [str(_COMPONENTIZE_PY), "--version"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise BuildError(f"cannot execute {_COMPONENTIZE_PY}: {error}") from error
    version = (probe.stdout or probe.stderr).strip()
    expected = f"componentize-py {COMPONENTIZE_PY_VERSION}"
    if probe.returncode != 0 or version != expected:
        raise BuildError(f"expected {expected}, found {version or 'no version output'}")
    return _COMPONENTIZE_PY


def _run_componentize(
    project_dir: Path, manifest: Manifest, work_dir: Path, component_path: Path
) -> None:
    """Run the pinned componentize-py command for one temporary entry module."""
    componentize = _componentize_executable()
    entry_name = "verglas_worker_entry"
    module_name = _main_module_name(manifest, project_dir)
    entry_path = work_dir / f"{entry_name}.py"
    entry_path.write_text(
        "from importlib import import_module\n"
        "from verglas_worker._component import Handler, set_worker_module\n"
        f"set_worker_module(import_module({module_name!r}))\n",
        encoding="utf-8",
    )

    command = [
        str(componentize),
        "-d",
        str(_WIT_DIR),
        "-w",
        "durable-object",
        "componentize",
        entry_name,
        "-p",
        str(work_dir),
        "-p",
        str(_PACKAGE_DIR),
        "-p",
        str(project_dir),
        "--stub-wasi",
        "-o",
        str(component_path),
    ]
    environment = os.environ.copy()
    environment["VIRTUAL_ENV"] = str(_PACKAGE_DIR / ".venv")
    path = environment.get("PATH", "").split(os.pathsep)
    environment["PATH"] = os.pathsep.join(
        [str(_COMPONENTIZE_PY.parent), *[item for item in path if item]]
    )
    try:
        result = subprocess.run(
            command,
            cwd=_REPOSITORY_DIR,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise BuildError(f"componentize-py could not start: {error}") from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        if len(detail) > 4000:
            detail = detail[-4000:]
        raise BuildError(f"componentize-py failed: {detail or result.returncode}")


def _update_gateway_manifest(
    gateway_path: Path, output_dir: Path, component_digest: str
) -> None:
    """Update a project's checked-in gateway digest after writing the artifact."""
    try:
        source = gateway_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return
    except OSError as error:
        raise BuildError(f"cannot read gateway manifest {gateway_path}: {error}") from error
    try:
        gateway = json.loads(source)
    except json.JSONDecodeError as error:
        raise BuildError(f"gateway manifest is not valid JSON: {gateway_path}: {error}") from error
    if not isinstance(gateway, dict):
        raise BuildError(f"gateway manifest must contain an object: {gateway_path}")
    gateway["component_digest"] = component_digest
    if "component_dir" in gateway:
        gateway["component_dir"] = str(output_dir)
    try:
        gateway_path.write_text(json.dumps(gateway, indent=2) + "\n", encoding="utf-8")
    except OSError as error:
        raise BuildError(f"cannot update gateway manifest {gateway_path}: {error}") from error


def build_project(
    project_dir: str | os.PathLike[str],
    output_dir: str | os.PathLike[str],
    gateway_path: str | os.PathLike[str] | None = None,
) -> BuildResult:
    """Build one project and update its gateway manifest when present."""
    project = Path(project_dir).resolve()
    output = Path(output_dir).resolve()
    gateway = Path(gateway_path).resolve() if gateway_path is not None else project / "gateway.json"
    manifest = load_manifest(project)

    with tempfile.TemporaryDirectory(prefix="verglas-worker-py-") as temporary:
        work_dir = Path(temporary)
        component_path = work_dir / "worker.wasm"
        _run_componentize(project, manifest, work_dir, component_path)
        try:
            component_bytes = component_path.read_bytes()
        except OSError as error:
            raise BuildError(f"componentize-py did not write {component_path}: {error}") from error

    component_digest = hashlib.sha256(component_bytes).hexdigest()
    output.mkdir(parents=True, exist_ok=True)
    output_component = output / f"{component_digest}.wasm"
    output_component.write_bytes(component_bytes)
    output_manifest = output / "manifest.out.json"
    output_manifest.write_text(
        json.dumps(
            {
                "name": manifest.name,
                "component_digest": component_digest,
                "bindings": manifest.bindings,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    _update_gateway_manifest(gateway, output, component_digest)
    return BuildResult(
        name=manifest.name,
        component_digest=component_digest,
        component_path=output_component,
        manifest_path=output_manifest,
        component_bytes=component_bytes,
        bindings=manifest.bindings,
    )


def _parse_arguments(argv: list[str]) -> argparse.Namespace:
    """Parse the deliberately small build command line."""
    parser = argparse.ArgumentParser(
        usage="python sdks/worker-py/build.py <project-dir> --out <dir> [--gateway <path>]"
    )
    parser.add_argument("project_dir", type=Path)
    parser.add_argument("--out", dest="output_dir", type=Path, required=True)
    parser.add_argument("--gateway", dest="gateway_path", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Build the requested project and print its lowercase component digest."""
    arguments = _parse_arguments(sys.argv[1:] if argv is None else argv)
    result = build_project(
        arguments.project_dir, arguments.output_dir, arguments.gateway_path
    )
    print(result.component_digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
