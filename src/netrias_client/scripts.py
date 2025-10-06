"""Coordinate project command entry points.

'why': centralize developer workflows for `uv run` execution
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

_LOGGER = logging.getLogger("netrias_client.scripts")
_COMMANDS: Final[tuple[tuple[str, ...], ...]] = (
    ("pytest",),
    ("ruff", "check", "."),
    ("basedpyright", "."),
)
_REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
_PYPROJECT_PATH: Final[Path] = _REPO_ROOT / "pyproject.toml"
_PACKAGE_INIT_PATH: Final[Path] = Path(__file__).resolve().parent / "__init__.py"
_DIST_PATH: Final[Path] = _REPO_ROOT / "dist"
_VERSION_PATTERN: Final[re.Pattern[str]] = re.compile(r'^version\s*=\s*"(?P<value>[^"]+)"$', re.MULTILINE)
_INIT_VERSION_PATTERN: Final[re.Pattern[str]] = re.compile(r'^__version__\s*=\s*"(?P<value>[^"]+)"$', re.MULTILINE)
_REPOSITORY_CONFIG: Final[dict[str, tuple[str, str | None]]] = {
    "testpypi": ("TEST_PYPI_TOKEN", "https://test.pypi.org/legacy/"),
    "pypi": ("PYPI_TOKEN", None),
}


@dataclass(frozen=True)
class ReleaseOptions:
    """Capture release CLI arguments as structured options.

    'why': keep parsing separate from orchestration logic
    """

    version: str | None
    bump: str
    repository: str | None


def check() -> None:
    """Run the combined test and lint pipeline invoked by `uv run check`.

    'why': provide a single entry point that exits early on the first failure
    """

    _ensure_logging()
    for command in _COMMANDS:
        _run_command_or_raise(command)


def live_check() -> None:
    """Execute the live service smoke test harness.

    'why': exercise production endpoints without duplicating CLI plumbing
    """

    _run_command_or_raise(("python", "-m", "netrias_client.live_test.test"))


def release(argv: Sequence[str] | None = None) -> None:
    """Run the release pipeline: bump version, validate, build, publish.

    'why': streamline TestPyPI/PyPI releases from a single script
    """

    options = _parse_release_args(argv)
    _ensure_logging()
    target_version = _determine_target_version(options)
    _update_versions(target_version)
    _LOGGER.info("version synchronized → %s", target_version)
    check()
    artifacts = _build_distributions()
    _verify_artifacts(artifacts)
    if options.repository:
        _publish_artifacts(options.repository)


def _ensure_logging() -> None:
    """Provision a minimalist logging configuration for script execution."""

    if not _LOGGER.handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


def _run_command(command: Sequence[str], *, env: Mapping[str, str] | None = None, display_command: Sequence[str] | None = None) -> int:
    """Run `command` and return its exit status without raising on failure.

    'why': centralize subprocess logging and leave error handling to callers
    """

    shown = display_command or command
    _LOGGER.info("→ %s", " ".join(shown))
    completed = subprocess.run(command, check=False, env=dict(env) if env else None)
    return completed.returncode


def _run_command_or_raise(
    command: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    display_command: Sequence[str] | None = None,
) -> None:
    """Execute `command` and abort immediately on failure.

    'why': surface the first failing command to halt composite workflows
    """

    exit_code = _run_command(command, env=env, display_command=display_command)
    if exit_code != 0:
        raise SystemExit(exit_code)


def _parse_release_args(argv: Sequence[str] | None) -> ReleaseOptions:
    """Parse CLI arguments into a `ReleaseOptions` instance.

    'why': isolate argparse wiring for straightforward testing
    """

    parser = argparse.ArgumentParser(prog="uv run release")
    group = parser.add_mutually_exclusive_group()
    _ = group.add_argument("--version", help="Explicit semantic version to publish")
    _ = group.add_argument("--bump", choices=("patch", "minor", "major"), default="patch", help="Increment the current version")
    _ = parser.add_argument("--publish", choices=("testpypi", "pypi"), dest="repository", help="Publish artifacts after verification")
    namespace = parser.parse_args(list(argv) if argv is not None else sys.argv[1:])
    version_arg = cast(str | None, getattr(namespace, "version", None))
    bump_arg = cast(str | None, getattr(namespace, "bump", None))
    repository_arg = cast(str | None, getattr(namespace, "repository", None))
    bump = bump_arg or "patch"
    return ReleaseOptions(version=version_arg, bump=bump, repository=repository_arg)


def _determine_target_version(options: ReleaseOptions) -> str:
    """Decide the release version based on explicit input or a bump type.

    'why': ensure both pyproject and package versions stay aligned
    """

    current = _read_version(_PYPROJECT_PATH, _VERSION_PATTERN)
    _assert_versions_match(current)
    if options.version:
        return options.version
    return _bump_semver(current, options.bump)


def _assert_versions_match(expected: str) -> None:
    """Verify the package and pyproject versions are identical before bumping.

    'why': prevent partial version updates that ship inconsistent metadata
    """

    package_version = _read_version(_PACKAGE_INIT_PATH, _INIT_VERSION_PATTERN)
    if package_version != expected:
        message = " ".join(
            [
                f"Package version mismatch: pyproject.toml has {expected}",
                f"but src/netrias_client/__init__.py has {package_version}",
            ]
        )
        raise RuntimeError(message)


def _read_version(path: Path, pattern: re.Pattern[str]) -> str:
    """Extract a version string from `path` using `pattern`.

    'why': share parsing logic between pyproject and package metadata
    """

    text = path.read_text(encoding="utf-8")
    match = pattern.search(text)
    if match is None:
        raise RuntimeError(f"Could not locate version string in {path}")
    return match.group("value")


def _bump_semver(version: str, bump: str) -> str:
    """Return a new semantic version string incremented by `bump` type.

    'why': keep release increments predictable without external tooling
    """

    major_str, minor_str, patch_str = version.split(".")
    major, minor, patch = int(major_str), int(minor_str), int(patch_str)
    if bump == "major":
        return f"{major + 1}.0.0"
    if bump == "minor":
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def _update_versions(version: str) -> None:
    """Write `version` to pyproject.toml and the package __init__ module.

    'why': guarantee distribution metadata matches the Python package
    """

    _replace_version(_PYPROJECT_PATH, _VERSION_PATTERN, f'version = "{version}"')
    _replace_version(_PACKAGE_INIT_PATH, _INIT_VERSION_PATTERN, f'__version__ = "{version}"')


def _replace_version(path: Path, pattern: re.Pattern[str], replacement: str) -> None:
    """Swap the first version match in `path` with `replacement`.

    'why': avoid manual editing and keep formatting stable
    """

    text = path.read_text(encoding="utf-8")
    updated, count = pattern.subn(replacement, text, count=1)
    if count != 1:
        raise RuntimeError(f"Failed to update version in {path}")
    _ = path.write_text(updated, encoding="utf-8")


def _build_distributions() -> list[Path]:
    """Build wheel and sdist artifacts and return their paths.

    'why': provide a clean slate before handing artifacts to verifiers
    """

    if _DIST_PATH.exists():
        shutil.rmtree(_DIST_PATH)
    _run_command_or_raise(("uv", "build"))
    return sorted(_DIST_PATH.glob("*"))


def _verify_artifacts(artifacts: Sequence[Path]) -> None:
    """Run metadata checks and a local install smoke test for `artifacts`.

    'why': catch packaging issues before publishing to a remote index
    """

    dist_files = [
        path
        for path in artifacts
        if path.suffix == ".whl" or tuple(path.suffixes[-2:]) == (".tar", ".gz")
    ]
    if not dist_files:
        raise RuntimeError("No distribution artifacts produced; run `uv build` first")
    _run_command_or_raise(("uv", "run", "twine", "check", *[str(path) for path in dist_files]))
    _smoke_test_artifacts(dist_files)


def _smoke_test_artifacts(artifacts: Sequence[Path]) -> None:
    """Install the wheel into a temp venv and import the package.

    'why': validate that the built wheel installs and exposes metadata
    """

    wheel = next((path for path in artifacts if path.suffix == ".whl"), None)
    if wheel is None:
        raise RuntimeError("Wheel artifact missing; expected *.whl after build")
    with tempfile.TemporaryDirectory() as tmp_dir:
        env_path = Path(tmp_dir) / "venv"
        _create_virtualenv(env_path)
        python_path = _resolve_python(env_path)
        if not python_path.exists():
            raise RuntimeError(f"Smoke test interpreter missing: {python_path}")
        _LOGGER.info("smoke test interpreter → %s", python_path)
        _run_command_or_raise((str(python_path), "-m", "pip", "install", str(wheel)))
        _run_command_or_raise((str(python_path), "-c", "import netrias_client as pkg; print(pkg.__version__)"))


def _create_virtualenv(target: Path) -> None:
    """Create a fresh virtual environment at `target` with pip installed.

    'why': isolate smoke tests from the developer environment
    """

    _run_command_or_raise(("uv", "venv", str(target), "--seed"))


def _resolve_python(env_path: Path) -> Path:
    """Locate the Python executable underneath `env_path`.

    'why': handle platform differences without duplicating logic
    """

    bin_dir = env_path / ("Scripts" if sys.platform == "win32" else "bin")
    candidates = ("python", "python3", "python.exe")
    for candidate in candidates:
        python_path = bin_dir / candidate
        if python_path.exists():
            return python_path
    raise RuntimeError(f"Python executable not found under {bin_dir}")


def _publish_artifacts(repository: str) -> None:
    """Publish artifacts to the requested repository via `uv publish`.

    'why': keep credential handling opinionated yet minimal
    """

    if repository not in _REPOSITORY_CONFIG:
        raise RuntimeError(f"Unsupported repository '{repository}'")
    env_var, publish_url = _REPOSITORY_CONFIG[repository]
    token = os.environ.get(env_var)
    if not token:
        raise RuntimeError(f"Set {env_var} before publishing to {repository}")
    command: list[str] = ["uv", "publish", "--username", "__token__", "--password", token]
    display: list[str] = ["uv", "publish", "--username", "__token__", "--password", "******"]
    if publish_url:
        command.extend(["--publish-url", publish_url])
        display.extend(["--publish-url", publish_url])
    _run_command_or_raise(tuple(command), display_command=tuple(display))
