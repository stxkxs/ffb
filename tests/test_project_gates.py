"""The build gates the repository commits to, pinned against silent relaxation."""

import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


def _mypy_config() -> dict[str, object]:
    with PYPROJECT.open("rb") as handle:
        config = tomllib.load(handle)
    mypy: dict[str, object] = config["tool"]["mypy"]
    return mypy


def test_mypy_runs_in_strict_mode() -> None:
    assert _mypy_config()["strict"] is True


def test_mypy_config_carves_out_no_strict_mode_check() -> None:
    """Every boolean under `[tool.mypy]` enables a check rather than disabling one.

    A `false` here reads as strict while exempting whichever check it names, so the
    only honest way to relax one is to drop `strict` itself.
    """
    disabled = [key for key, value in _mypy_config().items() if value is False]
    assert disabled == []


def test_mypy_suppressions_cover_only_stubless_dependencies() -> None:
    """No module under `src` buys its clean type check by silencing its own types.

    A dependency that ships no stubs is outside this repository's control, so
    `import-untyped` is the one code an inline suppression may carry.
    """
    suppressed = [
        f"{path.relative_to(REPO_ROOT)}:{number}"
        for path in sorted((REPO_ROOT / "src").rglob("*.py"))
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if "type: ignore" in line and "type: ignore[import-untyped]" not in line
    ]
    assert suppressed == []


def test_ci_fails_on_unformatted_source() -> None:
    assert "uv run ruff format --check ." in CI_WORKFLOW.read_text()


def test_ci_fails_on_lint_type_test_and_build_failure() -> None:
    workflow = CI_WORKFLOW.read_text()
    for command in (
        "uv run ruff check .",
        "uv run mypy src",
        "uv run pytest --cov",
        "uv build",
    ):
        assert command in workflow
