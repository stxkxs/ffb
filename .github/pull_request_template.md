## Why

The problem or the goal. What breaks, what is missing, or what this unlocks. A diff shows what
changed; only you can supply the reason it is worth changing.

## What changed

The shape of the change, not a file list. Name the boundaries it crosses: which engine, which
screen, which part of the data layer, and any behaviour a user of the TUI sees differently.

## How tested

The commands you ran and what they told you. Name the tests that cover the change, and say plainly
which parts you exercised by hand in the TUI and which parts nothing covers.

---

`.github/workflows/ci.yml` runs ruff, mypy, pytest, `uv build`, and a pip-audit vulnerability scan
on every push and pull request. Report here only what those gates cannot check.
