"""
Contract tests for app.py.

Nothing else in the suite imports app.py — it is the repository's only user
interface (~1,900 lines of Streamlit) and it is otherwise untested. These
tests are a guard, not a rewrite: they catch attribute-shape mistakes (an
analysis function reaching for `discovery.archives`, which does not exist,
instead of `discovery.file_details`) statically, without needing a running
Streamlit session or real scan data.
"""
import ast
import dataclasses
import importlib
from pathlib import Path

from takeout_scout.hashing import HashIndex
from takeout_scout.models import FileDetails, TakeoutDiscovery


def test_app_module_imports() -> None:
    """app.py must at least import. Nothing else in the suite touches it.

    Importing app.py runs its module-level side effects — `STATE_DIR.mkdir(
    parents=True, exist_ok=True)` and `ensure_directories()` — against the
    real filesystem. Both are idempotent against directories that already
    exist, so importing it here (and possibly more than once, across test
    runs) is safe and does not require mocking Streamlit.
    """
    module = importlib.import_module("app")
    assert module is not None


# Local variable names in app.py that consistently hold instances of a known
# class, and the class each one holds. Used to statically check that every
# `name.attr` access in app.py refers to a real attribute of that class.
KNOWN_OBJECTS = {
    "discovery": TakeoutDiscovery,
    "fd": FileDetails,
    "hash_index": HashIndex,
}


def _valid_attrs_for(cls) -> set:
    """The set of legitimate attribute names for one of KNOWN_OBJECTS's classes."""
    if dataclasses.is_dataclass(cls):
        return {f.name for f in dataclasses.fields(cls)} | set(dir(cls))
    # HashIndex: _by_hash and _by_path are created in __init__, so they do
    # not appear on the class itself — it must be instantiated to see them.
    return set(dir(cls()))


def _collect_known_object_attr_accesses(source: str):
    """Walk app.py's AST for `name.attr` accesses on names in KNOWN_OBJECTS.

    Returns a list of (line_number, name, attr) tuples.
    """
    tree = ast.parse(source, filename="app.py")
    accesses = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id in KNOWN_OBJECTS
        ):
            accesses.append((node.lineno, node.value.id, node.attr))
    return accesses


def _app_source() -> str:
    app_path = Path(__file__).resolve().parent.parent / "app.py"
    return app_path.read_text(encoding="utf-8")


def test_known_object_attribute_accesses_are_real() -> None:
    """Every `discovery.x` / `fd.x` / `hash_index.x` access in app.py must
    name a real attribute of the corresponding class.

    This test would have caught bug 1 (discovery.archives / .directories,
    which do not exist — the files are a flat list, discovery.file_details)
    and bug 2 (hash_index._index, which does not exist — the real attribute
    is _by_hash) exactly, because both are attribute-name mistakes visible
    from source alone.

    It CANNOT catch bug 3 (HashIndex entries being 3-tuples, unpacked as if
    they were 2-tuples) — that is a call-shape mistake, not an attribute
    name, and nothing static can catch it. That is what
    tests/test_hashing.py's summarize_sources tests are for.
    """
    valid_attrs = {name: _valid_attrs_for(cls) for name, cls in KNOWN_OBJECTS.items()}

    violations = []
    for lineno, name, attr in _collect_known_object_attr_accesses(_app_source()):
        if attr not in valid_attrs[name]:
            violations.append(f"app.py:{lineno}: {name}.{attr}")

    assert not violations, (
        "app.py accesses attributes that do not exist on their known "
        "objects:\n" + "\n".join(violations)
    )


def test_guard_actually_inspects_something() -> None:
    """A guard that collected no accesses would pass silently forever.

    Pins the AST walk itself: if it stopped matching anything (e.g. because
    app.py was refactored to use different local variable names, or the
    walker broke), test_known_object_attribute_accesses_are_real would pass
    vacuously. This asserts there is real material under inspection.
    """
    accesses = _collect_known_object_attr_accesses(_app_source())

    assert len(accesses) >= 20, (
        f"expected at least 20 known-object attribute accesses in app.py, "
        f"found {len(accesses)} — the AST walk may be broken"
    )

    names_seen = {name for _, name, _ in accesses}
    assert names_seen == set(KNOWN_OBJECTS), (
        f"expected accesses on all of {set(KNOWN_OBJECTS)}, only saw {names_seen}"
    )
