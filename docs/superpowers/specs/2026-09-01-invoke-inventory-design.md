# Invoking Takeout_Inventory for a deep pass — design

**Date:** 2026-09-01
**Status:** approved; not yet implemented.
Extends the architecture settled in
[`2026-09-01-retire-tkinter-ui-design.md`](2026-09-01-retire-tkinter-ui-design.md),
whose positioning paragraph currently says Scout and Inventory "share no code".
That sentence stays true after this work — see *The licence boundary*.

## The problem

Scout answers "what is in these archives?" quickly and in a browser. It does
that well. There is one question it answers **wrongly**, and it cannot tell.

Scout pairs a photo to its `.json` sidecar within a single archive
(`takeout_scout/discovery.py`). `Takeout_Inventory` measured the real export:

> **55,960 of 78,038 paired photos — 71.7% — have their sidecar in a different
> archive than the media file.**

Any tool reasoning one archive at a time gets this wrong roughly seven times in
ten. Scout has no way to know, because the evidence it needs is in a file it is
not looking at.

Inventory already solves this. It scans every archive, resolves pairings
globally through an eleven-rule ladder, and publishes the result as
`takeout-index.sqlite`. Nothing consumes it from Scout.

## The division of labour

**Inventory is machine-friendly. Scout is human-friendly and in the browser.**

That is the whole architecture. Scout is the front door: a person points it at
folders, gets a fast shallow answer, and is then *offered* a deep pass that
Scout does not perform itself.

```
Scout (GPL-3, browser)              Inventory (AGPL-3, CLI)
  quick scan
  counts, EXIF, hashes, duplicates
       |
       +-- offers the deep pass
       |        \__ subprocess: uv run takeout_inventory.py scan, then index
       |                        writes takeout-index.sqlite
       |
       +-- reads the index, read-only
                \__ renders a work list
```

**Neither tool gains a dependency on the other.** Scout without Inventory is
Scout as it is today. Inventory never learns Scout exists.

### What each side has that the other lacks

Measured, not assumed. Inventory performs **no EXIF extraction and no content
hashing** — its single `hashlib` use fingerprints a *filename*, not file bytes.

| | Scout | Inventory |
| --- | --- | --- |
| cross-archive media↔sidecar pairing | no | **yes** |
| pairing confidence (`own` / `related`) | no | **yes** |
| parsed sidecar metadata (date, GPS, trashed) | no | **yes** |
| EXIF (`photos_with_exif`, GPS, datetime) | **yes** | no |
| content hashes, duplicate detection | **yes** | no |
| service detection, multi-part grouping | **yes** | no |
| live / motion photo pairs | **yes** | no |

This is why the index cannot replace Scout's engine, and why "Scout becomes a
frontend for Inventory" is the wrong framing. They are complementary, and this
work connects them without merging them.

## The licence boundary

**Scout is GPL-3.0-or-later. Inventory is AGPL-3.0-or-later.**

If Scout `import`ed Inventory, the two would form a combined work in one
process, and AGPL §13 — the network clause — would reach a program that serves
a web UI. That is precisely the case the AGPL exists for, and it would
effectively make Scout AGPL.

Running Inventory as a **subprocess** keeps them separate programs communicating
through argv and a file on disk. The licence boundary stays exactly where it is
today, and the README's existing claim that the two "share no code" remains
literally true.

This is not a workaround. It is Inventory's designed interface: a PEP 723
single-file script meant to be run as `uv run takeout_inventory.py`, per its
own ADR-0003.

**Consequence:** Scout must never vendor, copy, or import `takeout_inventory.py`.
A test enforces this — see *Testing*.

## Scope

**In.** Locating Inventory; invoking `scan` then `index` as a subprocess with
live output; reading the published index read-only; rendering a work list; the
three UI touchpoints that expose all of it.

**Out.** Writing anything to the archives. Modifying Inventory. Replacing
Scout's scanning engine. Making either tool require the other. Any use of the
index for EXIF or duplicate analysis — the index carries neither.

## Components

Three of the four units are pure and testable without Streamlit. That is
deliberate: `app.py` has no test harness, so logic that can live outside it
does.

### `takeout_scout/inventory_runner.py` — new

Subprocess mechanics. No Streamlit import.

```python
@dataclass(frozen=True)
class InventoryTool:
    """A located takeout_inventory.py and how it was found."""
    script: Path
    source: str          # "sibling" | "remembered" | "manual"


def find_inventory(remembered: str | None = None) -> InventoryTool | None:
    """Locate takeout_inventory.py, or None if it is not present.

    Order: an explicit remembered path, then ../Takeout_Inventory/
    takeout_inventory.py relative to Scout's repository root. Returns None
    rather than raising - a missing Inventory is an ordinary state, not an
    error, and the deep-pass offer simply does not render.
    """


def deep_pass_commands(tool: InventoryTool, takeout_dir: Path) -> list[list[str]]:
    """The two commands to run, in order: scan, then index.

    Separate rather than combined so a failure names which phase failed.
    `takeout_dir` is the single directory Inventory will walk - see
    "Which directory the deep pass runs against".
    """


def run_streaming(cmd: list[str], cwd: Path) -> Iterator[str]:
    """Yield the subprocess's output lines as they arrive.

    Raises InventoryFailed with the exit code and the last lines of output
    when the command exits non-zero.
    """
```

**Exact commands**, verified against Inventory's argument parser:

```
uv run <script> scan  --takeout <dir>
uv run <script> index --out-sqlite <dir>/takeout-index.sqlite \
                      --out-json   <dir>/takeout-index.json
```

`scan` caches per archive on `name+size+mtime`, so a re-run over unchanged
archives is fast. That is what makes offering the deep pass repeatedly
reasonable.

**Why `uv run`:** Inventory declares its dependencies in a PEP 723 header
(`rich>=13.7`) and `requires-python = ">=3.11"`. `uv run` resolves both without
Scout knowing or caring what they are — which is exactly the arm's-length
relationship the licence boundary needs.

**Output encoding:** `rich` detects that stdout is not a terminal and emits
plain text without ANSI escapes or live-updating bars. Read the pipe with
`encoding="utf-8", errors="replace"` — Inventory prints file names, and a
Takeout export contains every script on earth.

### `takeout_scout/index_reader.py` — new

I/O at construction only; pure afterwards.

```python
class IndexUnusable(Exception):
    """The index is missing, unreadable, or a schema this version cannot read."""


@dataclass(frozen=True)
class IndexedPairing:
    media_path: str
    archive: str | None
    sidecar_path: str | None
    rule: str            # exact | edited | orphan | ambiguous | ...
    confidence: str      # own | related | none


class TakeoutIndex:
    @classmethod
    def open(cls, path: Path) -> "TakeoutIndex": ...
    def pairings(self) -> Iterator[IndexedPairing]: ...
    def counts_by_rule(self) -> dict[str, int]: ...
    def counts_by_confidence(self) -> dict[str, int]: ...
    def unparseable_sidecars(self) -> list[tuple[str, str]]: ...
```

Open read-only via a percent-encoded `file:...?mode=ro` URI — Scout must be
incapable of writing to Inventory's output.

**Schema gate.** Read `index_meta.schema_version`. Inventory's
`INDEX_SCHEMA_VERSION` is currently **1**. A higher value, or a missing
`index_meta` table, raises `IndexUnusable` rather than guessing at an older
layout. Assert the expected column set on open, so schema drift in Inventory
surfaces as a clear error instead of a wrong answer.

**A seam named, not hidden.** These two repositories version independently.
The schema check turns a drift into a legible failure, but only a test against
a real index catches it fully. That is accepted knowingly, and stated here so
nobody discovers it by surprise.

### `takeout_scout/worklist.py` — new

Pure. Takes rows, returns findings. No I/O, no Streamlit.

```python
@dataclass(frozen=True)
class Finding:
    kind: str            # see the table below
    path: str
    detail: str


def build_worklist(index: TakeoutIndex) -> list[Finding]: ...


def compare_with_scout(
    index: TakeoutIndex,
    scout_pairings: dict[str, str | None],
) -> tuple[int, int, list[Finding]]:
    """(agreements, disagreements, findings) between the two pairings.

    Only media present on both sides is compared; anything Scout never saw is
    not a disagreement.
    """
```

### `app.py` — three touchpoints

1. **The offer.** After a quick scan completes, a section: what the deep pass
   would do, roughly how long it takes, and a button. Absent, with one
   explanatory line, when Inventory cannot be found.
2. **The output pane.** Lines stream into an `st.empty()` placeholder as they
   arrive. The tab is busy until the run finishes — that is honest, and it
   beats a silent multi-minute spinner that is indistinguishable from a hang.
3. **The work list.** Rendered from the index, with the comparison against
   Scout's own pairing shown first.

## Which directory the deep pass runs against

Inventory's `scan` takes `--takeout <dir>`, a single directory holding the
export. Scout's quick scan accepts folders *and* individual files, and may hold
several unrelated sources at once. These do not map one to one.

**The deep pass is offered per source directory, not for the session.** When
Scout's pending list contains archives from one directory, that directory is
the argument. When it contains more than one, the offer names each and the
person picks — Inventory has no notion of "these seven archives from two
places", and inventing one on Scout's side would mean second-guessing another
tool's model of the world.

Individual files selected in Files mode are offered under their common parent
directory, and the offer says so, because that is what Inventory will actually
walk. If they share no common parent, no offer is made for them.

The index is written beside the export, as `takeout-index.sqlite` in that same
directory. That is Inventory's own default and Scout does not override it.

## Locating Inventory

In order:

1. A path remembered in `state/` from a previous session
2. `../Takeout_Inventory/takeout_inventory.py`, relative to Scout's repository
   root
3. A path field the person fills in, which is then remembered

Finding nothing is not an error. The offer does not render, and one line
explains why and what would make it appear. Scout must remain completely usable
with Inventory absent.

## The work list

Not an inventory — Inventory already produces one. A list of things that are
wrong, each with enough context to act on.

| Finding | Source in the index | Why it matters |
| --- | --- | --- |
| media with no sidecar | `media.rule = 'orphan'` | no date and no location are recoverable for this file |
| ambiguous pairing | `media.rule = 'ambiguous'` | more than one candidate; Inventory refused to guess |
| sidecar naming no media | sidecar rows claimed by no pairing | metadata describing a file absent from the export |
| `related` pairing | `media.confidence = 'related'` | date is trustworthy; **GPS is not** — it describes a different photograph |
| unparseable sidecar | `sidecar.parse_error` non-null | metadata exists but is corrupt; distinct from having none |
| **Scout/Inventory disagreement** | both sides present, different sidecar | Scout paired within one archive; Inventory paired across all |

The last row is what earns this feature. It is the honest measure of what the
quick scan got wrong, computed rather than asserted, and on a real multi-part
export it will be a large number.

The `related` row matters for a reason worth stating: it encodes GPTH issue
#139, where photos silently acquired another photograph's GPS coordinates.
Marking it is the difference between a date you can trust and a location you
cannot.

**Nothing is written to the archives.** The output is a work list, to hand to
another tool or act on by hand.

## Error handling

Every case degrades to Scout working as it does today.

| Situation | Behaviour |
| --- | --- |
| Inventory not found | offer does not render; one explanatory line |
| `uv` not on PATH | offer renders, button reports the missing prerequisite |
| `scan` exits non-zero | name the phase, show the last output lines, keep any prior index |
| `index` exits non-zero | same, naming `index` rather than `scan` |
| index absent after a clean run | report it; do not silently show an empty work list |
| `schema_version` higher than known | `IndexUnusable`; tell the reader Scout needs updating |
| `index_meta` missing | `IndexUnusable`; a pre-versioned index is not readable |
| index unreadable or corrupt | `IndexUnusable`; offer to re-run the deep pass |

A failed deep pass must never damage a quick scan's results already on screen.

## Testing

Four layers.

1. **`find_inventory`** — sibling found; sibling absent; remembered path wins
   over sibling; a remembered path that no longer exists falls through rather
   than raising.
2. **`deep_pass_commands`** — exact argv for both phases, asserted literally.
   These strings are a contract with another repository's CLI; if Inventory
   renames a flag, this test is what notices.
3. **`index_reader` against a synthetic database** built from the schema
   literal — including the refusals: `schema_version = 2`, absent `index_meta`,
   a missing column. Each must raise `IndexUnusable`, not return wrong data.
   One test must also prove the connection is genuinely read-only, by
   attempting a write and asserting sqlite refuses it. "Opened read-only" is a
   claim about a URI string until something tries to write.
4. **`build_worklist` and `compare_with_scout`** over fixture rows, one test
   per finding kind, plus the case where the two pairings agree and no
   disagreement is reported.

**Mutation-proven.** For each of layers 3 and 4, break the code the test guards
and confirm the test fails. A test that passes against a gutted implementation
is worse than no test, and this repository has shipped exactly that before.

**One structural test.** `takeout_inventory` must not be importable from Scout,
and no file under `takeout_scout/` may contain `import takeout_inventory`. The
licence boundary is a design constraint, and a design constraint with no test
is a comment.

**Not tested:** the Streamlit touchpoints, which have no harness. Said plainly
rather than papered over with tests that assert nothing.

## Deferred

- **Teaching Inventory EXIF and content hashing**, which would let it subsume
  Scout's engine entirely. A much larger job in the other repository, and it
  would end the duplication rather than bridging it. Worth revisiting once this
  bridge has been used in anger.
- **Acting on the work list.** Renaming files, writing sidecar dates into EXIF,
  moving photos into album folders. That is a destructive operation on
  irreplaceable data and needs its own design, its own dry-run mode, and its own
  argument about whether it belongs in either of these tools.
