"""Turn Inventory's index into a list of what needs repairing.

Not an inventory - Inventory already produces one. This is the subset that is
*wrong*: media with no metadata, metadata with no media, pairings whose
location cannot be trusted, and the places where Scout's own per-archive scan
disagrees with Inventory's global one.

Nothing here writes to the archives. The output is a work list.
"""
from __future__ import annotations

from dataclasses import dataclass

ORPHAN_MEDIA = "orphan_media"
AMBIGUOUS_PAIRING = "ambiguous_pairing"
ORPHAN_SIDECAR = "orphan_sidecar"
RELATED_PAIRING = "related_pairing"
UNPARSEABLE_SIDECAR = "unparseable_sidecar"
DISAGREEMENT = "disagreement"


@dataclass(frozen=True)
class Finding:
    """One thing that is wrong, and enough context to act on it."""

    kind: str
    path: str
    detail: str


def build_worklist(index) -> list[Finding]:
    """Every defect the index describes, sorted by path for a stable display."""
    findings: list[Finding] = []

    for pairing in index.pairings():
        if pairing.rule == "ambiguous":
            findings.append(Finding(
                AMBIGUOUS_PAIRING, pairing.media_path,
                "more than one candidate sidecar; Inventory refused to guess"))
        elif pairing.sidecar_path is None:
            findings.append(Finding(
                ORPHAN_MEDIA, pairing.media_path,
                "no sidecar: neither date nor location is recoverable"))
        elif pairing.confidence == "related":
            findings.append(Finding(
                RELATED_PAIRING, pairing.media_path,
                f"paired to {pairing.sidecar_path} by rule "
                f"'{pairing.rule}': the date describes this photograph, "
                f"the GPS describes a different one"))

    for path in index.all_sidecar_paths() - index.claimed_sidecar_paths():
        findings.append(Finding(
            ORPHAN_SIDECAR, path,
            "metadata for a file that is not in the export"))

    for path, error in index.unparseable_sidecars():
        findings.append(Finding(
            UNPARSEABLE_SIDECAR, path,
            f"metadata exists but could not be read: {error}"))

    return sorted(findings, key=lambda f: (f.path, f.kind))


def compare_with_scout(
    index, scout_pairings: dict[str, str | None]
) -> tuple[int, int, list[Finding]]:
    """(agreements, disagreements, findings) between the two pairings.

    Scout pairs within a single archive. Inventory pairs across all of them,
    and on the real export 71.7% of photos have their sidecar in a different
    archive - so on a multi-part export this disagreement count is the honest
    measure of what the quick scan got wrong.

    Only media present on both sides is compared. Anything Scout never looked
    at is not a disagreement.
    """
    agreements = 0
    findings: list[Finding] = []

    for pairing in index.pairings():
        if pairing.media_path not in scout_pairings:
            continue
        scout_said = scout_pairings[pairing.media_path]
        if scout_said == pairing.sidecar_path:
            agreements += 1
            continue
        findings.append(Finding(
            DISAGREEMENT, pairing.media_path,
            f"Scout paired {scout_said or 'nothing'}; "
            f"Inventory paired {pairing.sidecar_path or 'nothing'} "
            f"by rule '{pairing.rule}'"))

    findings.sort(key=lambda f: f.path)
    return agreements, len(findings), findings
