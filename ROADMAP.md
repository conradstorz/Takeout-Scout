# Roadmap

## Where this fits

Four projects, two jobs.

**[ImageHarbor](https://github.com/conradstorz/ImageHarbor) is the daily tool.**
It keeps a personal photo collection organized and identifies what is in it —
where a photo was taken and who is in it. It runs continuously.

**The Takeout trio are occasional tools**, this one among them. They exist to
acquire files from Google's cloud and work out what those files are. You run
them when an export lands, not every day.

| Project | Job | Cadence |
|---|---|---|
| [Google-Takeout-Downloader](https://github.com/conradstorz/Google-Takeout-Downloader) | Fetch the archives without babysitting them. | Once per export |
| **Takeout Scout** | Explore an export in a browser. Human-facing. | Once per export |
| [Takeout_Inventory](https://github.com/conradstorz/Takeout_Inventory) | Catalog an export and publish a machine-readable index. | Once per export |
| [ImageHarbor](https://github.com/conradstorz/ImageHarbor) | Organize, verify and identify the photo library itself. | Continuous |

**Scout is the human-friendly half of a pair.** `Takeout_Inventory` is the
machine-friendly half: it produces a file another program can read. Scout
produces a screen a person can look at. They complement each other rather than
competing, and Scout is the entry point — you scan here first, and Scout offers
to invoke Inventory when you want the complete answer.

## Where it is now

**A Streamlit web app, and only that.** The Tkinter desktop UI was retired; there
is one interface to maintain and one place a bug can hide.

- **Quick scan** — reads ZIP and TGZ archives without extracting, identifies
  content types and Google services, groups multi-part exports, and shows the
  result as a sortable table with CSV export.
- **Deep pass** — if `Takeout_Inventory` is available, Scout offers to run it
  after a scan, then reads the published index back as a work list: orphaned
  media, orphaned sidecars, pairings whose location data cannot be trusted, and
  every place Scout's own quick answer was wrong.

The deep pass exists because Scout's own pairing is *usually* wrong on a
multi-part export. Scout pairs a photo with its sidecar inside one archive; in
one measured export, 71.7% of photos had their sidecar in a **different**
archive. That is not a bug to fix in the quick scan — resolving it needs every
archive open at once, which is exactly what Inventory does.

## What is next

1. **Repair, not just report.** The work list today is a list. The obvious next
   step is acting on it: renaming files to match their sidecar, writing sidecar
   dates into EXIF, moving photos into the albums Google recorded. This is
   deliberately unbuilt. It writes to irreplaceable data, so it needs its own
   design, a dry run that is the default rather than a flag, and a way to undo.
   Nothing about "report what would need repairing" prepares the code for
   actually doing it.

2. **Retire the duplicate scanning engine.** Scout and Inventory answer the same
   question with two different code paths. If Inventory grows content hashing
   and EXIF extraction — [its own next
   step](https://github.com/conradstorz/Takeout_Inventory) — Scout could become purely a presentation layer over Inventory's output, and
   the second engine could go. That is the long-term shape.

3. **Documentation that matches the code.** There is no architecture document
   and no working agreement; `DISCOVERY_TRACKING.md` and `METADATA_FEATURES.md`
   describe subsystems in isolation with nothing tying them together.

## Settled, and not up for revisiting

- **One interface.** The browser. Adding a second UI was tried and undone.
- **Nothing in your archives is modified by a scan or a deep pass.** Inventory
  writes its index and cache *beside* the archives; the output Scout shows you
  is a list. If item 1 above ever ships, it will be an explicit, separate,
  opt-in action — never a side effect of looking.
- **`Takeout_Inventory` is run as a subprocess and never imported.** Scout is
  GPL-3.0-or-later and Inventory is AGPL-3.0-or-later; importing it would form a
  combined work and pull AGPL section 13 — the network clause — onto a program
  that serves a web UI. `tests/test_licence_boundary.py` enforces this. It is a
  licence constraint, not a style preference.

## Licence

GPL-3.0-or-later. See [LICENSE](LICENSE).
