# goodreads

Exploratory project for mining [Open Library](https://openlibrary.org) bulk
data dumps into a local dataset of books, works, and authors — including
their cross-referenced Goodreads IDs.

## Status

Early / exploratory. File names like `test.ipynb`, `test_openlibrary.py`,
and `dataset/test/` reflect that this is still being scoped out, not a
finished pipeline.

## Layout

| Path | What it is |
|---|---|
| `dataset/` | Raw Open Library dump files, plus decompressed/derived output. Not tracked in git — see [Data](#data) below. |
| `test.ipynb` | Notebook for exploring the dump format: reads the tab-separated dump lines, recursively discovers the JSON schema across a sample of records, and writes a few full records to `sample_data.json` for inspection. |
| `test_openlibrary.py` | Integration tests against the live OpenLibrary REST API (`/api/books`, `/search.json`). Requires network access. |
| `requirements.txt` | Python dependencies. |
| `venv/` | Local virtualenv. Not tracked in git. |

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

To run `test.ipynb` you'll also need Jupyter, which isn't in
`requirements.txt` yet:

```bash
pip install jupyter ipykernel
```

## Data

This project works from the [Open Library data dumps](https://openlibrary.org/developers/dumps) —
bulk `.txt.gz` exports of every author, work, and edition record on the
site. Each line is tab-separated:

```
<type>	<key>	<revision>	<last_modified>	<json>
```

For example, one author record:

```
/type/author	/authors/OL10000080A	1	2021-12-26T21:23:30.303089	{"type": {"key": "/type/author"}, "name": "Gunthard Heller", "key": "/authors/OL10000080A", ...}
```

Author records include a `remote_ids.goodreads` field (alongside IDs for
VIAF, Wikidata, LibraryThing, etc.), which is what makes this dump useful
as a source for a Goodreads-oriented dataset.

Files currently under `dataset/`:

| File | Size | Status |
|---|---|---|
| `ol_dump_authors_2026-08-31.txt.gz` | — | decompressed into `authors.txt` (~5.6 GB) |
| `ol_dump_works_2026-08-31.txt.gz` (as `ol_dump_works.txt.gz`) | ~3.8 GB | not yet decompressed |

`dataset/test/` is an empty scratch folder, presumably meant for a small
sample subset of the data for fast local iteration instead of loading the
full multi-GB dumps.

These dump files are **not** checked into git — they're multi-gigabyte and
easy to regenerate. Download fresh copies from the
[Open Library dumps page](https://openlibrary.org/developers/dumps) if you
need them.

## Running the tests

```bash
python3 -m unittest -v test_openlibrary
```

Run a single test case:

```bash
python3 -m unittest test_openlibrary.OpenLibraryBooksAPITest.test_fetch_book_by_isbn
```

## Repository hygiene

This directory isn't a git repo yet. Whenever you do run `git init`, the
included `.gitignore` already excludes `venv/`, `__pycache__/`, and the
large files under `dataset/`, so a first commit won't try to pull in
several gigabytes of data.
