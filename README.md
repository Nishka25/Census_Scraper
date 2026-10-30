Here's the full README as raw markdown. Copy everything inside the block.

````markdown
# Census Scraper (MacroCorpus, Census sub-team)

`census_scraper.py` downloads time-series datasets from the [Census Data API](https://www.census.gov/data/developers.html) and saves **every individual series as its own JSON file**, along with dataset metadata and a per-dataset run log.

It reads the API catalog (~1,800 datasets), keeps the time-series ones, downloads them in parallel (one dataset per worker), splits each dataset into series, and writes the results to disk. Reruns skip datasets that already finished, so an interrupted run picks up where it stopped.

---

## Setup

Requires Python 3.8+ and `requests`.

```bash
pip install requests
```

Get a free API key at https://api.census.gov/data/key_signup.html, then set it as an environment variable:

```bash
# macOS / Linux
export CENSUS_API_KEY=your_key

# Windows
set CENSUS_API_KEY=your_key
```

You can also pass it directly with `--key your_key`. A key is required for everything except `--list`.

---

## Usage

```bash
python census_scraper.py --list                          # preview which datasets would be downloaded
python census_scraper.py                                 # download all time-series datasets
python census_scraper.py --only eits                     # only economic indicators (mrts, mwts, m3, qss, ...)
python census_scraper.py --only eits/mwts eits/mrts --workers 2
python census_scraper.py --since 2000                    # only observations from 2000 on
python census_scraper.py --force                         # re-download datasets that already finished
```

**Tip:** run `--list` first, then test on a small slice (`--only eits --limit 3`) before a full run.

### Options

| Flag | Default | What it does |
| --- | --- | --- |
| `--only ID [ID ...]` | all | Keep datasets whose ID contains any of these strings |
| `--exclude ID [ID ...]` | none | Skip datasets whose ID contains any of these strings |
| `--scope {timeseries,all}` | `timeseries` | `all` also includes non-time-series datasets (metadata only, no series) |
| `--workers N` | `min(8, CPU cores)` | Number of datasets downloaded in parallel |
| `--since YYYY` | none | Only request observations from this year onward |
| `--limit N` | none | Only process the first N datasets (for testing) |
| `--out PATH` | `data/Census` next to the script | Output folder |
| `--key KEY` | `$CENSUS_API_KEY` | Census API key |
| `--force` | off | Re-download datasets that already finished |
| `--list` | off | Print the selected datasets and exit |

Optional environment variable: `CENSUS_BASE_URL` (default `https://api.census.gov`).

---

## How it works

For each dataset, a worker process:

1. **Saves metadata.** It fetches `variables.json`, `geography.json`, and `groups.json` and writes them to `metadata/<dataset>.json`.
2. **Plans variables.** Required code columns become series dimensions, and numeric columns become values. Values are split into request chunks so each call stays under the API's ~50-variable limit (`MAX_VARS = 49`).
3. **Picks a geography.** It tries options from simplest to most specific (`us:*`, `us:1`, `state:*`, ..., no geography) and uses the first one that returns data.
4. **Pulls the full history.** It requests everything in one call (`from 1900` or `from --since`). If that fails, it falls back to decade-by-decade requests.
5. **Groups rows into series** by their dimension codes and sorts observations by time.
6. **Writes files.** It saves one JSON per series plus a `_manifest.json` for the dataset.

When all datasets finish, it writes `run_log.csv` and `run_summary.json`.

---

## Output

```
data/Census/
├── catalog.json                         # datasets selected for this run
├── metadata/<dataset>.json              # variables, geography, groups, catalog entry
├── series/<dataset>/<series_id>.json    # one file per series
├── series/<dataset>/_manifest.json      # list of the dataset's series + status
├── run_log.csv                          # one row per dataset (written live during the run)
└── run_summary.json                     # totals, status counts, list of failures
```

Dataset folder names are the API path with `/` replaced by `_`, e.g. `timeseries/eits/mwts` becomes `timeseries_eits_mwts`.

### Series file format

```json
{
  "series_id": "timeseries_eits_mwts__SM_yes_42",
  "series_key": "cell_value|category_code=42|data_type_code=SM|...",
  "source": "census",
  "dataset_id": "timeseries/eits/mwts",
  "dataset_title": "...",
  "value_variable": "cell_value",
  "value_label": "...",
  "dimensions": { "category_code": "42", "data_type_code": "SM", "...": "..." },
  "dimension_labels": { "category_code": "...", "...": "..." },
  "frequency": "monthly",
  "units": null,
  "n_observations": 412,
  "start": "1992-01",
  "end": "2026-08",
  "query": { "api": "...", "geography": "for=us:* in=None", "time": "from 1900" },
  "fetched_at": "2026-10-30T17:00:00+00:00",
  "observations": [
    { "time": "1992-01", "value": 656420 },
    { "time": "1992-02", "value": null, "raw": "(S)" }
  ]
}
```

- `value` is parsed as a number. Non-numeric Census flags like `(S)`, `(NA)`, or `X` become `null`, and the original text is kept in `raw`.
- Extra non-key columns on a row are stored under `attributes`.
- `frequency` is one of `monthly`, `quarterly`, `annual`, or `unknown`.
- Dimensions that are the same across the whole dataset are left out of the readable `series_id` but kept in `dimensions`. If two IDs would collide, or an ID gets too long, a short hash is appended.

### Run statuses (`run_log.csv`)

| Status | Meaning |
| --- | --- |
| `ok` | Series downloaded and saved |
| `empty` | Valid query, but the API returned no rows |
| `error` | Something failed; see the `message` column |
| `metadata_only` | Not a time series (only with `--scope all`); metadata saved, no series |
| `skipped` | Already finished in an earlier run (use `--force` to redo) |
| `key_error` | API key missing or invalid; the whole run stops |

---

## Reliability

- **Resumable.** Datasets with a finished manifest (`ok`, `empty`, `metadata_only`) are skipped on rerun.
- **Safe writes.** Every JSON is written to a temp file and then renamed, so a crash or Ctrl-C never leaves a half-written file.
- **Retries.** Requests retry with exponential backoff on throttling (429) and gateway errors (502/503/504).
- **Isolated failures.** An error in one dataset is logged without stopping the run. Only an API key error stops everything.
- **Key redaction.** The API key is stripped from all logged messages.
- **Memory.** Worker processes restart every 10 datasets so memory from large datasets is freed.

---

## Known limitations

- **One geography per dataset.** The scraper keeps the first geography that returns data, usually national (`us`). It does not download every state or county.
- **State-level-only datasets use Wyoming.** If a dataset only works inside a state, the scraper uses Wyoming (FIPS `56`, and county `001` when needed) as a small test geography. Those series cover Wyoming only.
- **`units` is always `null`.** The Census API doesn't provide units, so they need to be filled in from documentation.
- **Duplicate rows.** If rows can't be separated into unique (series, time) pairs, the first value is kept. The number dropped is recorded in `_manifest.json` and in the log message.
- **Decade fallback.** Without `--since`, the decade-by-decade fallback starts at 1940.
- **Column caps.** At most 20 code/label columns are requested per call (`MAX_STRING_COLS`), and the remaining slots go to value columns.
````
