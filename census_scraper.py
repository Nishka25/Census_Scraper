#!/usr/bin/env python3
"""
census_scraper.py — MacroCorpus / Census sub-team

One script that:
  1. reads the Census API catalog (every dataset the API offers),
  2. downloads every TIME-SERIES dataset in parallel (one dataset per worker),
  3. splits each dataset into its individual series,
  4. saves every series as its own JSON file, plus metadata and a run log.

FLOW
  api.census.gov/data.json  (catalog of ~1,800 datasets)
          |  keep time-series datasets (or --scope all)
          v
  multiprocessing.Pool  ->  one dataset per worker/core
          |  for each dataset:
          |   a. metadata: variables.json, geography.json, groups.json -> metadata/<dataset>.json
          |   b. choose variables: series dimensions (required codes) + value columns (<=49 per call)
          |   c. choose geography: simplest one that returns data (us:* -> us:1 -> state ... -> none)
          |   d. pull full history (falls back to decade-by-decade if one call is too big)
          |   e. group rows into series by their dimension codes, sort by time
          |   f. write series/<dataset>/<series_id>.json  +  series/<dataset>/_manifest.json
          v
  run_log.csv + run_summary.json  (status of every dataset, why anything failed)

SETUP
  pip install requests
  export CENSUS_API_KEY=your_key            (Windows: set CENSUS_API_KEY=your_key)

USAGE
  python census_scraper.py --list                 # show which datasets would be downloaded
  python census_scraper.py                        # download all time-series datasets
  python census_scraper.py --only eits            # only the economic indicators (mrts, mwts, m3, qss ...)
  python census_scraper.py --only eits/mwts eits/mrts --workers 2
  python census_scraper.py --since 2000           # only observations from 2000 on
  python census_scraper.py --force                # re-download datasets that already finished

Rerunning skips datasets that already finished, so an interrupted run picks up where it stopped.

OUTPUT (default folder: data/Census next to this script)
  catalog.json                          datasets selected for this run
  metadata/<dataset>.json               variables, geography, groups, catalog entry
  series/<dataset>/<series_id>.json     one file per series (sorted observations)
  series/<dataset>/_manifest.json       list of the dataset's series + status
  run_log.csv, run_summary.json         one row per dataset: ok / empty / error, and why
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from multiprocessing import Pool, cpu_count
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
BASE = os.environ.get("CENSUS_BASE_URL", "https://api.census.gov").rstrip("/")
KEY = os.environ.get("CENSUS_API_KEY")
OUT = Path(__file__).resolve().parent / "data" / "Census"

MAX_VARS = 49            # Census allows ~50 variables per request
MAX_STRING_COLS = 20     # cap on code/label columns per request (rest of the slots go to values)
SKIP = {"for", "in", "ucgid", "time"}   # predicates, not data columns
SMALL_STATE = "56"       # Wyoming, for datasets that only work inside a state
TIMEOUT = 180            # seconds per request
SINCE = None             # set by --since
TIME_LIKE = re.compile(r"^(year|quarter|month|period|date_?code)$", re.I)

_session = None


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def slugify(text, limit=80):
    s = re.sub(r"[^A-Za-z0-9.-]+", "_", str(text)).strip("_")
    return s[:limit] or "x"


def redact(text):
    """Never let the API key leak into logs (request errors can include the full URL)."""
    text = str(text)
    if KEY:
        text = text.replace(KEY, "***")
    return re.sub(r"key=[^&\s'\"]+", "key=***", text)


def write_json(path, obj):
    """Write to a temp file then rename, so a crash or Ctrl-C never leaves a half-written JSON."""
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1))
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
class ApiKeyError(Exception):
    pass


def session():
    global _session
    if _session is None:
        _session = requests.Session()
    return _session


def http_get(url, params=None, retries=4):
    """GET with the API key, retries on throttling / gateway errors, clear error on a bad key."""
    params = dict(params or {})
    if KEY:
        params["key"] = KEY
    last = None
    for attempt in range(retries):
        try:
            r = session().get(url, params=params, timeout=TIMEOUT, allow_redirects=False)
        except requests.RequestException as e:
            last = redact(f"{e.__class__.__name__}: {e}")
            time.sleep(2 ** attempt)
            continue
        if r.status_code in (301, 302, 303, 307, 308):
            loc = r.headers.get("Location", "")
            if "key" in loc:
                raise ApiKeyError(f"Census redirected to {loc} (API key missing or invalid)")
            return r
        if r.status_code in (429, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            time.sleep(3 * 2 ** attempt)
            continue
        return r
    raise RuntimeError(f"request failed after {retries} tries ({last})")


def get_json(url):
    """Metadata request. Returns None on 404 / empty / non-JSON instead of crashing."""
    r = http_get(url)
    if r.status_code != 200 or not r.text.strip():
        return None
    try:
        return r.json()
    except ValueError:
        return None


def query(base, names, for_=None, in_=None, time_val=None):
    """Data request. Returns (rows_as_dicts, error_message). rows=[] means a valid but empty answer."""
    params = {"get": ",".join(names)}
    if for_:
        params["for"] = for_
    if in_:
        params["in"] = in_
    if time_val:
        params["time"] = time_val
    try:
        r = http_get(base, params)
    except ApiKeyError:
        raise
    except Exception as e:
        return None, str(e)[:200]
    if r.status_code == 204 or not r.text.strip():
        return [], None
    if not r.ok:
        return None, f"HTTP {r.status_code}: " + r.text[:200].replace("\n", " ")
    try:
        rows = r.json()
    except ValueError:
        return None, "non-JSON reply: " + r.text[:120].replace("\n", " ")
    if not rows:
        return [], None
    header = rows[0]
    return [dict(zip(header, row)) for row in rows[1:]], None


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------
def dataset_id(entry):
    """Real API path (e.g. timeseries/eits/mwts or 2019/acs/acs5) from the catalog entry."""
    for dist in entry.get("distribution") or []:
        url = dist.get("accessURL") or ""
        if "/data/" in url:
            return url.split("/data/", 1)[1].strip("/")
    parts = ([str(entry["c_vintage"])] if entry.get("c_vintage") else []) + list(entry.get("c_dataset") or [])
    return "/".join(parts)


def is_timeseries(entry, ds_id):
    return bool(entry.get("c_isTimeseries")) or ds_id.startswith("timeseries/")


def load_catalog(scope, only, exclude, limit):
    data = get_json(f"{BASE}/data.json")
    if not data:
        sys.exit("Could not read the Census catalog (data.json).")
    jobs = []
    for e in data.get("dataset", []):
        ds_id = dataset_id(e)
        ts = is_timeseries(e, ds_id)
        if scope == "timeseries" and not ts:
            continue
        if only and not any(o in ds_id for o in only):
            continue
        if exclude and any(x in ds_id for x in exclude):
            continue
        jobs.append({
            "id": ds_id,
            "title": e.get("title"),
            "description": (e.get("description") or "").strip(),
            "vintage": e.get("c_vintage"),
            "is_timeseries": ts,
            "is_microdata": bool(e.get("c_isMicrodata")),
            "identifier": e.get("identifier"),
            "modified": e.get("modified"),
        })
    jobs.sort(key=lambda j: j["id"])
    return jobs[:limit] if limit else jobs


# ---------------------------------------------------------------------------
# Per-dataset helpers
# ---------------------------------------------------------------------------
def geo_options(fips):
    """(for, in) pairs to try, simplest first. Same logic the notebook tester validated."""
    opts = []
    for g in sorted(fips, key=lambda g: len(g.get("requires") or [])):
        name, req = g.get("name"), g.get("requires") or []
        if not name:
            continue
        if not req:
            code = "1" if name == "us" else SMALL_STATE if name == "state" else "*"
            opts += [(f"{name}:*", None), (f"{name}:{code}", None)]
        elif req == ["state"]:
            opts.append((f"{name}:*", f"state:{SMALL_STATE}"))
        elif req == ["state", "county"]:
            opts.append((f"{name}:*", f"state:{SMALL_STATE} county:001"))
    opts.append((None, None))                       # last resort: no geography
    return list(dict.fromkeys(opts))[:8]


def plan_variables(v):
    """Split variables into series dimensions (codes/labels) and value columns, in request chunks."""
    usable = [n for n, i in v.items() if not i.get("predicateOnly") and n not in SKIP]
    required = [n for n in usable if str(v[n].get("required", "")).lower() == "true"]
    if "cell_value" in v:                                   # economic indicators style: one value column
        values = ["cell_value"]
    else:
        values = [n for n in usable
                  if v[n].get("predicateType") in ("int", "float") and n not in required]
    required = [n for n in required if n not in values]     # cell_value is "required" but it is the value
    strings = [n for n in usable if n not in values]
    strings = (required + [n for n in strings if n not in required])
    strings = list(dict.fromkeys(strings))[:MAX_STRING_COLS]
    if not values:                                          # nothing numeric: treat the strings as data
        return required, strings, []
    room = max(1, MAX_VARS - len(strings))
    chunks = [values[i:i + room] for i in range(0, len(values), room)]
    return required, strings, chunks


def frequency(v, times):
    dt = (v.get("time") or {}).get("datetime") or {}
    if dt.get("month"):
        return "monthly"
    if dt.get("quarter"):
        return "quarterly"
    if dt.get("year") and not dt.get("month"):
        sample = next(iter(times), "")
        if re.fullmatch(r"\d{4}", sample):
            return "annual"
    sample = next(iter(times), "")
    if re.fullmatch(r"\d{4}-\d{2}", sample):
        return "monthly"
    if re.fullmatch(r"\d{4}-Q\d", sample):
        return "quarterly"
    if re.fullmatch(r"\d{4}", sample):
        return "annual"
    return "unknown"


def to_number(x):
    """'656420' -> 656420, '1.35' -> 1.35, '(S)' or '' -> None (the raw text is kept separately)."""
    if x is None:
        return None
    s = str(x).strip().replace(",", "")
    try:
        return int(s) if re.fullmatch(r"-?\d+", s) else float(s)
    except ValueError:
        return None


def add_time(records):
    """Give every row a '_time' string. Uses 'time', or builds one from year/quarter/month columns."""
    if not records:
        return None
    cols = records[0].keys()
    if "time" in cols:
        for r in records:
            r["_time"] = r["time"]
        return ["time"]
    tcols = [c for c in cols if TIME_LIKE.match(c)]
    year = next((c for c in tcols if c.lower() in ("year", "date_code", "datecode", "period")), None)
    if not year:
        return None
    sub = next((c for c in tcols if c.lower() in ("quarter", "month")), None)
    for r in records:
        t = str(r[year])
        if sub and r.get(sub) not in (None, ""):
            t += (f"-Q{r[sub]}" if sub.lower() == "quarter" else f"-{int(r[sub]):02d}")
        r["_time"] = t
    return [year] + ([sub] if sub else [])


def choose_key(records, base_cols, candidates):
    """Pick the columns that identify a series: start from the required codes + geography,
    add other code columns only while that removes duplicate (series, time) pairs."""
    def dups(cols):
        seen, d = set(), 0
        for r in records:
            k = tuple(r.get(c) for c in cols) + (r["_time"],)
            if k in seen:
                d += 1
            else:
                seen.add(k)
        return d

    key, cands = list(base_cols), list(candidates)
    d = dups(key)
    while d and cands:
        scored = sorted(cands, key=lambda c: (dups(key + [c]), len({r.get(c) for r in records})))
        best = scored[0]
        nd = dups(key + [best])
        if nd >= d:
            break
        key.append(best)
        cands.remove(best)
        d = nd
    return key, d


def fetch_rows(base, names, geo, has_time):
    """Full history for one variable chunk. Falls back to decade chunks if one call fails."""
    for_, in_ = geo
    full = f"from {SINCE}" if SINCE else "from 1900"
    rows, err = query(base, names, for_, in_, full if has_time else None)
    if rows is not None or not has_time:
        return rows, err, full if has_time else None
    # one big call failed: go decade by decade
    out, errs = [], []
    start = int(SINCE) if SINCE else 1940
    for s in range(start, datetime.now().year + 1, 10):
        part, perr = query(base, names, for_, in_, f"from {s} to {s + 9}")
        if part:
            out += part
        elif perr:
            errs.append(f"{s}s: {perr[:80]}")
    if out:
        return out, None, "decade chunks" + (f" ({len(errs)} decades failed)" if errs else "")
    return None, (errs[0] if errs else err), "decade chunks"


def pick_geography(base, names, fips, has_time):
    """Try geographies from simplest to most specific until one returns data."""
    probes = [f"from {datetime.now().year - 3}", f"from {SINCE}" if SINCE else "from 1900"] if has_time else [None]
    tried = []
    for probe in probes:
        for for_, in_ in geo_options(fips):
            rows, err = query(base, names, for_, in_, probe)
            if rows:
                return (for_, in_), tried
            tried.append(f"for={for_} in={in_} time={probe}: {err or 'empty'}"[:160])
    return None, tried


# ---------------------------------------------------------------------------
# One dataset (runs inside a worker process)
# ---------------------------------------------------------------------------
def process_dataset(job):
    t0 = time.time()
    ds_id = job["id"]
    slug = slugify(ds_id.replace("/", "_"), 80)
    meta_path = OUT / "metadata" / f"{slug}.json"
    series_dir = OUT / "series" / slug
    manifest_path = series_dir / "_manifest.json"
    row = {"dataset_id": ds_id, "title": job.get("title"), "status": "", "n_series": 0,
           "n_observations": 0, "geography": "", "time_query": "", "value_vars": "",
           "key_columns": "", "message": ""}

    def done(status, message=""):
        row.update(status=status, message=redact(message)[:300], seconds=round(time.time() - t0, 1))
        return row

    # resume: skip datasets that already finished
    if manifest_path.exists() and not job.get("force"):
        try:
            m = json.loads(manifest_path.read_text())
            if m.get("status") in ("ok", "empty", "metadata_only"):
                row.update(n_series=m.get("n_series", 0), n_observations=m.get("n_observations", 0))
                return done("skipped", "already downloaded (use --force to redo)")
        except ValueError:
            pass

    base = f"{BASE}/data/{ds_id}"
    try:
        # a. metadata ----------------------------------------------------------
        v = (get_json(f"{base}/variables.json") or {}).get("variables") or {}
        if not v:
            return done("error", "no variables.json")
        fips = (get_json(f"{base}/geography.json") or {}).get("fips", [])
        groups = (get_json(f"{base}/groups.json") or {}).get("groups", [])
        meta = {"source": "census", "dataset": job, "fetched_at": now_iso(), "api": base,
                "variables": v, "geography": fips, "groups": groups,
                "stats": {"variable_count": len(v), "group_count": len(groups),
                          "geography_level_count": len(fips)}}
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        write_json(meta_path, meta)

        if not job.get("is_timeseries"):
            write_manifest(manifest_path, series_dir, job, "metadata_only", [], {})
            return done("metadata_only", "not a time series: metadata saved, no series")

        # b. variables ---------------------------------------------------------
        required, strings, chunks = plan_variables(v)
        if not chunks:
            return done("error", "no numeric value variables found")
        row["value_vars"] = ",".join(sum(chunks, []))[:200]
        has_time = "time" in v

        # c. geography ---------------------------------------------------------
        first_names = list(dict.fromkeys(strings + chunks[0]))
        geo, tried = pick_geography(base, first_names, fips, has_time)
        if geo is None:
            return done("empty" if all("empty" in t for t in tried) else "error",
                        tried[-1] if tried else "no geography worked")
        row["geography"] = f"for={geo[0]} in={geo[1]}"

        # d + e. pull each chunk, group into series ------------------------------
        series = defaultdict(lambda: {"obs": {}})     # key -> {...}
        key_cols, unresolved, time_desc = None, 0, ""
        for chunk in chunks:
            names = list(dict.fromkeys(strings + chunk))
            records, err, time_desc = fetch_rows(base, names, geo, has_time)
            if records is None:
                return done("error", f"data request failed: {err}")
            if not records:
                continue
            time_cols = add_time(records)
            if time_cols is None:
                return done("error", "rows have no time column")
            if key_cols is None:
                geo_cols = [c for c in records[0]
                            if c not in names and c not in time_cols and c != "_time"]
                base_cols = [c for c in required if c not in time_cols] + geo_cols
                cands = [c for c in strings if c not in base_cols and c not in time_cols]
                key_cols, unresolved = choose_key(records, base_cols, cands)
            attr_cols = [c for c in records[0]
                         if c not in key_cols and c not in chunk and c not in time_cols and c != "_time"]
            for r in records:
                dims = {c: r.get(c) for c in key_cols}
                for val in chunk:
                    k = (val,) + tuple(dims.values())
                    s = series[k]
                    s["value_variable"], s["dimensions"] = val, dims
                    if r["_time"] in s["obs"]:
                        continue                                  # duplicate time: keep the first
                    raw = r.get(val)
                    ob = {"time": r["_time"], "value": to_number(raw)}
                    if ob["value"] is None and raw not in (None, ""):
                        ob["raw"] = raw                           # flags like (S), (NA), X
                    attrs = {c: r.get(c) for c in attr_cols if r.get(c) not in (None, "")}
                    if attrs:
                        ob["attributes"] = attrs
                    s["obs"][r["_time"]] = ob

        row.update(time_query=time_desc or "", key_columns=",".join(key_cols or []))
        if not series:
            write_manifest(manifest_path, series_dir, job, "empty", [], {})
            return done("empty", "valid query but no rows")

        # f. write one JSON per series ---------------------------------------------
        entries = write_series(series_dir, job, slug, v, series, row, base, time_desc)
        n_obs = sum(e["n_observations"] for e in entries)
        extra = {"key_columns": key_cols, "geography": row["geography"],
                 "duplicate_rows_dropped": unresolved}
        write_manifest(manifest_path, series_dir, job, "ok", entries, extra)
        row.update(n_series=len(entries), n_observations=n_obs)
        msg = f"{unresolved} duplicate (series,time) rows dropped" if unresolved else ""
        return done("ok", msg)

    except ApiKeyError as e:
        return done("key_error", e)
    except Exception as e:                                  # never let one dataset kill the run
        return done("error", f"{e.__class__.__name__}: {e}")


def write_series(series_dir, job, slug, v, series, row, base, time_desc):
    if series_dir.exists():                                 # clear files from an older run
        for p in series_dir.glob("*.json"):
            p.unlink()
    series_dir.mkdir(parents=True, exist_ok=True)
    multi_value = len({k[0] for k in series}) > 1
    # dimensions that never change across the dataset (e.g. time_slot_id=0, us=1) stay in the
    # file but are left out of the readable id: timeseries_eits_mwts__SM_yes_42
    all_dims = [s["dimensions"] for s in series.values()]
    varying = [c for c in all_dims[0] if len({d.get(c) for d in all_dims}) > 1]
    used, entries = set(), []
    for k, s in series.items():
        val, dims = s["value_variable"], s["dimensions"]
        parts = ([val] if multi_value else []) + [str(dims[c]) for c in varying]
        if not parts:
            parts = [val]
        readable = slugify("_".join(parts), 110)
        sid = f"{slug}__{readable}"
        full_key = "|".join([val] + [f"{c}={x}" for c, x in dims.items()])
        if sid.lower() in used or len("_".join(parts)) > 110:
            sid += "_" + hashlib.sha1(full_key.encode()).hexdigest()[:8]
        used.add(sid.lower())
        obs = [s["obs"][t] for t in sorted(s["obs"])]       # Census does not return rows in order
        times = [o["time"] for o in obs]
        doc = {
            "series_id": sid,
            "series_key": full_key,
            "source": "census",
            "dataset_id": job["id"],
            "dataset_title": job.get("title"),
            "value_variable": val,
            "value_label": (v.get(val) or {}).get("label"),
            "dimensions": dims,
            "dimension_labels": {c: (v.get(c) or {}).get("label", "geography") for c in dims},
            "frequency": frequency(v, times),
            "units": None,      # not provided by the Census API; fill from documentation later
            "n_observations": len(obs),
            "start": times[0] if times else None,
            "end": times[-1] if times else None,
            "query": {"api": base, "geography": row["geography"], "time": time_desc},
            "fetched_at": now_iso(),
            "observations": obs,
        }
        path = series_dir / f"{sid}.json"
        write_json(path, doc)
        entries.append({"series_id": sid, "file": path.name, "value_variable": val, "dimensions": dims,
                        "n_observations": len(obs), "start": doc["start"], "end": doc["end"]})
    entries.sort(key=lambda e: e["series_id"])
    return entries


def write_manifest(path, series_dir, job, status, entries, extra):
    series_dir.mkdir(parents=True, exist_ok=True)
    doc = {"dataset_id": job["id"], "title": job.get("title"), "status": status, "fetched_at": now_iso(),
           "n_series": len(entries), "n_observations": sum(e["n_observations"] for e in entries),
           **extra, "series": entries}
    write_json(path, doc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def init_worker(out, key, base, since):
    global OUT, KEY, BASE, SINCE
    OUT, KEY, BASE, SINCE = Path(out), key, base, since


def main():
    global OUT, KEY, SINCE
    p = argparse.ArgumentParser(description="Download every Census time-series dataset, one JSON per series.")
    p.add_argument("--only", nargs="*", help="only datasets whose id contains one of these (e.g. eits eits/mwts)")
    p.add_argument("--exclude", nargs="*", help="skip datasets whose id contains one of these")
    p.add_argument("--scope", choices=["timeseries", "all"], default="timeseries",
                   help="timeseries (default) or all (non-time-series datasets get metadata only)")
    p.add_argument("--workers", type=int, default=min(8, cpu_count()), help="datasets downloaded in parallel")
    p.add_argument("--since", help="only observations from this year on, e.g. 2000")
    p.add_argument("--limit", type=int, help="only the first N datasets (for testing)")
    p.add_argument("--out", default=str(OUT), help="output folder (default: data/Census next to this script)")
    p.add_argument("--key", help="Census API key (default: CENSUS_API_KEY environment variable)")
    p.add_argument("--force", action="store_true", help="re-download datasets that already finished")
    p.add_argument("--list", action="store_true", help="just list the datasets that would be downloaded")
    a = p.parse_args()

    OUT, KEY, SINCE = Path(a.out), a.key or KEY, a.since
    if SINCE and not re.fullmatch(r"\d{4}", SINCE):
        sys.exit("--since must be a 4-digit year, e.g. --since 2000")
    if a.workers < 1:
        sys.exit("--workers must be at least 1")
    if not KEY and not a.list:
        sys.exit("Set CENSUS_API_KEY (or pass --key). Census requires a key for data requests.\n"
                 "Free key: https://api.census.gov/data/key_signup.html")

    jobs = load_catalog(a.scope, a.only, a.exclude, a.limit)
    print(f"{len(jobs)} datasets selected (scope={a.scope}" + (f", only={a.only}" if a.only else "") + ")")
    if a.list:
        for j in jobs:
            print(" ", j["id"], "|", j["title"])
        return
    if not jobs:
        return

    OUT.mkdir(parents=True, exist_ok=True)
    write_json(OUT / "catalog.json", jobs)
    for j in jobs:
        j["force"] = a.force

    started, results = time.time(), []
    log_path = OUT / "run_log.csv"
    fields = ["dataset_id", "title", "status", "n_series", "n_observations", "geography",
              "time_query", "value_vars", "key_columns", "message", "seconds"]
    print(f"Downloading with {a.workers} parallel workers -> {OUT}\n")
    # maxtasksperchild: restart each worker after a few datasets so memory from big ones is freed
    pool = Pool(a.workers, initializer=init_worker, initargs=(str(OUT), KEY, BASE, SINCE),
                maxtasksperchild=10)
    try:
        with open(log_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for k, res in enumerate(pool.imap_unordered(process_dataset, jobs), 1):
                results.append(res)
                writer.writerow(res)
                f.flush()                                   # log is readable while the run is going
                print(f"[{k}/{len(jobs)}] {res['status']:<13} {res['dataset_id']:<45} "
                      f"{res['n_series']:>6} series  {res.get('seconds', 0):>6}s  {res['message'][:70]}")
                if res["status"] == "key_error":
                    pool.terminate()
                    sys.exit(f"\nStopped: {res['message']}")
        pool.close()
    except KeyboardInterrupt:
        pool.terminate()
        print(f"\nStopped by you after {len(results)} datasets. Finished ones are saved; "
              "rerun the same command to continue where it left off.")
        sys.exit(130)
    finally:
        pool.join()

    status = Counter(r["status"] for r in results)
    summary = {"finished_at": now_iso(), "minutes": round((time.time() - started) / 60, 1),
               "datasets": len(results), "status_counts": dict(status),
               "total_series": sum(r["n_series"] for r in results),
               "total_observations": sum(r["n_observations"] for r in results),
               "failed": [{"dataset_id": r["dataset_id"], "status": r["status"], "message": r["message"]}
                          for r in results if r["status"] in ("error", "empty")]}
    write_json(OUT / "run_summary.json", summary)
    print(f"\nDone in {summary['minutes']} min: {dict(status)}")
    print(f"{summary['total_series']} series, {summary['total_observations']} observations -> {OUT}")
    print(f"Per-dataset status: {log_path}")


if __name__ == "__main__":
    main()
