#!/usr/bin/env python3
"""
census_qss.py: QSS (Quarterly Services Survey) metadata + sample series.

Same steps as the team's MRTS and M3 work:
  1. Collect the available metadata (variables, groups, geography, tags, examples,
     catalog entry, code lists)
  2. Look at what information exists at the dataset, series and observation levels
     (pulls one year of data, checks coverage, the series key, codes and flags)
  3. Pick 5 representative series and pull a small sample of observations

SETUP
  pip install requests pandas
  export CENSUS_API_KEY=your_key        (or it will ask for it)

USAGE
  python census_qss.py

OUTPUT (data/Census, same layout as the other datasets)
  catalog/census_qss_variables.json, _groups, _geography, _tags, _examples,
          _catalog_entry, _code_lists
  audit/census_qss_metadata_summary.json
  audit/census_qss_findings.md
  audit/census_qss_sample_observations.csv
  audit/census_qss_data_type_code_inventory.csv
  audit/census_qss_category_code_inventory.csv
"""

import json
import os
from getpass import getpass
from pathlib import Path
from pprint import pprint

import pandas as pd
import requests

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)


def display(table):
    print(table.to_string(index=False) if isinstance(table, pd.DataFrame) else table)

BASE = os.environ.get("CENSUS_BASE_URL", "https://api.census.gov").rstrip("/")
BASE_URL = f"{BASE}/data/timeseries/eits/qss"

CENSUS = Path(__file__).resolve().parent / "data" / "Census"
CATALOG = CENSUS / "catalog"
AUDIT = CENSUS / "audit"
CATALOG.mkdir(parents=True, exist_ok=True)
AUDIT.mkdir(parents=True, exist_ok=True)

print("Setup successful")


# Collect the metadata

# Every metadata endpoint the Census API publishes for a dataset
metadata = {}

for name in ["variables", "groups", "geography", "tags", "examples"]:
    response = requests.get(f"{BASE_URL}/{name}.json", timeout=30)
    print(f"{name}: HTTP {response.status_code}")
    if response.status_code != 200 or not response.text.strip():
        metadata[name] = None
        continue
    metadata[name] = response.json()
    with open(CATALOG / f"census_qss_{name}.json", "w") as f:
        json.dump(metadata[name], f, indent=2)

variables = metadata["variables"]["variables"]
groups = (metadata["groups"] or {}).get("groups", [])
tags = (metadata["tags"] or {}).get("tags", [])
geography = (metadata["geography"] or {}).get("fips", [])

print("\nVariables:", len(variables))
print("Groups:", len(groups))
print("Tags:", len(tags))
print("Geography levels:", [g.get("name") for g in geography])

# Dataset-level metadata: the dataset's entry in the Census API catalog (data.json)
catalog = requests.get(f"{BASE}/data.json", timeout=120).json()

entry = next(d for d in catalog["dataset"] if d.get("c_dataset") == ["timeseries", "eits", "qss"])

with open(CATALOG / "census_qss_catalog_entry.json", "w") as f:
    json.dump(entry, f, indent=2)

for field in ["title", "identifier", "modified", "spatial", "temporal", "contactPoint",
              "license", "c_isTimeseries", "c_isCube", "c_documentationLink"]:
    print(f"{field}: {entry.get(field, '(not given)')}")

print("\nFields in the catalog entry:", len(entry))
print(sorted(entry.keys()))

# Series-level metadata: variable definitions
for name, details in variables.items():
    print(f"\nVariable: {name}")
    print("  Label:", details.get("label"))
    print("  Concept:", details.get("concept"))
    print("  Type:", details.get("predicateType"))
    print("  Required:", details.get("required", "Not specified"))
    print("  Predicate only:", details.get("predicateOnly", False))
    nested = [k for k, v in details.items() if isinstance(v, (dict, list))]
    if nested:
        print("  Nested metadata:", {k: details[k] for k in nested})

# Classifications: does the API publish the meaning of each code? (variables/<name>.json -> values)
code_lists = {}

for name, details in variables.items():
    if details.get("predicateOnly"):
        continue
    r = requests.get(f"{BASE_URL}/variables/{name}.json", timeout=30)
    values = {}
    if r.status_code == 200 and r.text.strip():
        values = (r.json().get("values") or {}).get("item") or {}
    code_lists[name] = values
    print(f"{name}: {len(values)} published codes")

with open(CATALOG / "census_qss_code_lists.json", "w") as f:
    json.dump(code_lists, f, indent=2)


#  Pull one year of data to see the series and observation levels

CENSUS_API_KEY = os.environ.get("CENSUS_API_KEY") or getpass("Enter your Census API key: ")

FIELDS = "data_type_code,time_slot_id,seasonally_adj,category_code,cell_value,error_data"
SAMPLE_YEAR = "2025"                     # last full year of quarters

params = {"get": FIELDS, "for": "us:*", "time": SAMPLE_YEAR, "key": CENSUS_API_KEY}
response = requests.get(BASE_URL, params=params, timeout=60)

print("Status:", response.status_code)
print("URL:", response.url.replace(CENSUS_API_KEY, "REDACTED"))
response.raise_for_status()

data = response.json()
df = pd.DataFrame(data[1:], columns=data[0])

print("Total observations:", len(df))
print("\nUnique values per field:")
print(df.nunique())

# Historical coverage: find the first and last year that return data
def year_status(year):
    p = {"get": FIELDS, "for": "us:*", "time": str(year), "key": CENSUS_API_KEY}
    r = requests.get(BASE_URL, params=p, timeout=60)
    if r.status_code == 200 and r.text.strip():
        rows = r.json()[1:]
        return r.status_code, len(rows), sorted({row[6] for row in rows})
    return r.status_code, 0, []

coverage = {}
for year in [2000, 2001, 2002, 2003, 2004, 2005, 2026, 2027]:
    status, n, periods = year_status(year)
    coverage[year] = n
    print(f"Year {year}: HTTP {status}, {n} observations, periods: {periods}")

years_with_data = [y for y, n in coverage.items() if n]
first_year = min(years_with_data) if years_with_data else None

# Series level: which columns identify a series?
series_columns = ["data_type_code", "seasonally_adj", "category_code"]

duplicates = df.duplicated(subset=series_columns + ["time"]).sum()
series_counts = df.groupby(series_columns)["time"].nunique()

print("Duplicate rows using time + the three candidate fields:", duplicates)
print("Number of series:", len(series_counts))
print("Series with all 4 quarters:", (series_counts == 4).sum())
print("Series with fewer than 4 quarters:", (series_counts < 4).sum())
print("\nColumns that never change:", [c for c in df.columns if df[c].nunique() == 1])

def make_series_id(row):
    adjustment = "A" if row["seasonally_adj"] == "yes" else "U"
    return adjustment + row["category_code"] + row["data_type_code"]

series_ids = df[series_columns].drop_duplicates().copy()
series_ids["series_id"] = series_ids.apply(make_series_id, axis=1)
print("\nExample constructed series IDs:")
print(series_ids.head(10).to_string(index=False))
print("Unique constructed IDs:", series_ids["series_id"].nunique(), "of", len(series_ids))

# Code inventory: every data_type_code, how many industries it covers, and the error rows
code_counts = (
    df.groupby("data_type_code")
      .agg(row_count=("cell_value", "size"),
           categories=("category_code", "nunique"),
           seasonal_adjustment_values=("seasonally_adj", "nunique"),
           error_rows=("error_data", lambda s: (s == "yes").sum()))
      .reset_index()
      .sort_values("data_type_code")
)
display(code_counts)
code_counts.to_csv(AUDIT / "census_qss_data_type_code_inventory.csv", index=False)

# category_code = NAICS code + a letter suffix (e.g. 6221T, 6221A, 6221E)
df["naics"] = df["category_code"].str.extract(r"^(\d+)")[0]
df["suffix"] = df["category_code"].str.extract(r"(\D+)$")[0]
category_inventory = (
    df.groupby(["category_code", "naics", "suffix"], dropna=False)
      .agg(rows=("cell_value", "size"), data_types=("data_type_code", "nunique"))
      .reset_index()
)
category_inventory.to_csv(AUDIT / "census_qss_category_code_inventory.csv", index=False)

print("Distinct category codes:", df["category_code"].nunique())
print("Suffixes:", df["suffix"].value_counts(dropna=False).to_dict())
print("Saved code inventories.")

# Observation level: value formats and flags
numeric = pd.to_numeric(df["cell_value"].str.replace(",", ""), errors="coerce")
flags = df.loc[numeric.isna(), "cell_value"].value_counts()

print("Fields on every row:", list(df.columns[:8]))
print("Time format example:", df["time"].iloc[0])
print("Non-numeric cell_value flags:")
print(flags.to_string())
print("\nerror_data values:", df["error_data"].value_counts().to_dict())
print("data_type_codes on error rows:",
      sorted(df.loc[df["error_data"] == "yes", "data_type_code"].unique())[:12])



# 3. Five representative series

# Picks cover the main kinds of QSS series. If an exact combination does not exist,
# the most common category for that measure + adjustment is used instead.
picks = [
    ("QREV",   "yes", ["54T", "51T", "56T"],   "total revenue, seasonally adjusted"),
    ("QREV",   "no",  ["6221T", "622T"],       "total revenue, hospitals, not adjusted"),
    ("QEXP",   "no",  ["54T", "6221E"],        "total expenses"),
    ("GOV",    "no",  ["6221E", "622E", "6221T"], "revenue from government (by source)"),
    ("E_QREV", "no",  ["54T", "51T"],          "error measure for total revenue"),
]

available = df[series_columns].drop_duplicates()
chosen = []
for dtype, adj, preferred, why in picks:
    options = available[(available["data_type_code"] == dtype) & (available["seasonally_adj"] == adj)]
    if options.empty:
        print(f"skip {dtype}/{adj}: not in {SAMPLE_YEAR}")
        continue
    hit = [c for c in preferred if c in set(options["category_code"])]
    cat = hit[0] if hit else (df[(df["data_type_code"] == dtype) & (df["seasonally_adj"] == adj)]
                              ["category_code"].value_counts().index[0])
    chosen.append({"data_type_code": dtype, "seasonally_adj": adj, "category_code": cat, "why": why})

sample_series = pd.DataFrame(chosen)
print("Found", len(sample_series), "of", len(picks))
display(sample_series)

# Pull a small sample for each series: every quarter of the sample year
sampled = []
for _, s in sample_series.iterrows():
    obs = df[(df["data_type_code"] == s["data_type_code"]) &
             (df["seasonally_adj"] == s["seasonally_adj"]) &
             (df["category_code"] == s["category_code"])].sort_values("time")
    sampled.append(obs)

sample_observations = pd.concat(sampled, ignore_index=True).drop(columns=["naics", "suffix"])
sample_observations.to_csv(AUDIT / "census_qss_sample_observations.csv", index=False)
display(sample_observations)


# ===========================================================================
# 4. Save the summary and findings
# ===========================================================================

required = [n for n, d in variables.items()
            if str(d.get("required", "")).lower() == "true" and not d.get("predicateOnly")]
types = sorted({d.get("predicateType") for n, d in variables.items() if not d.get("predicateOnly")})
constant = [c for c in series_columns + ["time_slot_id"] if df[c].nunique() == 1]

summary = {
    "provider": "U.S. Census Bureau",
    "dataset": "Quarterly Services Survey",
    "dataset_identifier": entry.get("identifier", "").rsplit("/", 1)[-1],
    "dataset_path": "timeseries/eits/qss",
    "is_timeseries": entry.get("c_isTimeseries"),
    "is_cube": entry.get("c_isCube"),
    "geography": [g.get("name") for g in geography],
    "variable_count": len(variables),
    "variable_names": list(variables.keys()),
    "required_variables": required,
    "group_count": len(groups),
    "tags_count": len(tags),
    "time_granularity": "quarterly",
    "catalog_modified": entry.get("modified"),
    "coverage_first_year": first_year,
    "candidate_series_dimensions": series_columns,
    "series_count_sample_year": int(len(series_counts)),
    "duplicate_rows_with_key": int(duplicates),
    "data_type_code_count": int(df["data_type_code"].nunique()),
    "category_code_count": int(df["category_code"].nunique()),
    "observation_fields": ["time", "time_slot_id", "cell_value", "error_data"],
    "cell_value_flags": flags.to_dict(),
    "published_code_lists": {k: len(v) for k, v in code_lists.items() if v},
    "native_series_identifier": None,
    "limitations": [
        *(["Groups endpoint is empty."] if not groups else []),
        *(["Tags endpoint is empty."] if not tags else []),
        "No code labels or units in the API; code meanings must come from QSS documentation.",
        "Catalog 'modified' date is not the data release date.",
        "Estimates and their error measures (E_ codes) share the cell_value column.",
    ],
}

with open(AUDIT / "census_qss_metadata_summary.json", "w") as f:
    json.dump(summary, f, indent=2, default=str)
print(json.dumps(summary, indent=2, default=str)[:1500])

flag_text = ", ".join(f"`{k}` ({v} rows)" for k, v in flags.items()) or "none"
pub = summary["published_code_lists"]

findings = f"""# QSS Census Metadata Findings

## Dataset
- Quarterly Services Survey (QSS), path timeseries/eits/qss, identifier {summary['dataset_identifier']}
- Quarterly, national only (`us`), coverage starts {first_year} (tested), runs to 2026
- {len(variables)} variables, {len(groups)} groups, {len(tags)} tags
- Catalog entry: {len(entry)} fields; `modified` = {entry.get('modified')} (not the data release date); description is the generic EITS text

## Metadata structure
- Same EITS variable layout as MRTS and M3; data fields are typed {types} (cell_value included)
- Required: {', '.join(required)}; program_code is "default displayed"
- Only nested JSON: `time.datetime`
- Code lists published by the API: {pub if pub else 'none'}

## Dataset / series / observation levels
- Dataset: provider, path, title, contact, license, geography, frequency
- Series: no native series ID. Key = data_type_code + seasonally_adj + category_code
  ({len(series_counts):,} series in {SAMPLE_YEAR}, {duplicates} duplicates). Constant: {', '.join(constant)}
- {df['data_type_code'].nunique()} data_type_codes, {df['category_code'].nunique()} category_codes
- Observation: time (`{df['time'].iloc[0]}`), cell_value (string), error_data

## Codes
- category_code = NAICS code + suffix (T/A/E). FRED's QSS series show E = tax-exempt establishments;
  T (total) and A (taxable) still to confirm in Census documentation
- data_type_code: revenue (QREV), expenses (QEXP), revenue by source (BUS/GOV/HHD), hospital measures
  (INPAT/DISC/ADM), each with P* and E_* variants
- Rows with error_data = yes are the E_* codes: sampling-error measures stored in the same column as estimates
- cell_value flags: {flag_text}

## Sample
- {len(sample_series)} series x 4 quarters of {SAMPLE_YEAR} = {len(sample_observations)} rows in census_qss_sample_observations.csv

## Open questions
- Official definitions of the T/A suffixes, the P* prefix, and the S/Z flags
- How does QSS differ from M3/MRTS in measures and suffixes (compare inventories)
"""

(AUDIT / "census_qss_findings.md").write_text(findings)
print(findings)

print("OUTPUT FILES")
for path in sorted(list(CATALOG.glob("census_qss_*")) + list(AUDIT.glob("census_qss_*"))):
    print(f"  {path.relative_to(CENSUS.parent.parent)}: {path.stat().st_size:,} bytes")
