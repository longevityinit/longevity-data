import io
import re
import requests
import pandas as pd

def download_owid_chart_data(chart_slug: str, etag: str = None):
    """
    Downloads CSV and JSON metadata. 
    Returns None if the server returns 304 Not Modified.
    Otherwise, returns (csv_bytes, raw_metadata_dict, new_etag).
    """
    base_url = f"https://ourworldindata.org/grapher/{chart_slug}"
    headers = {'If-None-Match': etag} if etag else {}
    
    print(f"Fetching data from {base_url}.csv...")
    csv_response = requests.get(f"{base_url}.csv", headers=headers, timeout=30)

    # If the data hasn't changed, stop here and save bandwidth
    if csv_response.status_code == 304:
        return None

    csv_response.raise_for_status()

    print(f"Fetching metadata from {base_url}.metadata.json...")
    meta_response = requests.get(f"{base_url}.metadata.json", timeout=30)
    meta_response.raise_for_status()
    
    new_etag = csv_response.headers.get('ETag')
    return csv_response.content, meta_response.json(), new_etag


def _to_snake_case(name: str) -> str:
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    s = re.sub(r'[^0-9a-zA-Z]+', '_', s).strip('_')
    return s.lower()


def standardise_owid_chart_data(csv_bytes: bytes, metadata: dict):
    """
    Standardises an OWID chart CSV:
    - snake_cases the fixed columns (Entity, Code, Year)
    - renames each indicator column to its `shortName` from the metadata
      (falling back to a snake_cased version of the column header)
    Returns (DataFrame, thin_metadata_dict) where thin_metadata captures the
    chart-level title/citation and per-column display info needed downstream.
    """
    df = pd.read_csv(io.BytesIO(csv_bytes))

    fixed = {"Entity": "entity", "Code": "code", "Year": "year"}
    indicator_cols = [c for c in df.columns if c not in fixed]

    # Build a titleShort → metadata entry lookup for when CSV headers use
    # the display name rather than the full technical key OWID uses internally.
    meta_cols = metadata.get("columns", {})
    by_title_short = {v.get("titleShort"): v for v in meta_cols.values() if v.get("titleShort")}

    rename = dict(fixed)
    columns_meta = []
    for col in indicator_cols:
        col_meta = meta_cols.get(col) or by_title_short.get(col) or {}
        short_name = col_meta.get("shortName") or _to_snake_case(col)
        rename[col] = short_name
        columns_meta.append({
            "name": short_name,
            "source_title": col,
            "title_short": col_meta.get("titleShort"),
            "unit": col_meta.get("unit"),
            "short_unit": col_meta.get("shortUnit"),
            "owid_variable_id": col_meta.get("owidVariableId"),
            "last_updated": col_meta.get("lastUpdated"),
            "next_update": col_meta.get("nextUpdate"),
            "citation_short": col_meta.get("citationShort"),
            "citation_long": col_meta.get("citationLong"),
            "description_short": col_meta.get("descriptionShort"),
        })

    df = df.rename(columns=rename)

    dupes = int(df.duplicated(subset=["entity", "year"]).sum())
    if dupes:
        raise ValueError(f"Found {dupes} duplicate (entity, year) rows after standardisation")

    chart_meta = metadata.get("chart", {})
    thin_meta = {
        "title": chart_meta.get("title"),
        "citation": chart_meta.get("citation"),
        "original_chart_url": chart_meta.get("originalChartUrl"),
        "columns": columns_meta,
    }
    return df, thin_meta