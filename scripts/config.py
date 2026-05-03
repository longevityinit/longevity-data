"""Cross-pipeline filename conventions.

A typo in any of these strings used to break a downstream stage silently;
defining them once means the convention is enforced by import.

Per-dataset config (slugs, indicator renames, default selections) belongs in
dataset YAML files alongside the data, not here.
"""

# Snapshot stage
DATA_CSV_EXT = ".csv"
OWID_RAW_META_EXT = ".owid.json"
META_YAML_EXT = ".meta.yaml"
CURRENT_POINTER_FILENAME = "current.yaml"

# Chart stage
CHART_DATA_FILE_SUFFIX = "_tli.csv"
CHART_INDEX_FILENAME = "index.html"
