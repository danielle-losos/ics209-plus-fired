"""Link NIROPS events to FIRED DAILY perimeters, using the NIROPS event's
maximum-area daily perimeter as the spatial filter.

Adapted from `link_fired_wumi_events.py`, substituting the (complex-filtered)
NIROPS daily perimeter file in place of WUMI.

Unlike WUMI, NIROPS *does* have daily perimeter geometry (one row per
Incident_C per day). But per the requested matching logic, we don't match
FIRED day-by-day against every NIROPS daily polygon -- instead, for each
NIROPS event (grouped by `Incident_C`) we collapse its daily perimeters down
to a single representative polygon: the day with the largest footprint
(the event's maximum extent). That single polygon is then used exactly like
a WUMI event polygon was used before -- as a spatial filter, not a per-date
match -- so a FIRED daily polygon qualifies as belonging to a NIROPS event if:
  1. it falls in the same year as the NIROPS event, AND
  2. it spatially intersects that NIROPS event's max-area perimeter.

Because there's no per-day NIROPS geometry paired to each matched FIRED
daily polygon (we intentionally reduced NIROPS to one polygon per event),
the output keeps FIRED daily geometry as the sole active geometry column,
same as the WUMI version. Matched NIROPS event(s) are recorded as reference
columns (id/name/count) rather than as a second geometry layer.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union

DEV_MODE = False    # set True to test on a handful of NIROPS events
DEV_N_NIROPS = 20   # test with just 20 NIROPS events

# -----------------------------------------------------------------------------
# Step 1: projection and configuration
# -----------------------------------------------------------------------------
PROJECT_CRS = "EPSG:5070"  # NAD83 / Conus Albers (meters)

AOI_NAME = "westUS"    # Options: "westUS", "CATN", or add your own
AOI_BUFFER_M = 10_000   # Buffer distance in meters

# -----------------------------------------------------------------------------
# Step 2: paths
# -----------------------------------------------------------------------------
BASE_DIR = Path("/Users/dalo2903/Downloads/data/spatial/raw")

AOI_OPTIONS = {
    "westUS": BASE_DIR / "aoi/westUS_5070.gpkg",
    "CATN":   BASE_DIR / "aoi/CA_TN.gpkg",
}

FIRED_DAILY_PATH = (
    BASE_DIR
    / "FIRED/fired_conus_ak_2000_to_2025_S5_T11"
    / "fired_conus_ak_2000_to_2025_S5_T11"
    / "fired_conus_ak_2000_to_2025_daily.shp"
)

NIROPS_PATH = Path("/Users/dalo2903/NIROPS_2020_2023_no_complexes.shp")

OUTPUT_PATH = "/Users/dalo2903/repos/ics209-plus-fired/output/linked_nirops_fired_daily.gpkg"

# -----------------------------------------------------------------------------
# Step 2b: NIROPS column names
# -----------------------------------------------------------------------------
NIROPS_ID_COL   = "Incident_C"  # unique event identifier -- defines a NIROPS event
NIROPS_NAME_COL = "Inc Name"    # fire name (optional -- skipped if missing)
NIROPS_DATE_COL = "DateUTC"     # per-perimeter date; used to pick max-area day and event year


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def load_aoi(aoi_name: str, project_crs: str, buffer_m: float) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    if aoi_name not in AOI_OPTIONS:
        raise ValueError(
            f"AOI '{aoi_name}' not found. Available: {', '.join(AOI_OPTIONS)}"
        )
    aoi = gpd.read_file(AOI_OPTIONS[aoi_name]).to_crs(project_crs)
    aoi_buffered = aoi.copy()
    aoi_buffered["geometry"] = aoi_buffered.geometry.buffer(buffer_m)
    return aoi, aoi_buffered


def spatial_filter_to_aoi(gdf: gpd.GeoDataFrame, aoi_buffered: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return gdf[gdf.geometry.intersects(aoi_buffered.union_all())].copy()


def prefix_columns(gdf: gpd.GeoDataFrame, prefix: str) -> gpd.GeoDataFrame:
    rename_map = {c: f"{prefix}{c}" for c in gdf.columns if c != "geometry"}
    return gdf.rename(columns=rename_map)


def make_multipolygon(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()
    out["geometry"] = out.geometry.make_valid()
    out = out.explode(index_parts=False).reset_index(drop=True)
    out = out[out.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    def _to_multi(geom):
        if isinstance(geom, MultiPolygon):
            return geom
        if isinstance(geom, Polygon):
            return MultiPolygon([geom])
        return geom

    out["geometry"] = out.geometry.apply(_to_multi)
    return out


def build_nirops_event_polygons(
    nirops: gpd.GeoDataFrame, id_col: str, date_col: str
) -> gpd.GeoDataFrame:
    """Collapse NIROPS daily perimeters down to ONE representative polygon per
    event (`id_col`): the day with the largest footprint (max extent).

    Steps:
      1. Parse `date_col` and normalize to a day (drop time-of-day).
      2. Dissolve (union) any rows that share the same (id_col, date) --
         handles datasets where a single day's perimeter is split across
         multiple feature rows -- so daily area is computed correctly even
         if the day's footprint is multi-part.
      3. Compute each day's area (in PROJECT_CRS meters).
      4. For each event, keep only the day with the max area.
      5. Attach `event_year` = the year of the event's EARLIEST perimeter
         date (not necessarily the max-area day's date), so year-matching
         reflects when the fire started.
    """
    nirops = nirops.copy()
    nirops["geometry"] = nirops.geometry.make_valid()
    nirops["_date"] = pd.to_datetime(nirops[date_col], errors="coerce").dt.normalize()
    nirops = nirops.dropna(subset=["_date"])

    # dissolve same-day fragments into one geometry per (event, date)
    daily = nirops.dissolve(by=[id_col, "_date"], aggfunc="first").reset_index()
    daily["_area_m2"] = daily.geometry.area

    event_year = daily.groupby(id_col)["_date"].min().dt.year.rename("event_year")

    idx_max = daily.groupby(id_col)["_area_m2"].idxmax()
    rep = daily.loc[idx_max].copy()
    rep = rep.merge(event_year, left_on=id_col, right_index=True, how="left")
    rep = rep.drop(columns=["_area_m2", "_date"])

    print(
        f"  Collapsed {len(nirops)} NIROPS daily rows -> "
        f"{len(daily)} unique (event, date) footprints -> "
        f"{len(rep)} max-extent event polygons."
    )
    return rep


# -----------------------------------------------------------------------------
# Step 3: load data
# -----------------------------------------------------------------------------

def prepare_perimeters(aoi_buffered: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    # FIRED daily perimeters
    fired_daily = gpd.read_file(FIRED_DAILY_PATH).to_crs(PROJECT_CRS)
    fired_daily = spatial_filter_to_aoi(fired_daily, aoi_buffered)
    fired_daily["date"] = pd.to_datetime(fired_daily["date"], errors="coerce")
    fired_daily["perim_date"] = fired_daily["date"].dt.normalize()
    fired_daily = make_multipolygon(fired_daily)
    fired_daily = prefix_columns(fired_daily, "FIRED_")
    print(
        "FIRED daily time range:",
        fired_daily["FIRED_perim_date"].min(),
        "to",
        fired_daily["FIRED_perim_date"].max(),
    )

    # NIROPS perimeters -> collapsed to one max-extent polygon per event
    nirops_raw = gpd.read_file(NIROPS_PATH).to_crs(PROJECT_CRS)
    nirops_raw = spatial_filter_to_aoi(nirops_raw, aoi_buffered)
    print("NIROPS columns:", nirops_raw.columns.tolist())

    nirops_events = build_nirops_event_polygons(nirops_raw, NIROPS_ID_COL, NIROPS_DATE_COL)
    nirops_events = make_multipolygon(nirops_events)
    nirops_events = prefix_columns(nirops_events, "NIROPS_")

    nirops_year_col = "NIROPS_event_year"
    print(
        "NIROPS event-year range:",
        nirops_events[nirops_year_col].min(),
        "to",
        nirops_events[nirops_year_col].max(),
    )
    print(f"  {len(nirops_events)} NIROPS event polygons")
    print(f"  NIROPS columns: {list(nirops_events.columns)}")

    return fired_daily, nirops_events


# -----------------------------------------------------------------------------
# Step 4: match — one output row per (NIROPS event, date) combination, with
# every overlapping FIRED daily fragment on that date UNIONED into a single
# geometry. NIROPS max-extent geometry is used both to filter/match and to
# define the event identity (NIROPS_ids_matched).
# -----------------------------------------------------------------------------

def match_fired_daily_to_nirops(
    fired_daily: gpd.GeoDataFrame,
    nirops_events: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """For each year, find every FIRED daily polygon that spatially overlaps
    at least one NIROPS event's max-extent polygon from the same year.
    Matching is by year + spatial intersection only (the NIROPS side has
    been reduced to a single representative polygon per event, so there is
    no daily NIROPS geometry to match by exact date).

    A single FIRED "event day" can be represented by more than one raw FIRED
    daily polygon (disconnected fragments that share the same FIRED_id and
    date but haven't been dissolved into one feature). To guarantee one
    output row per day per fire event, this function:
      1. Matches each raw FIRED daily row to the NIROPS event(s) it overlaps
         (producing a `NIROPS_ids_matched` string per raw row).
      2. Groups all raw FIRED rows sharing the same (`NIROPS_ids_matched`,
         date) pair and UNIONS their geometries into a single row.

    The NIROPS id combination (not FIRED_id) defines fire-event identity in
    the output. On days where a fire's footprint overlaps a different (or
    an additional) NIROPS event than usual -- e.g. a merge with a
    neighboring fire -- that day will carry a different `NIROPS_ids_matched`
    value and therefore represent a distinct event-day combination; this is
    intentional, not a bug, since NIROPS overlap is the defined event key.
    """
    nirops_id_col = "NIROPS_" + NIROPS_ID_COL
    nirops_name_col_candidate = "NIROPS_" + NIROPS_NAME_COL
    nirops_name_col = nirops_name_col_candidate if nirops_name_col_candidate in nirops_events.columns else None
    nirops_year_col = "NIROPS_event_year"

    fired_reset = fired_daily.reset_index(drop=True).copy()
    fired_reset["_fired_pos"] = fired_reset.index
    fired_reset["_fired_year"] = fired_reset["FIRED_perim_date"].dt.year

    nirops_years = set(
        pd.to_numeric(nirops_events[nirops_year_col], errors="coerce").dropna().astype(int).tolist()
    )
    fired_years = set(fired_reset["_fired_year"].dropna().astype(int).tolist())
    shared_years = sorted(nirops_years & fired_years)
    print(f"  {len(shared_years)} years present in both NIROPS and FIRED daily.")

    fired_cols = [c for c in fired_reset.columns if c not in ("geometry", "_fired_pos", "_fired_year")]

    # --- Pass 1: per-raw-FIRED-row match --------------------------------
    matched_rows: list[dict] = []
    for yr in shared_years:
        fired_yr = fired_reset[fired_reset["_fired_year"] == yr].copy()
        nirops_yr = nirops_events[nirops_events[nirops_year_col] == yr].copy()

        if fired_yr.empty or nirops_yr.empty:
            continue

        # Spatial join: for each FIRED daily row, find all NIROPS events it overlaps this year.
        joined = gpd.sjoin(fired_yr, nirops_yr, how="inner", predicate="intersects")
        if joined.empty:
            continue

        for fired_pos, group in joined.groupby("_fired_pos"):
            fired_row = fired_yr.loc[fired_yr["_fired_pos"] == fired_pos].iloc[0]
            record = {col: fired_row[col] for col in fired_cols}
            record["geometry"] = fired_row.geometry
            record["NIROPS_ids_matched"] = ",".join(
                sorted(group[nirops_id_col].astype(str).unique())
            )
            if nirops_name_col and nirops_name_col in group.columns:
                record["NIROPS_names_matched"] = ",".join(
                    sorted(group[nirops_name_col].astype(str).unique())
                )
            record["NIROPS_n_matched"] = group[nirops_id_col].nunique()
            matched_rows.append(record)

    if not matched_rows:
        raise RuntimeError("No FIRED daily polygons overlap any NIROPS event in a shared year.")

    matched = gpd.GeoDataFrame(matched_rows, geometry="geometry", crs=fired_daily.crs)

    n_raw_dupes = matched.duplicated(subset=["NIROPS_ids_matched", "FIRED_perim_date"]).sum()
    print(
        f"  {len(matched)} raw FIRED-row matches before collapsing; "
        f"{n_raw_dupes} share a (NIROPS_ids_matched, date) combo with another row "
        f"and will be unioned together."
    )

    # --- Pass 2: collapse to one row per (NIROPS event, date) ------------
    static_attr_cols = [
        c for c in fired_cols
        if c not in ("FIRED_perim_date",)
    ]

    result_rows: list[dict] = []
    for (nirops_ids, date), group in matched.groupby(["NIROPS_ids_matched", "FIRED_perim_date"]):
        first = group.iloc[0]
        record = {col: first[col] for col in static_attr_cols}
        record["FIRED_perim_date"] = date
        record["geometry"] = unary_union(list(group.geometry))
        record["NIROPS_ids_matched"] = nirops_ids
        record["NIROPS_names_matched"] = first.get("NIROPS_names_matched")
        record["NIROPS_n_matched"] = first["NIROPS_n_matched"]
        record["FIRED_ids_merged"] = ",".join(
            sorted(group["FIRED_id"].astype(str).unique())
        ) if "FIRED_id" in group.columns else None
        record["FIRED_n_merged"] = group["FIRED_id"].nunique() if "FIRED_id" in group.columns else len(group)
        result_rows.append(record)

    result = gpd.GeoDataFrame(result_rows, geometry="geometry", crs=fired_daily.crs)

    print(
        f"  Final output: {len(result)} rows (one per NIROPS-event-day)  |  "
        f"Unique NIROPS event combinations: {result['NIROPS_ids_matched'].nunique()}  |  "
        f"Unique NIROPS events matched: {result['NIROPS_ids_matched'].str.split(',').explode().nunique()}  |  "
        f"Rows where >1 raw FIRED fragment was merged: {(result['FIRED_n_merged'] > 1).sum()}"
    )
    return result


# -----------------------------------------------------------------------------
# Step 5 (optional): diagnostics
# -----------------------------------------------------------------------------

def summarize(joined_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    joined_data = joined_data[joined_data["NIROPS_ids_matched"].notna()].copy()

    print(f"Rows with NIROPS match: {len(joined_data)}")
    print("NIROPS events matched per FIRED daily polygon — summary:")
    print(joined_data["NIROPS_n_matched"].describe())

    fig, ax = plt.subplots(1, 1, figsize=(8, 5))
    joined_data["NIROPS_n_matched"].plot.hist(ax=ax, bins=30)
    ax.set_title("Number of NIROPS events overlapping each FIRED daily polygon")
    plt.tight_layout()

    return joined_data


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
aoi, aoi_buffered = load_aoi(AOI_NAME, PROJECT_CRS, AOI_BUFFER_M)
fired_daily, nirops_events = prepare_perimeters(aoi_buffered)

if DEV_MODE:
    nirops_events = nirops_events.iloc[:DEV_N_NIROPS].copy()

joined_data = match_fired_daily_to_nirops(fired_daily, nirops_events)

# Optional filtering / diagnostics.
# joined_data = summarize(joined_data)

# Write output. Only FIRED daily geometry is written -- NIROPS was collapsed
# to a single max-extent polygon per event purely as a spatial filter, so
# this is a single layer (unlike a events script that writes both layers).
print(f"Writing {len(joined_data)} rows to {OUTPUT_PATH}...")
joined_data.to_file(OUTPUT_PATH, layer="fired_daily", driver="GPKG")
print(f"  FIRED daily layer written: {len(joined_data)} rows")

print(f"Saved FIRED daily (NIROPS-filtered) layer to: {OUTPUT_PATH}")