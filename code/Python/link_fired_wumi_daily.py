"""Link WUMI events to FIRED DAILY perimeters.

Adapted from `link_fired_wumi_events.py` (event-level FIRED), substituting
the FIRED daily perimeter file used by `linked_fired_nirops.py` in place of
the FIRED events file.

WUMI has no daily perimeter geometry, only one polygon per fire, so this
script uses the WUMI event polygon purely as a FILTER: for each year, keep
every FIRED daily polygon that spatially overlaps at least one WUMI event
polygon from that same year. Matching is by YEAR + spatial overlap only
(not by exact date, unlike the NIROPS script, since WUMI has no per-day
geometry to match against).

Because there is no daily WUMI geometry to pair with each matched FIRED
daily polygon, the output keeps the FIRED daily geometry as the sole active
geometry column. Matched WUMI fire(s) are recorded as reference columns
(id/name/count) rather than as a second geometry layer.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon

DEV_MODE = False   # set True to test on a handful of WUMI rows
DEV_N_WUMI = 20    # test with just 20 WUMI rows

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

WUMI_PATH = Path(
    "/Users/dalo2903/Downloads/WUMI/WUMI2024a_main_fires_unified_no_circles.gpkg"
)

OUTPUT_PATH = "/Users/dalo2903/repos/ics209-plus-fired/output/linked_wumi_fired_daily.gpkg"

# -----------------------------------------------------------------------------
# Step 2b: WUMI column names
# -----------------------------------------------------------------------------
WUMI_ID_COL      = "fireid"        # unique fire identifier
WUMI_NAME_COL    = "name"          # fire name
WUMI_DATE_COL    = "date"          # ignition/discovery date column
WUMI_AREA_HA_COL = "poly_area_ha"  # burned area, hectares (used only in diagnostics)
WUMI_YEAR_COL    = "fire_year"     # fire year (used directly for year-based matching)


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

    # WUMI perimeters
    wumi = gpd.read_file(WUMI_PATH).to_crs(PROJECT_CRS)
    wumi = spatial_filter_to_aoi(wumi, aoi_buffered)
    wumi[WUMI_DATE_COL] = pd.to_datetime(wumi[WUMI_DATE_COL], errors="coerce")
    wumi = make_multipolygon(wumi)
    wumi = prefix_columns(wumi, "WUMI_")

    wumi_date_col = f"WUMI_{WUMI_DATE_COL}"
    print(
        "WUMI time range:",
        wumi[wumi_date_col].min(),
        "to",
        wumi[wumi_date_col].max(),
    )
    print(f"  {len(wumi)} WUMI fire rows")
    print(f"  WUMI columns: {list(wumi.columns)}")

    return fired_daily, wumi


# -----------------------------------------------------------------------------
# Step 4: match — one output row per FIRED daily polygon that overlaps a
# WUMI fire from the same year (WUMI geometry used only as a filter).
# -----------------------------------------------------------------------------

def match_fired_daily_to_wumi(
    fired_daily: gpd.GeoDataFrame,
    wumi: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """For each year, keep every FIRED daily polygon that spatially
    overlaps at least one WUMI fire polygon from the same year. Matching is
    by year + spatial intersection only (no daily WUMI geometry exists to
    match by exact date). Output geometry is the FIRED daily perimeter;
    matched WUMI fire(s) are recorded as reference columns.
    """
    wumi_id_col = f"WUMI_{WUMI_ID_COL}"
    wumi_name_col = f"WUMI_{WUMI_NAME_COL}"
    wumi_year_col = f"WUMI_{WUMI_YEAR_COL}"

    fired_reset = fired_daily.reset_index(drop=True).copy()
    fired_reset["_fired_pos"] = fired_reset.index
    fired_reset["_fired_year"] = fired_reset["FIRED_perim_date"].dt.year

    wumi_years = set(pd.to_numeric(wumi[wumi_year_col], errors="coerce").dropna().astype(int).tolist())
    fired_years = set(fired_reset["_fired_year"].dropna().astype(int).tolist())
    shared_years = sorted(wumi_years & fired_years)
    print(f"  {len(shared_years)} years present in both WUMI and FIRED daily.")

    fired_cols = [c for c in fired_reset.columns if c not in ("geometry", "_fired_pos", "_fired_year")]

    result_rows: list[dict] = []
    for yr in shared_years:
        fired_yr = fired_reset[fired_reset["_fired_year"] == yr].copy()
        wumi_yr = wumi[wumi[wumi_year_col] == yr].copy()

        if fired_yr.empty or wumi_yr.empty:
            continue

        # Spatial join: for each FIRED daily row, find all WUMI fires it overlaps this year.
        joined = gpd.sjoin(fired_yr, wumi_yr, how="inner", predicate="intersects")
        if joined.empty:
            continue

        for fired_pos, group in joined.groupby("_fired_pos"):
            fired_row = fired_yr.loc[fired_yr["_fired_pos"] == fired_pos].iloc[0]
            record = {col: fired_row[col] for col in fired_cols}
            record["geometry"] = fired_row.geometry
            record["WUMI_ids_matched"] = ",".join(
                sorted(group[wumi_id_col].astype(str).unique())
            )
            if wumi_name_col in group.columns:
                record["WUMI_names_matched"] = ",".join(
                    sorted(group[wumi_name_col].astype(str).unique())
                )
            record["WUMI_n_matched"] = group[wumi_id_col].nunique()
            result_rows.append(record)

    if not result_rows:
        raise RuntimeError("No FIRED daily polygons overlap any WUMI fire in a shared year.")

    result = gpd.GeoDataFrame(result_rows, geometry="geometry", crs=fired_daily.crs)

    print(
        f"  Final output: {len(result)} rows  |  "
        f"Unique FIRED daily events matched: {result['FIRED_id'].nunique() if 'FIRED_id' in result.columns else 'n/a'}  |  "
        f"Unique WUMI fires matched: {result['WUMI_ids_matched'].str.split(',').explode().nunique()}"
    )
    return result


# -----------------------------------------------------------------------------
# Step 5 (optional): diagnostics
# -----------------------------------------------------------------------------

def summarize(joined_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    joined_data = joined_data[joined_data["WUMI_ids_matched"].notna()].copy()

    print(f"Rows with WUMI match: {len(joined_data)}")
    print("WUMI fires matched per FIRED daily polygon — summary:")
    print(joined_data["WUMI_n_matched"].describe())

    fig, ax = plt.subplots(1, 1, figsize=(8, 5))
    joined_data["WUMI_n_matched"].plot.hist(ax=ax, bins=30)
    ax.set_title("Number of WUMI fires overlapping each FIRED daily polygon")
    plt.tight_layout()

    return joined_data


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
aoi, aoi_buffered = load_aoi(AOI_NAME, PROJECT_CRS, AOI_BUFFER_M)
fired_daily, wumi = prepare_perimeters(aoi_buffered)

if DEV_MODE:
    wumi = wumi.iloc[:DEV_N_WUMI].copy()

joined_data = match_fired_daily_to_wumi(fired_daily, wumi)

# Optional filtering / diagnostics.
# joined_data = summarize(joined_data)

# Write output. Only FIRED daily geometry is written — there is no daily
# WUMI geometry to pair with it, so this is a single layer (unlike the
# events script, which writes both a WUMI layer and a FIRED layer).
print(f"Writing {len(joined_data)} rows to {OUTPUT_PATH}...")
joined_data.to_file(OUTPUT_PATH, layer="fired_daily", driver="GPKG")
print(f"  FIRED daily layer written: {len(joined_data)} rows")

print(f"Saved FIRED daily (WUMI-filtered) layer to: {OUTPUT_PATH}")