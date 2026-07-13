"""Link WUMI and FIRED wildfire events.

Adapted from `code/Python/link_fired_mtbs.py`, substituting MTBS perimeters
with WUMI (Western United States MTBS-Interagency Database of Large
Wildfires, 1984-2024, WUMI2024a) perimeters.

Differs from the MTBS script in one key way (borrowed from
`linked_fired_nirops.py`): instead of keeping only the single FIRED event
with the largest overlap for each WUMI fire, this script UNIONS every FIRED
event that spatially overlaps a given WUMI fire (within the same year) into
one combined `FIRED_geometry` per WUMI row. Matching is otherwise done the
same way as the MTBS script: by year, then spatial intersection.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union

# -----------------------------------------------------------------------------
# Step 1: projection and configuration
# -----------------------------------------------------------------------------
PROJECT_CRS = "EPSG:5070"  # NAD83 / Conus Albers (meters)

AOI_NAME = "westUS"    # Options: "westUS", "CATN", or add your own
AOI_BUFFER_M = 10_000   # Buffer distance in meters

# -----------------------------------------------------------------------------
# Step 2: paths
# -----------------------------------------------------------------------------
AOI_OPTIONS = {
    "westUS": Path("/Users/dalo2903/Downloads/data/spatial/raw/aoi/westUS_5070.gpkg"),
    "CATN":   Path("/Users/dalo2903/Downloads/data/spatial/raw/aoi/CA_TN.gpkg"),
}

FIRED_EVENTS_PATH = Path(
    "/Users/dalo2903/Downloads/data/spatial/raw/FIRED/"
    "fired_conus_ak_2000_to_2025_S5_T11/"
    "fired_conus_ak_2000_to_2025_S5_T11/"
    "fired_conus_ak_2000_to_2025_events.shp"
)

WUMI_PATH = Path(
    "/Users/dalo2903/Downloads/WUMI/WUMI2024a_main_fires_unified_no_circles.gpkg"
)

OUTPUT_PATH = "/Users/dalo2903/repos/ics209-plus-fired/output/linked_wumi_fired.gpkg"

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
        available = ", ".join(AOI_OPTIONS.keys())
        raise ValueError(f"AOI '{aoi_name}' not found. Available options: {available}")

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
    # FIRED events
    fired_events = gpd.read_file(FIRED_EVENTS_PATH).to_crs(PROJECT_CRS)
    fired_events = spatial_filter_to_aoi(fired_events, aoi_buffered)
    fired_events["ig_date"] = pd.to_datetime(fired_events["ig_date"], errors="coerce")
    fired_events["last_date"] = pd.to_datetime(fired_events["last_date"], errors="coerce")
    fired_events = make_multipolygon(fired_events)
    fired_events = prefix_columns(fired_events, "FIRED_")

    print(
        "FIRED time range:",
        fired_events["FIRED_ig_date"].min(),
        "to",
        fired_events["FIRED_last_date"].max(),
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

    return fired_events, wumi


# -----------------------------------------------------------------------------
# Step 4: match — union ALL overlapping FIRED events per WUMI fire
# -----------------------------------------------------------------------------

def union_overlaps_by_year(
    wumi_yr: gpd.GeoDataFrame,
    fired_yr: gpd.GeoDataFrame,
    wumi_cols: list[str],
) -> list[dict]:
    """For a single year, find every FIRED event that spatially overlaps
    each WUMI fire and union those FIRED geometries into one combined
    FIRED_geometry per WUMI row. Unlike `join_largest_overlap` in the MTBS
    script, this keeps *all* overlapping FIRED events rather than only the
    single largest-overlap match.
    """
    if wumi_yr.empty or fired_yr.empty:
        return []

    joined = gpd.sjoin(wumi_yr, fired_yr, how="inner", predicate="intersects")
    if joined.empty:
        return []

    # Look up the actual FIRED geometries via the sjoin index_right.
    joined["FIRED_geometry"] = fired_yr.loc[joined["index_right"], "geometry"].values

    rows: list[dict] = []
    for wumi_pos, group in joined.groupby(joined.index):
        wumi_row = wumi_yr.loc[wumi_pos]
        fired_union = unary_union(list(group["FIRED_geometry"]))
        record = {col: wumi_row[col] for col in wumi_cols}
        record["geometry"] = wumi_row.geometry
        record["FIRED_geometry"] = fired_union
        record["FIRED_ids_matched"] = ",".join(
            sorted(group["FIRED_id"].astype(str).unique())
        )
        record["FIRED_n_matched"] = group["FIRED_id"].nunique()
        rows.append(record)

    return rows


def run_join(fired_events: gpd.GeoDataFrame, wumi: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    wumi_year_col = f"WUMI_{WUMI_YEAR_COL}"

    fired_years = set(fired_events["FIRED_ig_date"].dt.year.dropna().astype(int).tolist())
    wumi_years = set(pd.to_numeric(wumi[wumi_year_col], errors="coerce").dropna().astype(int).tolist())
    years = sorted(fired_years.intersection(wumi_years))

    if not years:
        raise RuntimeError("No overlapping years found between FIRED and WUMI.")

    print(f"Join years between WUMI and FIRED: {min(years)} - {max(years)}")

    wumi_cols = [c for c in wumi.columns if c != "geometry"]

    all_rows: list[dict] = []
    for yr in years:
        print(f"Processing year: {yr}")
        fired_yr = fired_events[fired_events["FIRED_ig_date"].dt.year == yr].copy()
        wumi_yr = wumi[wumi[wumi_year_col] == yr].copy()
        all_rows.extend(union_overlaps_by_year(wumi_yr, fired_yr, wumi_cols))

    if not all_rows:
        raise RuntimeError("Spatial join returned no matches.")

    joined_data = gpd.GeoDataFrame(all_rows, geometry="geometry", crs=wumi.crs)
    joined_data["FIRED_geometry"] = gpd.GeoSeries(joined_data["FIRED_geometry"], crs=fired_events.crs)

    print(f"Total joined features: {len(joined_data)}")
    return joined_data


# -----------------------------------------------------------------------------
# Step 5 (optional): diagnostics / filtering
# -----------------------------------------------------------------------------

def summarize(joined_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    wumi_id_col = f"WUMI_{WUMI_ID_COL}"
    wumi_area_ha_col = f"WUMI_{WUMI_AREA_HA_COL}"

    joined_data = joined_data[joined_data["FIRED_ids_matched"].notna()].copy()

    if wumi_area_ha_col in joined_data.columns:
        joined_data["wumi_km2"] = joined_data[wumi_area_ha_col] * 0.01  # ha -> km2

    print(f"Rows with FIRED match: {len(joined_data)}")
    if wumi_id_col in joined_data.columns:
        print(f"Unique WUMI fires: {joined_data[wumi_id_col].nunique()}")
    print(f"FIRED events matched per WUMI fire — summary:")
    print(joined_data["FIRED_n_matched"].describe())

    fig, ax = plt.subplots(1, 1, figsize=(8, 5))
    joined_data["FIRED_n_matched"].plot.hist(ax=ax, bins=30)
    ax.set_title("Number of FIRED events unioned per WUMI fire")
    plt.tight_layout()

    return joined_data


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
print(f"Configuration: AOI={AOI_NAME}, buffer={AOI_BUFFER_M} m")

aoi, aoi_buffered = load_aoi(AOI_NAME, PROJECT_CRS, AOI_BUFFER_M)
fired_events, wumi = prepare_perimeters(aoi_buffered)

print(
    {
        "aoi": aoi.crs,
        "aoi_buffered": aoi_buffered.crs,
        "firedEvents": fired_events.crs,
        "wumi": wumi.crs,
    }
)

joined_data = run_join(fired_events, wumi)
joined_data = summarize(joined_data)

# Write output layers.
print(f"Writing {len(joined_data)} rows to {OUTPUT_PATH}...")

# WUMI layer: WUMI geometry is the active geometry column.
wumi_out = joined_data.drop(columns=["FIRED_geometry"])
wumi_out.to_file(OUTPUT_PATH, layer="wumi", driver="GPKG")
print(f"  WUMI layer written: {len(wumi_out)} rows")

# FIRED layer: swap active geometry to the unioned FIRED polygon.
fired_out = joined_data.set_geometry("FIRED_geometry").drop(columns=["geometry"])
fired_out.to_file(OUTPUT_PATH, layer="fired", driver="GPKG")
print(f"  FIRED layer written: {len(fired_out)} rows")

print(f"Saved WUMI and FIRED layers to: {OUTPUT_PATH}")
