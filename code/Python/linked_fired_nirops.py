"""Link NIROPS and FIRED wildfire perimeters.
 
Python adaptation of `code/Python/link_fired_mtbs.py`, substituting MTBS
perimeters with NIROPS perimeters and matching FIRED daily perimeters by date.
 
For each NIROPS daily perimeter, find all FIRED daily polygons that:
  1. Spatially overlap the NIROPS perimeter, AND
  2. Share the exact same date.
 
Union the matching FIRED geometries into a single geometry per NIROPS row.
Output has one row per NIROPS daily perimeter that has at least one FIRED match.
"""
 
from __future__ import annotations
 
from pathlib import Path
 
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union
 
DEV_MODE = False   # set False for full run
DEV_N_NIROPS = 20  # test with just 20 NIROPS rows
 
# -----------------------------------------------------------------------------
# Step 1: projection and configuration
# -----------------------------------------------------------------------------
PROJECT_CRS = "EPSG:5070"  # NAD83 / Conus Albers (meters)
 
AOI_NAME = "westUS"    # Options: "westUS", "CATN", or add your own
AOI_BUFFER_M = 10_000  # Buffer distance in meters
 
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
 
NIROPS_PATH = BASE_DIR / "NIROPS_2020_2023/NIROPS_2020_2023.shp"
OUTPUT_PATH = "/Users/dalo2903/repos/ics209-plus-fired/output/linked_nirops_fired.gpkg"
 
 
# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
 
def load_aoi(
    aoi_name: str, project_crs: str, buffer_m: float
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    if aoi_name not in AOI_OPTIONS:
        raise ValueError(
            f"AOI '{aoi_name}' not found. Available: {', '.join(AOI_OPTIONS)}"
        )
    aoi = gpd.read_file(AOI_OPTIONS[aoi_name]).to_crs(project_crs)
    aoi_buffered = aoi.copy()
    aoi_buffered["geometry"] = aoi_buffered.geometry.buffer(buffer_m)
    return aoi, aoi_buffered
 
 
def spatial_filter_to_aoi(
    gdf: gpd.GeoDataFrame, aoi_buffered: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
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
 
def prepare_perimeters(
    aoi_buffered: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
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
 
    # NIROPS perimeters
    nirops = gpd.read_file(NIROPS_PATH).to_crs(PROJECT_CRS)
    nirops = spatial_filter_to_aoi(nirops, aoi_buffered)
    nirops["DateUTC"] = pd.to_datetime(nirops["DateUTC"], errors="coerce")
    nirops["perim_date"] = nirops["DateUTC"].dt.normalize()
    nirops = make_multipolygon(nirops)
    nirops = prefix_columns(nirops, "NIROPS_")
    print(
        "NIROPS time range:",
        nirops["NIROPS_perim_date"].min(),
        "to",
        nirops["NIROPS_perim_date"].max(),
    )
    print(f"  {len(nirops)} NIROPS daily perimeter rows")
 
    return fired_daily, nirops
 
 
# -----------------------------------------------------------------------------
# Step 4: match — one output row per NIROPS daily perimeter
# -----------------------------------------------------------------------------
 
def match_fired_to_nirops(
    fired_daily: gpd.GeoDataFrame,
    nirops: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """For each NIROPS daily perimeter, find all FIRED daily polygons that
    spatially overlap it AND share the exact same date.  Union those FIRED
    geometries into a single FIRED_geometry per NIROPS row.
 
    Strategy
    --------
    Rather than a pure O(n*m) loop, group both datasets by date and run a
    spatial join within each date bucket — only touching dates that appear
    in both datasets.
    """
 
    nirops_reset = nirops.reset_index(drop=True).copy()
    nirops_reset["_nirops_pos"] = nirops_reset.index
 
    # Dates present in both datasets.
    nirops_dates = set(nirops_reset["NIROPS_perim_date"].dropna().unique())
    fired_dates  = set(fired_daily["FIRED_perim_date"].dropna().unique())
    shared_dates = sorted(nirops_dates & fired_dates)
    print(f"  {len(shared_dates)} dates present in both NIROPS and FIRED.")
 
    result_rows: list[dict] = []
    nirops_cols = [c for c in nirops_reset.columns if c not in ("geometry", "_nirops_pos")]
 
    for date in shared_dates:
        nirops_day = nirops_reset[nirops_reset["NIROPS_perim_date"] == date].copy()
        fired_day  = fired_daily[fired_daily["FIRED_perim_date"] == date].copy()
 
        if nirops_day.empty or fired_day.empty:
            continue
 
        # Spatial join: for each NIROPS row find all FIRED rows on the same date.
        joined = gpd.sjoin(
            nirops_day,
            fired_day,
            how="inner",
            predicate="intersects",
        )
        if joined.empty:
            continue
 
        # Look up the actual FIRED geometries via the sjoin index_right.
        joined["FIRED_geometry"] = fired_day.loc[
            joined["index_right"], "geometry"
        ].values
 
        # Union all matched FIRED geometries per NIROPS row.
        for nirops_pos, group in joined.groupby("_nirops_pos"):
            nirops_row = nirops_day.loc[nirops_day["_nirops_pos"] == nirops_pos].iloc[0]
            fired_union = unary_union(list(group["FIRED_geometry"]))
            record = {col: nirops_row[col] for col in nirops_cols}
            record["geometry"]       = nirops_row.geometry
            record["FIRED_geometry"] = fired_union
            # Store matched FIRED_ids for reference.
            record["FIRED_ids_matched"] = ",".join(
                sorted(group["FIRED_id"].astype(str).unique())
            )
            result_rows.append(record)
 
    if not result_rows:
        raise RuntimeError("No FIRED daily polygons match any NIROPS perimeter on the same date.")
 
    result = gpd.GeoDataFrame(result_rows, geometry="geometry", crs=nirops.crs)
    result["FIRED_geometry"] = gpd.GeoSeries(result["FIRED_geometry"], crs=fired_daily.crs)
 
    print(
        f"  Final output: {len(result)} rows  |  "
        f"Unique NIROPS incidents: {result['NIROPS_Incident_C'].nunique()}  |  "
        f"Dates matched: {result['NIROPS_perim_date'].nunique()}"
    )
    return result
 
 
# -----------------------------------------------------------------------------
# Step 5 (optional): diagnostics / filtering
# -----------------------------------------------------------------------------
 
def summarize_and_filter(joined_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    joined_data = joined_data[joined_data["FIRED_ids_matched"].notna()].copy()
 
    joined_data["nirops_km2"] = joined_data["NIROPS_Acres"] * 0.00404686
 
    fig, axes = plt.subplots(1, 1, figsize=(8, 5))
    joined_data["nirops_km2"].plot.hist(ax=axes, bins=50)
    axes.set_title("NIROPS area (km2)")
    plt.tight_layout()
 
    print(f"Rows with FIRED match: {len(joined_data)}")
    print(f"Unique NIROPS incidents: {joined_data['NIROPS_Incident_C'].nunique()}")
    return joined_data
 
 
# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
aoi, aoi_buffered = load_aoi(AOI_NAME, PROJECT_CRS, AOI_BUFFER_M)
fired_daily, nirops = prepare_perimeters(aoi_buffered)
 
if DEV_MODE:
    nirops = nirops.iloc[:DEV_N_NIROPS].copy()
 
joined_data = match_fired_to_nirops(fired_daily, nirops)
 
# Optional filtering / diagnostics.
# joined_data = summarize_and_filter(joined_data)
 
# Write output layers.
print(f"Writing {len(joined_data)} rows to {OUTPUT_PATH}...")
 
# NIROPS layer: NIROPS geometry is the active geometry column.
nirops_out = joined_data.drop(columns=["FIRED_geometry"])
nirops_out.to_file(OUTPUT_PATH, layer="nirops", driver="GPKG")
print(f"  NIROPS layer written: {len(nirops_out)} rows")
 
# FIRED layer: swap active geometry to the unioned FIRED polygon.
fired_out = (
    joined_data
    .set_geometry("FIRED_geometry")
    .drop(columns=["geometry"])
)
fired_out.to_file(OUTPUT_PATH, layer="fired", driver="GPKG")
print(f"  FIRED layer written: {len(fired_out)} rows")
 
print(f"Saved NIROPS and FIRED layers to: {OUTPUT_PATH}")