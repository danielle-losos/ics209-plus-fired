"""Link WUMI and FIRED wildfire perimeters.

Python adaptation of `code/Python/link_fired_mtbs.py`, substituting MTBS
perimeters with WUMI perimeters and matching FIRED daily perimeters by date.
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
# EPSG:5070 - NAD83 / Conus Albers (meters)
PROJECT_CRS = "EPSG:5070"

# Area of Interest
AOI_NAME = "westUS"  # Options: "westUS", "CATN", or add your own
AOI_BUFFER_M = 10_000  # Buffer distance in meters

# Geometry Type (kept for parity with prior script; not used in this script)
USE_GEOMETRY = "FIRED"  # Options: "FIRED" or "NIROPS"

# FIRED Perimeter Level (kept for parity with prior script; not used in this script)
USE_DAILY_PERIMS = False


# -----------------------------------------------------------------------------
# Step 2: load and buffer AOI
# -----------------------------------------------------------------------------
BASE_DIR = Path("/Users/dalo2903/Downloads/data/spatial/raw")

AOI_OPTIONS = {
    "westUS": BASE_DIR / "aoi/westUS_5070.gpkg",
    "CATN": BASE_DIR / "aoi/CA_TN.gpkg",
}

FIRED_DAILY_PATH = BASE_DIR / "FIRED/fired_conus_ak_2000_to_2025_S5_T11/" \
                              "fired_conus_ak_2000_to_2025_S5_T11/" \
                              "fired_conus_ak_2000_to_2025_daily.shp"

NIROPS_PATH = BASE_DIR / "NIROPS_2020_2023/NIROPS_2020_2023.shp"
OUTPUT_PATH = "/Users/dalo2903/repos/ics209-plus-fired/output/linked_nirops_fired.gpkg"

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


def prepare_perimeters(aoi_buffered: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    # FIRED daily perimeters
    fired_daily = gpd.read_file(FIRED_DAILY_PATH).to_crs(PROJECT_CRS)
    fired_daily = spatial_filter_to_aoi(fired_daily, aoi_buffered)
    # Preserve the full FIRED perimeter geometry before any explode/splitting.
    fired_daily["orig_geometry"] = fired_daily.geometry

    fired_daily['date'] = pd.to_datetime(fired_daily['date'], errors="coerce")
    fired_daily["perim_date"] = fired_daily['date'].dt.normalize()
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

    return fired_daily, nirops

def join_largest_overlap(nirops_yr: gpd.GeoDataFrame, fired_yr: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Return NIROPS->FIRED spatial join with FIRED geometry unioned per NIROPS feature."""
    if nirops_yr.empty or fired_yr.empty:
        return gpd.GeoDataFrame(
            columns=list(nirops_yr.columns) + [c for c in fired_yr.columns if c != "geometry"],
            geometry="geometry",
            crs=nirops_yr.crs,
        )

    candidates = gpd.sjoin(nirops_yr, fired_yr, how="inner", predicate="intersects")
    if candidates.empty:
        return candidates

    fired_lookup = fired_yr.geometry
    candidates = candidates.copy()
    candidates["FIRED_geometry"] = candidates["index_right"].apply(lambda idx: fired_lookup.loc[idx])

    grouped = candidates.groupby(candidates.index)
    result = grouped.agg(
        {
            **{
                col: "first"
                for col in candidates.columns
                if col not in ["FIRED_geometry", "geometry", "index_right"]
            },
            "FIRED_geometry": lambda geoms: unary_union(list(geoms)),
            "geometry": "first",
        }
    )

    if "geometry_right" in result.columns:
        result = result.drop(columns=["geometry_right"])

    result = gpd.GeoDataFrame(result, geometry="geometry", crs=nirops_yr.crs)
    result["FIRED_geometry"] = gpd.GeoSeries(result["FIRED_geometry"], crs=fired_yr.crs)
    return result


def run_join(fired_daily: gpd.GeoDataFrame, nirops: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    fired_dates = set(fired_daily["FIRED_perim_date"].dropna().tolist())
    nirops_dates = set(nirops["NIROPS_perim_date"].dropna().tolist())
    dates = sorted(fired_dates.intersection(nirops_dates))

    if not dates:
        raise RuntimeError("No overlapping daily dates found between FIRED and NIROPS.")

    print(f"Join dates between NIROPS and FIRED: {dates[0].date()} - {dates[-1].date()}")

    joins: list[gpd.GeoDataFrame] = []
    for date in dates:
        fired_day = fired_daily[fired_daily["FIRED_perim_date"] == date].copy()
        nirops_day = nirops[nirops["NIROPS_perim_date"] == date].copy()
        joined = join_largest_overlap(nirops_day, fired_day)
        if not joined.empty:
            joins.append(joined)

    if not joins:
        raise RuntimeError("Spatial join returned no matches.")

    joined_data = pd.concat(joins, ignore_index=True)
    joined_data = gpd.GeoDataFrame(joined_data, geometry="geometry", crs=nirops.crs)
    print(f"Total joined features: {len(joined_data)}")
    return joined_data


def summarize_and_filter(joined_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    joined_data = joined_data[joined_data["FIRED_id"].notna()].copy()

    joined_data["nirops_km2"] = joined_data["NIROPS_Acres"] * 0.00404686
    if "FIRED_daily_ar_km2" in joined_data.columns:
        fired_area_col = "FIRED_daily_ar_km2"
    elif "FIRED_tot_ar_km2" in joined_data.columns:
        fired_area_col = "FIRED_tot_ar_km2"
    else:
        raise KeyError("Expected FIRED area column not found (FIRED_daily_ar_km2 or FIRED_tot_ar_km2).")

    joined_data["area_diff_km2"] = (joined_data[fired_area_col] - joined_data["nirops_km2"]).abs()
    joined_data["date_diff"] = (
        (joined_data["NIROPS_perim_date"] - joined_data["FIRED_perim_date"]).dt.days.abs()
    )
    joined_data["perc_diff"] = (
        (joined_data[fired_area_col] - joined_data["nirops_km2"]).abs() / joined_data["nirops_km2"]
    ) * 100

    print(f"Duplicate FIRED IDs: {joined_data['FIRED_id'].duplicated().sum()}")
    print(f"Duplicate NIROPS Incident_C values: {joined_data['NIROPS_Incident_C'].duplicated().sum()}")

    print("\nArea difference (km2) summary:")
    print(joined_data["area_diff_km2"].describe())

    print("\nDate difference summary:")
    print(joined_data["date_diff"].describe())

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    joined_data["area_diff_km2"].plot.hist(ax=axes[0], bins=100)
    axes[0].set_title("Area difference (km2)")
    joined_data["date_diff"].plot.hist(ax=axes[1], bins=10)
    axes[1].set_title("Date difference (days)")
    plt.tight_layout()
    #plt.show()

    joined_filtered = joined_data.copy()
    '''joined_filtered = joined_data[
        (joined_data["date_diff"] <= 25)
        & ((joined_data["area_diff_km2"] < 202.34) | (joined_data["perc_diff"] < 50))
    ].copy()

    joined_filtered = (
        joined_filtered.sort_values("perc_diff")
        .drop_duplicates(subset=["FIRED_id"], keep="first")
        .copy()
    )'''

    print(f"Duplicate FIRED IDs: {joined_filtered['FIRED_id'].duplicated().sum()}")
    print(
        f"Duplicate NIROPS Incident_C values: "
        f"{joined_filtered['NIROPS_Incident_C'].duplicated().sum()}"
    )
    print(f"Final joined features: {len(joined_filtered)}")

    print("\nFiltered area difference (km2) summary:")
    print(joined_filtered["area_diff_km2"].describe())

    print("\nFiltered date difference summary:")
    print(joined_filtered["date_diff"].describe())

    return joined_filtered

aoi, aoi_buffered = load_aoi(AOI_NAME, PROJECT_CRS, AOI_BUFFER_M)
fired_daily, nirops = prepare_perimeters(aoi_buffered)

joined_data = run_join(fired_daily, nirops)
#joined_filtered = summarize_and_filter(joined_data)
joined_filtered = joined_data.drop(columns=["FIRED_orig_geometry"])
# NIROPS layer
joined_filtered.drop(columns="FIRED_geometry").to_file(OUTPUT_PATH, layer="nirops", driver="GPKG")
# FIRED layer
joined_filtered.set_geometry("FIRED_geometry").drop(columns="geometry").to_file(OUTPUT_PATH, layer="fired", driver="GPKG")
print(f"Saved NIROPS and FIRED layers to: {OUTPUT_PATH}")
