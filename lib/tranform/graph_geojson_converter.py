import os

import geopandas as gpd
import pandas as pd
import partridge as ptg
from opendataproduct.config.data_transformation_gold_loader import DataTransformation
from opendataproduct.tracking_decorator import TrackingDecorator
from shapely.geometry import LineString

# Mapping standard GTFS route_types to names
route_type_map = {
    # 0: "tram",
    # 1: "subway",
    # 2: "rail",
    # 3: "bus",
    # 4: "ferry",
    # 5: "cable_tram",
    # 6: "aerial_lift",
    # 7: "funicular",
    # 11: "trolleybus",
    # 12: "monorail",
    100: "regional-train",
    106: "regional-train",
    109: "s-bahn",
    400: "u-bahn",
    700: "bus",
    900: "tram",
    1000: "ferry",
}


@TrackingDecorator.track_time
def convert_transit_feed(
    data_transformation: DataTransformation,
    source_path,
    results_path,
    debug=False,
    clean=False,
    quiet=False,
):
    if data_transformation.input_ports:
        for input_port in data_transformation.input_ports:
            for file in input_port.files:
                source_file_path = os.path.join(
                    source_path, input_port.id, file.source_file_name
                )

                #
                # Load and filter GTFS data
                #

                feed = ptg.load_feed(source_file_path)

                routes = feed.routes
                trips = feed.trips
                stops = feed.stops
                stop_times = feed.stop_times
                shapes = feed.shapes

                # Map route_type code to string
                routes["mode_name"] = (
                    routes["route_type"].map(route_type_map).fillna("other")
                )

                # Get unique modes
                unique_modes = routes["mode_name"].unique()

                # Iterative over modes
                for mode in unique_modes:
                    target_file_path = os.path.join(
                        results_path,
                        input_port.id,
                        file.target_file_name.replace(".geojson", f"-{mode}.geojson"),
                    )

                    # Check if result needs to be generated
                    if clean or not os.path.exists(target_file_path):
                        # Filter routes by mode
                        mode_routes = routes[routes["mode_name"] == mode]
                        mode_route_ids = mode_routes["route_id"].tolist()

                        # Filter trips that use these routes
                        mode_trips = trips[trips["route_id"].isin(mode_route_ids)]
                        mode_trip_ids = mode_trips["trip_id"].tolist()

                        if len(mode_trips) == 0:
                            continue

                        #
                        # Process shapes
                        #

                        mode_shapes_gdf = gpd.GeoDataFrame()

                        # Filter shapes used by these trips
                        mode_shape_ids = mode_trips["shape_id"].unique()

                        if not shapes.empty:
                            # Only keep shapes for this mode
                            relevant_shapes = shapes[
                                shapes["shape_id"].isin(mode_shape_ids)
                            ]

                            if not relevant_shapes.empty:
                                relevant_shapes = relevant_shapes.sort_values(
                                    by=["shape_id", "shape_pt_sequence"]
                                )
                                lines = relevant_shapes.groupby("shape_id")[
                                    ["shape_pt_lon", "shape_pt_lat"]
                                ].apply(
                                    lambda x: LineString(
                                        list(zip(x.shape_pt_lon, x.shape_pt_lat))
                                    )
                                )
                                mode_shapes_gdf = gpd.GeoDataFrame(
                                    lines, columns=["geometry"], crs="EPSG:4326"
                                )
                                mode_shapes_gdf = mode_shapes_gdf.reset_index()
                                mode_shapes_gdf.rename(
                                    columns={mode_shapes_gdf.columns[0]: "shape_id"},
                                    inplace=True,
                                )

                                # Add metadata
                                dataframe_merged = mode_trips.merge(
                                    mode_routes, on="route_id"
                                )
                                desired_cols = [
                                    "shape_id",
                                    "route_short_name",
                                    "route_color",
                                ]
                                valid_cols = [
                                    c
                                    for c in desired_cols
                                    if c in dataframe_merged.columns
                                ]
                                meta = dataframe_merged[valid_cols]
                                meta = meta.drop_duplicates(subset=["shape_id"])

                                mode_shapes_gdf = mode_shapes_gdf.merge(
                                    meta, on="shape_id", how="left"
                                )

                                # Fix color format
                                def fix_color(c):
                                    return (
                                        f"#{c}"
                                        if pd.notnull(c) and not str(c).startswith("#")
                                        else c
                                    )

                                if "route_color" in mode_shapes_gdf.columns:
                                    mode_shapes_gdf["route_color"] = mode_shapes_gdf[
                                        "route_color"
                                    ].apply(fix_color)

                                mode_shapes_gdf["feature_type"] = "route"

                        #
                        # Process stops
                        #

                        relevant_stop_times = stop_times[
                            stop_times["trip_id"].isin(mode_trip_ids)
                        ]
                        relevant_stop_ids = relevant_stop_times["stop_id"].unique()

                        mode_stops = stops[stops["stop_id"].isin(relevant_stop_ids)]

                        mode_stops_gdf = gpd.GeoDataFrame(
                            mode_stops,
                            geometry=gpd.points_from_xy(
                                mode_stops.stop_lon, mode_stops.stop_lat
                            ),
                            crs="EPSG:4326",
                        )
                        mode_stops_gdf["feature_type"] = "stop"

                        # Cleanup columns
                        keep_cols = ["stop_id", "stop_name", "feature_type", "geometry"]
                        mode_stops_gdf = mode_stops_gdf[keep_cols]

                        unified_gdf = pd.concat(
                            [mode_stops_gdf, mode_shapes_gdf], ignore_index=True
                        )
                        unified_gdf = unified_gdf.fillna("")

                        if debug:
                            save_dataframe_as_geojson(
                                mode_stops_gdf,
                                target_file_path.replace(".geojson", "-stops.geojson"),
                            )
                            save_dataframe_as_geojson(
                                mode_shapes_gdf,
                                target_file_path.replace(".geojson", "-lines.geojson"),
                            )

                        save_dataframe_as_geojson(unified_gdf, target_file_path)

                        not quiet and print(
                            f"✓ Convert {os.path.basename(source_file_path)} to {os.path.basename(target_file_path)}"
                        )


def save_dataframe_as_geojson(gdf: pd.DataFrame, geojson_file_path):
    # Make results path
    os.makedirs(os.path.dirname(geojson_file_path), exist_ok=True)

    # Save as geojson
    gdf.to_file(geojson_file_path, driver="GeoJSON")
