# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "click>=8.2.1",
#     "open-data-product-python-lib",
# ]
#
# [tool.uv.sources]
# open-data-product-python-lib = { git = "https://github.com/open-data-product/open-data-product-python-lib.git" }
# ///

import os
import sys

import click
from opendataproduct.config.data_product_manifest_loader import (
    load_data_product_manifest,
)
from opendataproduct.config.data_transformation_gold_loader import (
    load_data_transformation_gold,
)
from opendataproduct.config.data_transformation_silver_loader import (
    load_data_transformation_silver,
)
from opendataproduct.config.dpds_loader import load_dpds
from opendataproduct.config.odps_loader import load_odps
from opendataproduct.document.data_product_canvas_generator import (
    generate_data_product_canvas,
)
from opendataproduct.document.data_product_manifest_updater import (
    update_data_product_manifest,
)
from opendataproduct.document.dpds_canvas_generator import generate_dpds_canvas
from opendataproduct.document.dpds_updater import update_dpds
from opendataproduct.document.jupyter_notebook_creator import (
    create_jupyter_notebook_for_csv,
)
from opendataproduct.document.odps_canvas_generator import generate_odps_canvas
from opendataproduct.document.odps_updater import update_odps
from opendataproduct.extract.data_extractor import extract_data
from opendataproduct.transform.data_aggregator import aggregate_data
from opendataproduct.transform.data_copier import copy_data
from opendataproduct.transform.data_csv_converter import convert_data_to_csv

file_path = os.path.realpath(__file__)
script_path = os.path.dirname(file_path)


@click.command()
@click.option("--clean", "-c", default=False, is_flag=True, help="Regenerate results.")
@click.option("--quiet", "-q", default=False, is_flag=True, help="Do not log outputs.")
def main(clean, quiet):
    data_path = os.path.join(script_path, "data")
    bronze_path = os.path.join(data_path, "01-bronze")
    silver_path = os.path.join(data_path, "02-silver")
    gold_path = os.path.join(data_path, "03-gold")
    docs_path = os.path.join(script_path, "docs")

    data_product_manifest = load_data_product_manifest(config_path=script_path)
    data_transformation_silver = load_data_transformation_silver(
        config_path=script_path
    )
    data_transformation_gold = load_data_transformation_gold(config_path=script_path)
    odps = load_odps(config_path=script_path)
    dpds = load_dpds(config_path=script_path)

    #
    # Bronze: Integrate
    #

    extract_data(
        data_product_manifest=data_product_manifest,
        results_path=bronze_path,
        clean=clean,
        quiet=quiet,
    )

    #
    # Silver: Transform
    #

    copy_data(
        data_transformation=data_transformation_silver,
        source_path=bronze_path,
        results_path=silver_path,
        clean=clean,
        quiet=quiet,
    )

    convert_data_to_csv(
        data_transformation=data_transformation_silver,
        source_path=silver_path,
        results_path=silver_path,
        clean=clean,
        quiet=quiet,
    )

    #
    # Gold: Aggregate
    #

    aggregate_data(
        data_transformation=data_transformation_gold,
        source_path=silver_path,
        results_path=gold_path,
        clean=clean,
        quiet=quiet,
    )

    #
    # Documentation
    #

    create_jupyter_notebook_for_csv(
        data_product_manifest=data_product_manifest,
        results_path=script_path,
        data_path=gold_path,
        clean=True,
        quiet=quiet,
    )

    update_data_product_manifest(
        data_product_manifest=data_product_manifest,
        config_path=script_path,
        data_paths=[gold_path],
        file_endings=(".csv", ".parquet"),
    )

    update_odps(
        data_product_manifest=data_product_manifest,
        odps=odps,
        config_path=script_path,
    )

    update_dpds(
        data_product_manifest=data_product_manifest,
        dpds=dpds,
        config_path=script_path,
    )

    generate_data_product_canvas(
        data_product_manifest=data_product_manifest,
        docs_path=docs_path,
    )

    generate_odps_canvas(
        odps=odps,
        docs_path=docs_path,
    )

    generate_dpds_canvas(
        dpds=dpds,
        docs_path=docs_path,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
