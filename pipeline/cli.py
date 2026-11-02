"""Run pipeline steps without Airflow, e.g. `python -m pipeline.cli all 2026-06`."""

import argparse
import logging

from pipeline.config import load_settings, validate_month


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=["extract", "transform", "load", "all"])
    parser.add_argument("month", type=validate_month, help="YYYY-MM")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = load_settings()
    if args.step in ("extract", "all"):
        from pipeline.extract import extract_month

        print(extract_month(args.month, settings))
    if args.step in ("transform", "load", "all"):
        from pipeline.spark import get_spark

        spark = get_spark(settings)
        if args.step in ("transform", "all"):
            from pipeline.transform import transform_month

            print(transform_month(args.month, settings, spark))
        if args.step in ("load", "all"):
            from pipeline.load import load_trips, load_zones

            print(load_zones(settings), load_trips(args.month, settings, spark))
        spark.stop()


if __name__ == "__main__":
    main()
