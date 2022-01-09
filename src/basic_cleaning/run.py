#!/usr/bin/env python
"""
Performs basic cleaning on the data and saves the results in Weights & Biases parameters.
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    sample_data = pd.read_csv(artifact_local_path)

    idx = sample_data['price'].between(args.min_price, args.max_price)
    sample_data = sample_data[idx].copy()

    sample_data['last_review'] = pd.to_datetime(sample_data['last_review'])
    logger.info("last_review datetime format changed")

    idx = sample_data['longitude'].between(-74.25, -73.50) & sample_data['latitude'].between(40.5, 41.2)
    sample_data = sample_data[idx].copy()

    sample_data.to_csv(args.output_artifact, index=False)

    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
    )
    artifact.add_file(args.output_artifact)
    logger.info("Basic Cleaning: artifact.add_file successful")

    run.log_artifact(artifact)
    logger.info("Basic_Cleaning: run.log_artifact successful")
    artifact.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleans the data")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="sample.csv file",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="basic_cleaned_data.csv",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="output type",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="the minimum price",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="the maximum price",
        required=True
    )

    args = parser.parse_args()

    go(args)
