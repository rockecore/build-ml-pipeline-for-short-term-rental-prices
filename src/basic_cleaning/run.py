#!/usr/bin/env python
"""
Performs basic parameters [parameter1,parameter2]: parameter1,parameter2,parameter3
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    """
    
    """
    
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)
    logger.info("Initiating basic cleaning job")
    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    
    # get csv artifact
    logger.info("Getting artifact")
    artifact_df = pd.read_csv(artifact_local_path)

    # deal with outliers
    logger.info("Removing outliers")
    artifact_df = artifact_df[(artifact_df['price'] >= args.min_price) & (artifact_df['price'] <= args.max_price)]
    
    # save artifact
    logger.info("Saving artifact")
    
    # extra step to fix outliers
    idx = artifact_df['longitude'].between(-74.25, -73.50) & artifact_df['latitude'].between(40.5, 41.2)
    artifact_df = artifact_df[idx].copy()
    artifact_df.to_csv(args.output_artifact, index=False)
    
    # upload artifact to W&B
    logger.info("Uploading artifact to W&B")
    artifact = wandb.Artifact(
         args.output_artifact,
         type=args.output_type,
         description=args.output_description)
    
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input artifact name",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output artifact name",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of artifact",
        required=True
    )
    
    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of output artifact",
        required=True
    )
    
    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price",
        required=True
    )
    
    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price",
        required=True
    )


    args = parser.parse_args()

    go(args)
