import json
from io import StringIO

import boto3
import pandas as pd


def extract_fund_data_from_s3() -> pd.DataFrame:
    """Simulate extracting fund data stored in an S3 object."""
    # In production, this would read an object from S3 with boto3 S3 client.
    sample_csv = """fund_id,currency,starting_nav,ending_nav,capital_flows
FUND001,USD,1000000,1125000,50000
FUND002,EUR,750000,765000,-10000
FUND003,GBP,500000,590000,25000
"""
    return pd.read_csv(StringIO(sample_csv))


def calculate_nav_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate NAV return and performance metrics for each fund."""
    # Time-weighted style return approximation using starting NAV and net capital flows.
    df = df.copy()
    df["net_asset_value_change"] = df["ending_nav"] - df["starting_nav"]
    df["return_pct"] = (
        (df["ending_nav"] - df["capital_flows"] - df["starting_nav"])
        / df["starting_nav"]
        * 100
    ).round(2)
    return df


def load_results_to_dynamodb(df: pd.DataFrame, table_name: str) -> None:
    """Simulate loading calculated NAV results into a DynamoDB table."""
    # In production, this would use table.put_item for each record.
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _ = dynamodb.Table(table_name)  # Placeholder to reflect target table usage.

    print(f"Simulating load into DynamoDB table: {table_name}")
    for record in df.to_dict(orient="records"):
        print(json.dumps(record))


def main() -> None:
    """Run the end-to-end fund reporting simulation."""
    # Step 1: Simulate extraction from S3.
    raw_data = extract_fund_data_from_s3()

    # Step 2: Calculate NAV performance metrics.
    nav_results = calculate_nav_metrics(raw_data)

    # Step 3: Simulate loading results to DynamoDB.
    load_results_to_dynamodb(nav_results, table_name="FundFlowNavResults")


if __name__ == "__main__":
    main()
