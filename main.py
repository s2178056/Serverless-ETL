import os
from google.cloud import storage, bigquery
import pandas as pd
import functions_framework

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    """
    Event-driven ETL function for processing Excel files in a Google Cloud Storage bucket
    and writing the results directly to BigQuery.
    """
    try:
        # Cloud Event data
        data = cloud_event.data
        bucket_name = data["bucket"]
        print(f"Processing bucket: {bucket_name}")

        # File paths for input files
        file_paths = [
            "customers.xlsx",
            "products.xlsx",
            "orders.xlsx",
            "order_details.xlsx",
        ]

        # Create a storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # Read input files into DataFrames
        dataframes = {}
        for file in file_paths:
            blob = bucket.blob(file)
            if not blob.exists():
                raise FileNotFoundError(f"File {file} not found in bucket {bucket_name}.")
            data = blob.download_as_string()
            dataframes[file] = pd.read_excel(data, engine="openpyxl")

        # Extract individual DataFrames
        customers_df = dataframes["customers.xlsx"]
        products_df = dataframes["products.xlsx"]
        orders_df = dataframes["orders.xlsx"]
        order_details_df = dataframes["order_details.xlsx"]

        # Transform: Join orders and order details
        merged_df = orders_df.merge(order_details_df, on="OrderID", how="inner")

        # Add aggregate attributes
        customer_agg = (
            merged_df.groupby("CustomerID")
            .agg(CustomerOrderCount=("OrderID", "count"), CustomerMeanDiscount=("Discount", "mean"))
            .reset_index()
        )
        product_agg = (
            merged_df.groupby("ProductID")
            .agg(ProductOrderCount=("OrderID", "count"), ProductTotalQuantity=("Quantity", "sum"))
            .reset_index()
        )

        # Merge aggregates
        merged_with_aggregates = merged_df.merge(customer_agg, on="CustomerID", how="left")
        merged_with_aggregates = merged_with_aggregates.merge(product_agg, on="ProductID", how="left")

        # Create Order Fact Table
        order_fact_table = merged_with_aggregates[
            [
                "OrderID",
                "CustomerID",
                "ProductID",
                "OrderDate",
                "ShipDate",
                "CustomerOrderCount",
                "CustomerMeanDiscount",
                "ProductOrderCount",
                "ProductTotalQuantity",
            ]
        ]

        # Create Time Dimension Table
        unique_dates = pd.concat([orders_df["OrderDate"], orders_df["ShipDate"]]).dropna().unique()
        unique_dates = pd.to_datetime(unique_dates)
        time_dim = pd.DataFrame({"Date": sorted(unique_dates)})
        # Add additional columns
        time_dim["TimeID"] = time_dim["Date"].dt.strftime("%Y%m%d")
        time_dim["Year"] = time_dim["Date"].dt.year
        time_dim["Month"] = time_dim["Date"].dt.month
        time_dim["Day"] = time_dim["Date"].dt.day
        time_dim["WeekDay"] = time_dim["Date"].dt.day_name()

        order_fact_table["OrderDate"] = order_fact_table["OrderDate"].astype(str)
        order_fact_table["ShipDate"] = order_fact_table["ShipDate"].astype(str)
        customers_df["SignupDate"] = customers_df["SignupDate"].astype(str)

        # Convert all columns to string
        time_dim = time_dim.astype(str)
            
        # Initialize BigQuery Client
        bq_client = bigquery.Client()

        # Define your dataset ID (replace with your actual dataset)
        dataset_id = "dark-caldron-441505-i4.data_mining"
        print("Access")

        # Define output DataFrames
        output_files = {
            "Customer_Dimension": customers_df,
            "Product_Dimension": products_df,
            "Order_Fact": order_fact_table,
            "Time_Dimension": time_dim,
        }

        # Write DataFrames to BigQuery
        for table_name, df in output_files.items():
            # Define full table ID
            table_id = f"{dataset_id}.{table_name}"

            # Write DataFrame to BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table
            )
            job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete

            print(f"Table {table_name} written to BigQuery: {table_id}")

    except Exception as e:
        print(f"Error during ETL process: {e}")

