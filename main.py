import os
import zipfile
from google.cloud import storage
import pandas as pd
import functions_framework

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    temp_dir = "/tmp/etl_output"
    try:
        data = cloud_event.data

        event_id = cloud_event["id"]
        event_type = cloud_event["type"]

        bucket = data["bucket"]
        name = data["name"]
        print(f"Processing file: {name} in bucket: {bucket}")

        # List of required input files
        file_paths = [
            "customers.xlsx",
            "products.xlsx",
            "orders.xlsx",
            "order_details.xlsx",
        ]

        # Create a storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket)

        # Read each required file into a DataFrame
        dataframes = {}
        for file in file_paths:
            blob = bucket.blob(file)
            if not blob.exists():
                raise FileNotFoundError(f"File {file} not found in bucket {bucket}.")
            data = blob.download_as_string()
            dataframes[file] = pd.read_excel(data, engine="openpyxl")

        # Extract individual DataFrames
        customers_df = dataframes["customers.xlsx"]
        products_df = dataframes["products.xlsx"]
        orders_df = dataframes["orders.xlsx"]
        order_details_df = dataframes["order_details.xlsx"]


        # Transform: Join orders and order details
        merged_df = orders_df.merge(order_details_df, on="OrderID", how="inner")

        # Add aggregate attributes to the merged table
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

        # Merge aggregates into the merged table
        merged_with_aggregates = merged_df.merge(customer_agg, on="CustomerID", how="left")
        merged_with_aggregates = merged_with_aggregates.merge(product_agg, on="ProductID", how="left")

        # Select only required columns for Order Fact Table
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

        unique_dates = pd.concat([orders_df["OrderDate"], orders_df["ShipDate"]]).dropna().unique()
        unique_dates = pd.to_datetime(unique_dates)  # Ensure all dates are in datetime format
        unique_dates = pd.DataFrame({"Date": sorted(unique_dates)})
        # Create Time Dimension Table
        time_dim = unique_dates
        time_dim["TimeID"] = time_dim["Date"].dt.strftime("%Y%m%d")
        time_dim["Year"] = time_dim["Date"].dt.year
        time_dim["Month"] = time_dim["Date"].dt.month
        time_dim["Day"] = time_dim["Date"].dt.day
        time_dim["WeekDay"] = time_dim["Date"].dt.day_name()

        # Save DataFrames as CSV
        output_files = {
            "Customer_Dimension": customers_df,
            "Product_Dimension": products_df,
            "Order_Fact": order_fact_table,
            "Time_Dimension": time_dim,
        }

        # Create a temporary directory
        os.makedirs(temp_dir, exist_ok=True)

        for table_name, df in output_files.items():
            output_path = os.path.join(temp_dir, f"{table_name}.csv")
            df.to_csv(output_path, index=False)
            print(f"Saved {table_name} to {output_path}")

        # Create a ZIP file
        zip_file_path = os.path.join(temp_dir, "etl_output.zip")
        with zipfile.ZipFile(zip_file_path, "w") as zipf:
            for file_name in os.listdir(temp_dir):
                file_path = os.path.join(temp_dir, file_name)
                if file_name.endswith(".csv"):  # Only include CSV files
                    zipf.write(file_path, arcname=file_name)

        print(f"Created ZIP file at {zip_file_path}")

        # Upload the ZIP file back to the GCS bucket
        output_blob_name = "etl_output.zip"
        output_blob = bucket.blob(output_blob_name)
        output_blob.upload_from_filename(zip_file_path)

        print(f"Uploaded ZIP file to GCS bucket: {bucket_name} as {output_blob_name}")

    except Exception as e:
        print(f"Error during ETL process: {e}")

    finally:
        # Clean up the temporary directory
        try:
            if os.path.exists(temp_dir):
                for file_name in os.listdir(temp_dir):
                    os.remove(os.path.join(temp_dir, file_name))
                os.rmdir(temp_dir)
            print("Cleaned up temporary directory.")
        except Exception as cleanup_error:
            print(f"Error during cleanup: {cleanup_error}")
