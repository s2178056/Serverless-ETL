This project provides a Google Cloud Function that performs an ETL (Extract, Transform, Load) process triggered by events in a Google Cloud Storage (GCS) bucket. The function processes Excel files stored in the bucket, performs transformations, and generates CSV files for dimensional tables. These files are then packaged into a ZIP archive and re-uploaded to the GCS bucket.
