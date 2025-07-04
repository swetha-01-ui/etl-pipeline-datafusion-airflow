from faker import Faker
import pandas as pd
import random
from google.cloud import storage
import os

# ---------- Step 1: Generate Dummy Data ----------

fake = Faker()
num_employees = 100

def generate_employee_data(n):
    data = []
    for _ in range(n):
        salary = round(random.uniform(40000, 150000), 2)
        employee = {
            "EmployeeID": fake.unique.random_int(min=1000, max=9999),
            "FirstName": fake.first_name(),
            "LastName": fake.last_name(),
            "Email": fake.email(),
            "PhoneNumber": fake.phone_number(),
            "Address": fake.address().replace('\n', ', '),
            "DOB": fake.date_of_birth(minimum_age=18, maximum_age=65).strftime("%Y-%m-%d"),
            "SSN": fake.ssn(),
            "JobTitle": fake.job(),
            "Department": fake.random_element(elements=("HR", "Finance", "Engineering", "Sales", "IT")),
            "StartDate": fake.date_between(start_date='-10y', end_date='today').strftime("%Y-%m-%d"),
            "Salary": salary
        }
        data.append(employee)
    return data

# File path and name
file_name = "dummy_employee_data.csv"

employee_data = generate_employee_data(num_employees)
df = pd.DataFrame(employee_data)
df.to_csv(file_name, index=False)

print(f"{file_name} created.")

# ---------- Step 2: Upload to GCS ----------

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to GCS bucket"""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/airflow/gcs/data/decoded-pier-464104-j0-32a3bcae3176.json"
    # 🔍 Add these debug prints here
    print("Credential path:", os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    print("Local file exists:", os.path.isfile(source_file_name))
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}")

# Set these:
GCS_BUCKET_NAME = "employee-data01"
GCS_DESTINATION_BLOB = "employee_data/" + file_name  # destination path in bucket

upload_to_gcs(GCS_BUCKET_NAME, file_name, GCS_DESTINATION_BLOB)

