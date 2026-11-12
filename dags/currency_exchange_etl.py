from airflow.sdk import dag, task
from pendulum import datetime

from datetime import datetime as python_datetime, timezone
import requests
import os
from pymongo import MongoClient


API_URL = "https://api.frankfurter.dev/v2/rates"


@dag(
    dag_id="currency_exchange_etl",
    start_date=datetime(2026, 11, 12, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "Astro",
        "retries": 2,
    },
    tags=["ETL", "Currency", "MongoDB"],
)
def currency_exchange_etl():

    # TASK 1: EXTRACT
    @task
    def extract_currency_rates(**context):
        response = requests.get(
            API_URL,
            params={
                "base": "EUR",
                "quotes": "USD,GBP,JPY",
            },
            timeout=30,
        )

        response.raise_for_status()
        raw_data = response.json()

        if not raw_data:
            raise ValueError("API returned no exchange-rate records")

        context["ti"].xcom_push(
            key="raw_exchange_rates",
            value=raw_data,
        )

        print(f"Extracted {len(raw_data)} exchange rates")

    # TASK 2: TRANSFORM
    @task
    def transform_exchange_rates(**context):
        raw_data = context["ti"].xcom_pull(
            task_ids="extract_currency_rates",
            key="raw_exchange_rates",
        )

        if not raw_data:
            raise ValueError("No raw exchange-rate data found in XCom")

        transformed_data = []

        retrieved_at = python_datetime.now(
            timezone.utc
        ).isoformat()

        for record in raw_data:
            transformed_record = {
                "base_currency": record["base"],
                "target_currency": record["quote"],
                "exchange_rate": float(record["rate"]),
                "rate_date": record["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }

            transformed_data.append(transformed_record)

        context["ti"].xcom_push(
            key="transformed_exchange_rates",
            value=transformed_data,
        )

        print(f"Transformed {len(transformed_data)} records")

    # TASK 3: LOAD
    @task
    def load_exchange_rates(**context):
        transformed_data = context["ti"].xcom_pull(
            task_ids="transform_exchange_rates",
            key="transformed_exchange_rates",
        )

        if not transformed_data:
            raise ValueError("No transformed data found in XCom")

        mongo_uri = os.getenv("MONGO_URI")
        database_name = os.getenv("MONGO_DATABASE")

        if not mongo_uri or not database_name:
            raise ValueError("MongoDB environment variables are missing")

        client = MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=5000,
        )

        try:
            # Verify MongoDB connection
            client.admin.command("ping")

            db = client[database_name]
            collection = db["exchange_rates"]

            result = collection.insert_many(transformed_data)

            print(f"Successfully inserted {len(result.inserted_ids)} documents")

        finally:
            client.close()

    # DEFINE TASK DEPENDENCIES
    extract_currency_rates() >> transform_exchange_rates() >> load_exchange_rates()


# Instantiate DAG
currency_exchange_etl()