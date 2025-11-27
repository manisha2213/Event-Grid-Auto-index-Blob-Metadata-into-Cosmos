import logging
import os
import time
from datetime import datetime

import azure.functions as func
from azure.cosmos import CosmosClient, exceptions as cosmos_exceptions
import pyodbc
from tenacity import retry, wait_exponential, stop_after_attempt


# ---------------------------
# ENVIRONMENT VARIABLES
# ---------------------------
COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
COSMOS_DB = os.getenv("COSMOS_DB")
COSMOS_CONTAINER = os.getenv("COSMOS_CONTAINER")

SQL_CONNECTION_STRING = os.getenv("SQL_CONNECTION_STRING")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
COSMOS_PAGE_SIZE = int(os.getenv("COSMOS_PAGE_SIZE", "100"))


# ---------------------------
# SQL QUERIES
# ---------------------------
INSERT_PRODUCT_SQL = """
INSERT INTO dbo.Products (Id, Name, Price, Category)
VALUES (?, ?, ?, ?)
"""

INSERT_TAG_SQL = """
INSERT INTO dbo.ProductTags (ProductId, Tag)
VALUES (?, ?)
"""


# ---------------------------
# HELPERS
# ---------------------------
def connect_sql():
    conn = pyodbc.connect(SQL_CONNECTION_STRING, autocommit=False)
    try:
        conn.fast_executemany = True
    except:
        logging.warning("fast_executemany not supported on this machine, continuing without it")
    return conn



def map_product(doc):
    return (
        str(doc.get("id")),
        doc.get("name"),
        doc.get("price"),
        doc.get("category")
    )


def map_tags(doc):
    tags = doc.get("tags", [])
    if not tags:
        return []
    pid = str(doc.get("id"))
    return [(pid, str(tag)) for tag in tags]


def stream_docs(container):
    query = "SELECT * FROM c"
    iterator = container.query_items(
        query=query,
        enable_cross_partition_query=True,
        max_item_count=COSMOS_PAGE_SIZE
    )
    for item in iterator:
        yield item


def is_throttled(ex):
    return (
        isinstance(ex, cosmos_exceptions.CosmosHttpResponseError)
        and getattr(ex, "status_code", None) == 429
    )


# Retry SQL errors
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(5))
def execute_batch(cursor, sql, values):
    cursor.executemany(sql, values)


# ---------------------------
# MAIN MIGRATION FUNCTION
# ---------------------------
async def main(req: func.HttpRequest) -> func.HttpResponse:
    start_time = datetime.utcnow()
    logging.info("=== Cosmos → SQL Migration Started ===")

    # Cosmos
    cosmos_client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
    database = cosmos_client.get_database_client(COSMOS_DB)
    container = database.get_container_client(COSMOS_CONTAINER)

    # SQL
    sql_conn = connect_sql()
    cursor = sql_conn.cursor()

    total_migrated = 0
    total_failed = 0
    batch_products = []
    batch_tags = []

    for doc in stream_docs(container):
        try:
            # Prepare rows
            batch_products.append(map_product(doc))
            batch_tags.extend(map_tags(doc))

            # Batch flush
            if len(batch_products) >= BATCH_SIZE:
                execute_batch(cursor, INSERT_PRODUCT_SQL, batch_products)
                if batch_tags:
                    execute_batch(cursor, INSERT_TAG_SQL, batch_tags)
                sql_conn.commit()

                logging.info(f"Committed {len(batch_products)} products")
                total_migrated += len(batch_products)

                batch_products.clear()
                batch_tags.clear()

        except cosmos_exceptions.CosmosHttpResponseError as cx:
            if is_throttled(cx):
                retry_ms = getattr(cx, "retry_after_in_milliseconds", 2000)
                sleep_time = retry_ms / 1000
                logging.warning(f"429 Throttle → sleeping {sleep_time}s")
                time.sleep(sleep_time)
                continue
            else:
                total_failed += 1
                logging.error(f"Cosmos error: {cx}")
                continue

        except Exception as ex:
            total_failed += 1
            logging.error(f"Migration error: {ex}")
            continue

    # Final flush
    if batch_products:
        try:
            execute_batch(cursor, INSERT_PRODUCT_SQL, batch_products)
            if batch_tags:
                execute_batch(cursor, INSERT_TAG_SQL, batch_tags)
            sql_conn.commit()

            total_migrated += len(batch_products)
        except Exception as ex:
            logging.error(f"Final batch failed: {ex}")
            total_failed += len(batch_products)

    # Close SQL
    cursor.close()
    sql_conn.close()

    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()

    # ---------------------------
    # MIGRATION REPORT
    # ---------------------------
    report = {
        "status": "Completed",
        "total_migrated": total_migrated,
        "total_failed": total_failed,
        "duration_seconds": duration,
        "started_at": str(start_time),
        "ended_at": str(end_time)
    }

    logging.info("=== Migration Finished ===")
    logging.info(report)

    return func.HttpResponse(
        str(report),
        status_code=200,
        mimetype="application/json"
    )
