
from base import spark
import logging
import os

logging.basicConfig(level=logging.INFO)

base_path = "/app/Datasets/"
delta_base = "/app/delta_lake/bronze/"

data = {
    'customers': 'olist_customers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'order_reviews': 'olist_order_reviews_dataset.csv',
    'orders': 'olist_orders_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'product_category_name_translation': 'product_category_name_translation.csv',
}

def extract_data(file_name: str, save_name: str):
    full_path = base_path + file_name
    save_path = delta_base + save_name
    try:
        logging.info(f"Reading: {full_path}")
        df = spark.read.csv(full_path, header=True, inferSchema=True)
        row_count = df.count()
        logging.info(f"Rows read: {row_count}")

        df.write.format("delta").mode("overwrite").save(save_path)
        logging.info(f"Saved to Delta Lake: {save_path}")
        return df

    except Exception as e:
        logging.error(f"Failed on {file_name}: {e}")
        raise


if __name__ == "__main__":
    for name, file in data.items():
        logging.info(f"--- Processing: {name} ---")
        extract_data(file, name)
        logging.info(f"Done: {name}")