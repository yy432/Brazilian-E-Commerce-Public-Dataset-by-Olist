import pandas as pd
import kagglehub
from kagglehub import KaggleDatasetAdapter
from pathlib import Path

# Kaggle dataset
DATASET_SLUG = "olistbr/brazilian-ecommerce"

# All CSVs we want to download: (entity_name, kaggle_filename)
FILES = [
    ("olist_customers", "olist_customers_dataset.csv"),
    ("olist_geolocation", "olist_geolocation_dataset.csv"),
    ("olist_order_items", "olist_order_items_dataset.csv"),
    ("olist_order_payments", "olist_order_payments_dataset.csv"),
    ("olist_order_reviews", "olist_order_reviews_dataset.csv"),
    ("olist_orders", "olist_orders_dataset.csv"),
    ("olist_products", "olist_products_dataset.csv"),
    ("olist_sellers", "olist_sellers_dataset.csv"),
    ("product_category_name_translation",
     "product_category_name_translation.csv"),
]

DATA_DIR = Path("data")


def main():
    print("📥 Downloading from Kaggle using pandas...")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for entity, kaggle_file in FILES:
        print(f"\n=== Downloading {kaggle_file} for entity '{entity}' ===")

        df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            DATASET_SLUG,
            kaggle_file,
        )

        print("✅ Data shape:", df.shape)
        print(df.head())

        output_path = DATA_DIR / kaggle_file
        df.to_csv(output_path, index=False)

        print("💾 Saved to:", output_path.resolve())


if __name__ == "__main__":
    main()


