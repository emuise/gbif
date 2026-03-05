import polars as pl
import os

dataset = pl.scan_parquet(os.path.join("data", "gbif_hive"))


dataset.collect()
