from pathlib import Path

import polars as pl

INPUT_FILE = Path("data/items.parquet")
OUTPUT_FILE = Path("data/items.csv")


def main() -> None:
    OUTPUT_FILE.unlink(missing_ok=True)

    main_df = pl.read_parquet(INPUT_FILE)
    main_df = main_df.drop("image_urls")

    unique_keys = set()
    for row in main_df["details"]:
        if row:  # Check if struct is not null
            unique_keys.update(row.keys())
    unique_keys = sorted(unique_keys)
    print(unique_keys)

    unpack_exprs = [pl.col("details").struct.field(key).alias(key) for key in unique_keys]
    df_unpacked = main_df.select(
        pl.col("id"),
        pl.col("url"),
        pl.col("title"),
        pl.col("category"),
        *unpack_exprs,
    )

    df_unpacked.write_csv(OUTPUT_FILE)


if __name__ == "__main__":
    main()
