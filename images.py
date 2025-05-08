import os
import shutil
import sys
from pathlib import Path

import httpx
import polars as pl
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

URL_ROOT = os.getenv("URL_ROOT")
assert URL_ROOT
ITEMS_FILE = Path("data/items.parquet")
IMAGES_FOLDER = Path("data/images/")


def download_image(url: str, save_path: Path) -> bool:
    try:
        with httpx.stream("GET", url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            response.raise_for_status()
            with open(save_path, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    f.write(chunk)
    except httpx.HTTPError as e:
        tqdm.write(f"Failed to download {url}: {e}", file=sys.stderr)
        return False
    else:
        return True


def main() -> None:
    for item in IMAGES_FOLDER.iterdir():
        if item.is_dir():
            shutil.rmtree(item)

    main_df = pl.read_parquet(ITEMS_FILE)
    for row in tqdm(main_df.iter_rows(named=True), total=len(main_df), desc="Items"):
        product_id = row["id"]
        dir_path = IMAGES_FOLDER / Path(product_id)
        dir_path.mkdir(exist_ok=True)
        if row["image_urls"]:
            for i, image_url in enumerate(row["image_urls"]):
                filename = f"{i + 1}.jpg"
                save_path = dir_path / Path(filename)
                res = download_image(image_url, save_path)
                if not res:
                    tqdm.write(f"missed image: {row['title']}")


if __name__ == "__main__":
    main()
