import argparse
import html
import sys
from pathlib import Path

import httpx
import polars as pl
from selectolax.parser import HTMLParser
from tqdm import tqdm

CATALOG_PATH = Path("data/catalog.txt")
OUTPUT_FILE = Path("data/items.parquet")


def grab_text(t: HTMLParser, css_selector: str) -> str:
    content = t.css_first(css_selector)
    if not content:
        raise ValueError

    return html.unescape(content.text(deep=False)).strip()


def text_to_dict(t: HTMLParser, html_text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    extras: list[str] = []

    p_tags = t.css("p")
    if p_tags:
        for p in p_tags:
            text = p.text(deep=True).strip()
            if ":" in text:
                key, value = text.split(":", 1)
                key = key.strip()
                value = value.strip()
                result[key] = value
            else:
                extras.append(text)
    else:
        lines = [line.strip() for line in html_text.split("<br>") if line.strip()]
        for line in lines:
            if ":" in line:
                key, value = line.split(":", 1)
                key = key.strip()
                value = value.strip()
                result[key] = value
            else:
                extras.append(line)

    result["extras"] = ";".join(extras)
    return result


def main(num: int | None) -> None:
    OUTPUT_FILE.unlink(missing_ok=True)

    with open(CATALOG_PATH, "r") as f:
        lines = [line.strip() for line in f]

    products = []
    end = len(lines) if num is None else num
    for i, url in tqdm(enumerate(lines), total=end, desc="Items"):
        with httpx.Client() as client:
            response = client.get(url)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                tqdm.write(f"Failed to fetch {url}: {e}", file=sys.stderr)
                break

        tree = HTMLParser(response.text)

        try:
            title = grab_text(tree, ".pro-con-right > h2:nth-child(1)")
        except ValueError:
            raise ValueError(f"Item title not found: {url}") from None

        try:
            category = grab_text(tree, ".location > div:nth-child(1)")
        except ValueError:
            raise ValueError(f"Item category not found: {url}") from None

        try:
            details_text = grab_text(tree, ".canshu > div:nth-child(2)")
        except ValueError:
            raise ValueError(f"Item details not found: {url}") from None
        details = text_to_dict(tree, details_text)

        products.append(
            {
                "url": url,
                "title": title,
                "category": category,
                "details": details,
            },
        )

        if num is not None and i >= num:
            break

    print("Saving parquet file")
    main_df = pl.DataFrame(products)
    main_df.write_parquet(OUTPUT_FILE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape product details from URLs",
    )
    parser.add_argument(
        "--num",
        type=int,
        default=None,
        help="Maximum number of URLs to process (default: None - process all URLs)",
    )
    args = parser.parse_args()
    main(args.num)
