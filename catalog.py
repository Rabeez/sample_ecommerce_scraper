import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv
from selectolax.parser import HTMLParser
from tqdm import tqdm

load_dotenv()

URL_ROOT = os.getenv("URL_ROOT")
assert URL_ROOT
_PARTIAL_TEMPLATE = os.getenv("URL_SUFFIX_TEMPLATE")
assert _PARTIAL_TEMPLATE
URL_TEMPLATE = URL_ROOT + _PARTIAL_TEMPLATE
URL_FULL_LINE = URL_ROOT + "{suffix}\n"

MAX_PAGES = 147
OUTPUT_FILE = Path("data/catalog.txt")


def main() -> None:
    OUTPUT_FILE.unlink(missing_ok=True)

    for page in tqdm(range(1, MAX_PAGES + 1), desc="Scraping pages"):
        with httpx.Client() as client:
            response = client.get(URL_TEMPLATE.format(page=page))
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                tqdm.write(f"Failed to fetch {page}: {e}", file=sys.stderr)
                break

        tree = HTMLParser(response.text)
        ul_element = tree.css_first(".product-list > ul:nth-child(1)")
        if not ul_element:
            raise ValueError("UL element not found")

        for li in ul_element.css("li"):
            a_tag = li.css_first("a")
            if a_tag and "href" in a_tag.attributes:
                href = a_tag.attributes["href"]
                if not href:
                    tqdm.write(f"No `href` on <a> tag {page}", file=sys.stderr)
                    continue
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    f.write(URL_FULL_LINE.format(suffix=href))
            else:
                tqdm.write(f"No <a> tag or href in <li> on page {page}", file=sys.stderr)


if __name__ == "__main__":
    main()
