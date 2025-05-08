# Ecommerce web scraper

## Setup

Ensure `.env` file exists in project root following this pattern:

```shell
URL_ROOT="https://www.example.com"
URL_SUFFIX_TEMPLATE="/list-{page}.html"
```

The page `URL_ROOT` is the root of all URLs in the scraping activity. The `URL_SUFFIX_TEMPLATE` will be concatenated with root and filled-in with page number.

## Usage

Execute these commands *in order*:

```shell
pixi run catalog
pixi run items --num 100
pixi run process
```

1. `catalog` fetches item URLs for a predefined number of pages
1. `items` fetches item-level details for specified number of pages (if `--nums` is omitted, all items are processed). Resulting DataFrame is stored as a parquet file.
1. `process` converts the nested `polars` DataFrame into a flat CSV file
