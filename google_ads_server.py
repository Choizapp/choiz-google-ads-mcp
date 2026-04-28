from typing import Any, Dict, List, Optional, Union
from pydantic import Field
import os
import json
import requests
from datetime import datetime, timedelta
from pathlib import Path

import logging

# MCP
from mcp.server.fastmcp import FastMCP

# Google Ads library (uses gRPC — works reliably vs REST which returns 500)
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('google_ads_server')

mcp = FastMCP(
    "google-ads-server",
    dependencies=[
        "google-ads",
        "google-auth-oauthlib",
        "google-auth",
        "requests",
        "python-dotenv"
    ]
)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("Environment variables loaded from .env file")
except ImportError:
    logger.warning("python-dotenv not installed, skipping .env file loading")

# Config from environment
GOOGLE_ADS_CREDENTIALS_PATH = os.environ.get("GOOGLE_ADS_CREDENTIALS_PATH")
GOOGLE_ADS_DEVELOPER_TOKEN   = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")


def format_customer_id(customer_id: str) -> str:
    customer_id = str(customer_id).replace('"', '').replace("'", "")
    return ''.join(c for c in customer_id if c.isdigit())


def _load_oauth_creds() -> dict:
    """Load OAuth credentials.

    Production (containerized): read from GOOGLE_ADS_OAUTH_CLIENT_ID /
    GOOGLE_ADS_OAUTH_CLIENT_SECRET / GOOGLE_ADS_OAUTH_REFRESH_TOKEN env vars.
    Local dev: fall back to the credentials.json file at GOOGLE_ADS_CREDENTIALS_PATH.
    """
    cid = os.environ.get("GOOGLE_ADS_OAUTH_CLIENT_ID")
    csec = os.environ.get("GOOGLE_ADS_OAUTH_CLIENT_SECRET")
    crt = os.environ.get("GOOGLE_ADS_OAUTH_REFRESH_TOKEN")
    if cid and csec and crt:
        return {"client_id": cid, "client_secret": csec, "refresh_token": crt}

    if not GOOGLE_ADS_CREDENTIALS_PATH:
        raise ValueError(
            "Provide OAuth credentials via either GOOGLE_ADS_OAUTH_CLIENT_ID/SECRET/REFRESH_TOKEN "
            "env vars (preferred) or GOOGLE_ADS_CREDENTIALS_PATH pointing to a credentials.json file."
        )
    if not os.path.exists(GOOGLE_ADS_CREDENTIALS_PATH):
        raise FileNotFoundError(f"credentials.json not found at {GOOGLE_ADS_CREDENTIALS_PATH}. Run setup_oauth.py first.")

    with open(GOOGLE_ADS_CREDENTIALS_PATH) as f:
        return json.load(f)


# Module-level cache for the GoogleAdsClient. Constructing one is expensive
# (it opens gRPC channels to googleads.googleapis.com and pulls a large
# protobuf descriptor pool into memory). Re-creating per tool call leaks
# ~50–90 MB of resident memory per call under the supergateway+FastMCP
# stateless pattern, which on the gateway eventually trips the cgroup
# mem_limit and forces a child-process restart. Caching keeps a single
# client + channel for the life of the process.
#
# Safe because GOOGLE_ADS_LOGIN_CUSTOMER_ID is configured once at startup
# and login_customer_id is a property of the *client*, while the per-request
# customer_id is passed to GoogleAdsService.search() separately. If a use
# case ever needs different login_customer_ids in the same process, switch
# to a dict cache keyed on login_id.
_ADS_CLIENT: Optional[GoogleAdsClient] = None


def get_ads_client(customer_id: str = None) -> GoogleAdsClient:
    global _ADS_CLIENT
    if _ADS_CLIENT is not None:
        return _ADS_CLIENT

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("GOOGLE_ADS_DEVELOPER_TOKEN environment variable not set")

    creds = _load_oauth_creds()

    config = {
        "client_id":       creds["client_id"],
        "client_secret":   creds["client_secret"],
        "refresh_token":   creds["refresh_token"],
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "use_proto_plus":  True,
    }

    login_id = GOOGLE_ADS_LOGIN_CUSTOMER_ID or customer_id
    if login_id:
        config["login_customer_id"] = format_customer_id(login_id)

    _ADS_CLIENT = GoogleAdsClient.load_from_dict(config)
    return _ADS_CLIENT


def run_query(customer_id: str, query: str) -> list:
    client = get_ads_client(customer_id)
    ga_service = client.get_service("GoogleAdsService")
    response = ga_service.search(customer_id=format_customer_id(customer_id), query=query)
    return list(response)


def proto_to_dict(obj) -> dict:
    """Convert a proto-plus GoogleAdsRow to a plain dict, preserving zero-value fields."""
    from google.protobuf.json_format import MessageToDict
    try:
        return MessageToDict(
            obj._pb,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
    except Exception:
        pass
    # Fallback: to_json returns a JSON string — parse it back to dict
    try:
        raw = type(obj).to_json(obj)
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, dict):
            return raw
    except Exception:
        pass
    return {}


def flatten_dict(d, parent=""):
    items = {}
    for k, v in d.items():
        key = f"{parent}.{k}" if parent else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, key))
        else:
            items[key] = v
    return items


@mcp.tool()
async def list_accounts() -> str:
    """
    Lists all accessible Google Ads accounts.
    Run this first to find available account IDs for subsequent queries.
    """
    try:
        client = get_ads_client()
        customer_service = client.get_service("CustomerService")
        response = customer_service.list_accessible_customers()
        lines = ["Accessible Google Ads Accounts:", "-" * 50]
        for r in response.resource_names:
            lines.append(f"Account ID: {r.split('/')[-1]}")
        return "\n".join(lines)
    except GoogleAdsException as ex:
        return "Google Ads API error:\n" + "\n".join(str(e.message) for e in ex.failure.errors)
    except Exception as e:
        return f"Error listing accounts: {str(e)}"


@mcp.tool()
async def run_gaql(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'"),
    query: str = Field(description="Valid GAQL query string"),
    format: str = Field(default="csv", description="Output format: 'csv' (default, compact), 'json', or 'table' (padded ASCII — verbose, use sparingly)")
) -> str:
    """
    Execute any GAQL (Google Ads Query Language) query with custom formatting.

    EXAMPLE QUERIES:
    1. Campaign metrics last 7 days:
       SELECT campaign.name, metrics.clicks, metrics.impressions, metrics.cost_micros
       FROM campaign WHERE segments.date DURING LAST_7_DAYS

    2. Ad group performance:
       SELECT ad_group.name, metrics.conversions, metrics.cost_micros, campaign.name
       FROM ad_group WHERE metrics.clicks > 100

    Note: Cost values are in micros (1,000,000 = 1 unit of currency).
    """
    # Default 'csv' (and not 'table') because the ASCII-padded table inflates
    # rows ~30x with whitespace; combined with the claude.ai ~2-3 KB tool-result
    # ceiling, 'table' on multi-row results hits the wall. Caller can still
    # request 'table' explicitly.
    fmt = format if isinstance(format, str) else "csv"
    try:
        rows = run_query(customer_id, query)
        cid = format_customer_id(customer_id)
        if not rows:
            return "No results found for the query."

        dicts = [proto_to_dict(r) for r in rows]

        if fmt.lower() == "json":
            return json.dumps(dicts, ensure_ascii=False)

        flat_rows = [flatten_dict(d) for d in dicts]
        # Drop rows that are completely empty (shouldn't happen after proto fix, but defensive)
        flat_rows = [r for r in flat_rows if r]
        if not flat_rows:
            return f"Query returned {len(rows)} rows but all fields were empty. Try format='json' for raw output."

        all_keys = list(dict.fromkeys(k for r in flat_rows for k in r))

        if fmt.lower() == "csv":
            lines = [",".join(all_keys)]
            for row in flat_rows:
                lines.append(",".join(str(row.get(k, "")).replace(",", ";") for k in all_keys))
            return "\n".join(lines)

        widths = {k: max(len(k), max(len(str(r.get(k, ""))) for r in flat_rows)) for k in all_keys}
        header = " | ".join(f"{k:{widths[k]}}" for k in all_keys)
        sep = "-" * len(header)
        lines = [f"Query Results for Account {cid}:", sep, header, sep]
        for row in flat_rows:
            lines.append(" | ".join(f"{str(row.get(k, '')):{widths[k]}}" for k in all_keys))
        return "\n".join(lines)

    except GoogleAdsException as ex:
        return "Google Ads API error:\n" + "\n".join(str(e.message) for e in ex.failure.errors)
    except Exception as e:
        return f"Error executing GAQL query: {str(e)}"


@mcp.tool()
async def execute_gaql_query(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'"),
    query: str = Field(description="Valid GAQL query string")
) -> str:
    """Execute a custom GAQL query. Returns JSON-formatted results."""
    try:
        rows = run_query(customer_id, query)
        if not rows:
            return "No results found for the query."
        dicts = [proto_to_dict(r) for r in rows]
        lines = [f"Query Results for Account {format_customer_id(customer_id)}:", "-" * 80]
        for d in dicts:
            lines.append(json.dumps(d, ensure_ascii=False))
        return "\n".join(lines)
    except GoogleAdsException as ex:
        return "Google Ads API error:\n" + "\n".join(str(e.message) for e in ex.failure.errors)
    except Exception as e:
        return f"Error executing GAQL query: {str(e)}"


@mcp.tool()
async def get_campaign_performance(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'"),
    days: int = Field(default=30, description="Number of days to look back (7, 14, 30, 90, etc.)"),
    limit: int = Field(default=10, description="Max campaigns to return. Default 10 keeps output under the ~2-3 KB claude.ai payload ceiling; raise carefully.")
) -> str:
    """Get campaign performance metrics. Cost values are in micros (1,000,000 = 1 unit of currency)."""
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.conversions, metrics.average_cpc
        FROM campaign
        WHERE segments.date DURING LAST_{days}_DAYS
        ORDER BY metrics.cost_micros DESC
        LIMIT {int(limit)}
    """
    return await run_gaql(customer_id, query, "csv")


@mcp.tool()
async def get_ad_performance(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'"),
    days: int = Field(default=30, description="Number of days to look back (7, 14, 30, 90, etc.)"),
    limit: int = Field(default=10, description="Max ads to return. Default 10 keeps output under the ~2-3 KB claude.ai payload ceiling.")
) -> str:
    """Get ad-level performance metrics. Cost values are in micros (1,000,000 = 1 unit of currency)."""
    query = f"""
        SELECT ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.status,
               campaign.name, ad_group.name,
               metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions
        FROM ad_group_ad
        WHERE segments.date DURING LAST_{days}_DAYS
        ORDER BY metrics.impressions DESC
        LIMIT {int(limit)}
    """
    return await run_gaql(customer_id, query, "csv")


@mcp.tool()
async def get_account_currency(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'")
) -> str:
    """Retrieve the currency code used by the account. Run this before analyzing cost data."""
    try:
        rows = run_query(customer_id, "SELECT customer.id, customer.currency_code, customer.descriptive_name FROM customer LIMIT 1")
        if not rows:
            return "No customer data found."
        r = rows[0]
        return f"Account {r.customer.id}: {r.customer.descriptive_name}\nCurrency: {r.customer.currency_code}"
    except GoogleAdsException as ex:
        return "Google Ads API error:\n" + "\n".join(str(e.message) for e in ex.failure.errors)
    except Exception as e:
        return f"Error retrieving account currency: {str(e)}"


@mcp.tool()
async def get_ad_creatives(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'")
) -> str:
    """Get ad creative details including headlines, descriptions, and URLs."""
    query = """
        SELECT ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type,
               ad_group_ad.ad.final_urls, ad_group_ad.status,
               ad_group_ad.ad.responsive_search_ad.headlines,
               ad_group_ad.ad.responsive_search_ad.descriptions,
               ad_group.name, campaign.name
        FROM ad_group_ad
        WHERE ad_group_ad.status != 'REMOVED'
        ORDER BY campaign.name, ad_group.name
        LIMIT 25
    """
    try:
        rows = run_query(customer_id, query)
        if not rows:
            return "No ad creatives found."

        cid = format_customer_id(customer_id)
        lines = [f"Ad Creatives for Account {cid}:", "=" * 80]

        for i, row in enumerate(rows, 1):
            ad = row.ad_group_ad.ad
            lines += [
                f"\n{i}. Campaign: {row.campaign.name}",
                f"   Ad Group: {row.ad_group.name}",
                f"   Ad ID: {ad.id}  |  Status: {row.ad_group_ad.status.name}  |  Type: {ad.type_.name}",
            ]
            rsa = ad.responsive_search_ad
            if rsa.headlines:
                lines.append("   Headlines: " + " / ".join(h.text for h in rsa.headlines[:3]))
            if rsa.descriptions:
                lines.append("   Descriptions: " + " / ".join(d.text for d in rsa.descriptions[:2]))
            if ad.final_urls:
                lines.append(f"   URL: {ad.final_urls[0]}")
            lines.append("-" * 80)

        return "\n".join(lines)

    except GoogleAdsException as ex:
        return "Google Ads API error:\n" + "\n".join(str(e.message) for e in ex.failure.errors)
    except Exception as e:
        return f"Error retrieving ad creatives: {str(e)}"


@mcp.tool()
async def get_image_assets(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'"),
    limit: int = Field(default=25, description="Maximum number of image assets to return")
) -> str:
    """Retrieve image assets in the account including their full-size URLs."""
    query = f"""
        SELECT asset.id, asset.name, asset.type,
               asset.image_asset.full_size.url,
               asset.image_asset.full_size.height_pixels,
               asset.image_asset.full_size.width_pixels,
               asset.image_asset.file_size
        FROM asset
        WHERE asset.type = 'IMAGE'
        LIMIT {limit}
    """
    try:
        rows = run_query(customer_id, query)
        if not rows:
            return "No image assets found."

        cid = format_customer_id(customer_id)
        lines = [f"Image Assets for Account {cid}:", "=" * 80]

        for i, row in enumerate(rows, 1):
            asset = row.asset
            fs = asset.image_asset.full_size
            lines += [
                f"\n{i}. Asset ID: {asset.id}  |  Name: {asset.name}",
                f"   Dimensions: {fs.width_pixels} x {fs.height_pixels} px",
            ]
            if asset.image_asset.file_size:
                lines.append(f"   File Size: {asset.image_asset.file_size / 1024:.2f} KB")
            if fs.url:
                lines.append(f"   URL: {fs.url}")
            lines.append("-" * 80)

        return "\n".join(lines)

    except GoogleAdsException as ex:
        return "Google Ads API error:\n" + "\n".join(str(e.message) for e in ex.failure.errors)
    except Exception as e:
        return f"Error retrieving image assets: {str(e)}"


@mcp.tool()
async def list_resources(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'")
) -> str:
    """List valid GAQL resource names (valid options for the FROM clause)."""
    query = """
        SELECT google_ads_field.name, google_ads_field.category, google_ads_field.data_type
        FROM google_ads_field
        WHERE google_ads_field.category = 'RESOURCE'
        ORDER BY google_ads_field.name
        LIMIT 200
    """
    return await run_gaql(customer_id, query, "csv")


@mcp.tool()
async def get_asset_usage(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'"),
    asset_id: str = Field(default=None, description="Optional: specific asset ID to look up"),
    asset_type: str = Field(default="IMAGE", description="Asset type: 'IMAGE', 'TEXT', 'VIDEO', etc.")
) -> str:
    """Find where specific assets are used across campaigns and ad groups."""
    where = f"asset.type = '{asset_type}'"
    if asset_id:
        where += f" AND asset.id = {asset_id}"
    query = f"""
        SELECT campaign.id, campaign.name, asset.id, asset.name, asset.type
        FROM campaign_asset
        WHERE {where}
        LIMIT 50
    """
    return await run_gaql(customer_id, query, "csv")


@mcp.tool()
async def analyze_image_assets(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'"),
    days: int = Field(default=30, description="Number of days to look back (7, 30, 90, etc.)")
) -> str:
    """Analyze image assets with performance metrics (impressions, clicks, conversions, cost)."""
    query = """
        SELECT asset.id, asset.name,
               asset.image_asset.full_size.url,
               asset.image_asset.full_size.width_pixels,
               asset.image_asset.full_size.height_pixels,
               campaign.name,
               metrics.impressions, metrics.clicks, metrics.conversions, metrics.cost_micros
        FROM campaign_asset
        WHERE asset.type = 'IMAGE' AND segments.date DURING LAST_30_DAYS
        ORDER BY metrics.impressions DESC
        LIMIT 50
    """
    try:
        rows = run_query(customer_id, query)
        if not rows:
            return "No image asset performance data found."

        cid = format_customer_id(customer_id)
        assets_data = {}

        for row in rows:
            aid = str(row.asset.id)
            fs  = row.asset.image_asset.full_size
            m   = row.metrics

            if aid not in assets_data:
                assets_data[aid] = {
                    "name": row.asset.name,
                    "url": fs.url or "N/A",
                    "dims": f"{fs.width_pixels} x {fs.height_pixels}",
                    "impressions": 0, "clicks": 0,
                    "conversions": 0.0, "cost_micros": 0,
                    "campaigns": set()
                }
            assets_data[aid]["impressions"]  += m.impressions
            assets_data[aid]["clicks"]       += m.clicks
            assets_data[aid]["conversions"]  += m.conversions
            assets_data[aid]["cost_micros"]  += m.cost_micros
            assets_data[aid]["campaigns"].add(row.campaign.name)

        lines = [f"Image Asset Performance — Account {cid} (Last {days} days):", "=" * 100]
        for aid, d in sorted(assets_data.items(), key=lambda x: x[1]["impressions"], reverse=True):
            ctr = (d["clicks"] / d["impressions"] * 100) if d["impressions"] > 0 else 0
            lines += [
                f"\nAsset ID: {aid}  |  {d['name']}  |  {d['dims']}",
                f"Impressions: {d['impressions']:,}  |  Clicks: {d['clicks']:,}  |  CTR: {ctr:.2f}%",
                f"Conversions: {d['conversions']:.2f}  |  Cost (micros): {d['cost_micros']:,}",
                f"Campaigns: {', '.join(list(d['campaigns'])[:5])}",
            ]
            if d["url"] != "N/A":
                lines.append(f"URL: {d['url']}")
            lines.append("-" * 100)

        return "\n".join(lines)

    except GoogleAdsException as ex:
        return "Google Ads API error:\n" + "\n".join(str(e.message) for e in ex.failure.errors)
    except Exception as e:
        return f"Error analyzing image assets: {str(e)}"


@mcp.tool()
async def download_image_asset(
    customer_id: str = Field(description="Google Ads customer ID (digits only). Example: '5916996729'"),
    asset_id: str = Field(description="The ID of the image asset to download"),
    output_dir: str = Field(default="./ad_images", description="Directory to save the downloaded image")
) -> str:
    """Download a specific image asset to local disk."""
    query = f"""
        SELECT asset.id, asset.name, asset.image_asset.full_size.url
        FROM asset WHERE asset.type = 'IMAGE' AND asset.id = {asset_id} LIMIT 1
    """
    try:
        rows = run_query(customer_id, query)
        if not rows:
            return f"No image asset found with ID {asset_id}"

        asset = rows[0].asset
        url   = asset.image_asset.full_size.url
        name  = asset.name or f"image_{asset_id}"

        if not url:
            return f"No download URL found for asset ID {asset_id}"

        base = Path.cwd()
        out  = Path(output_dir).resolve()
        try:
            out.relative_to(base)
        except ValueError:
            out = base / "ad_images"

        out.mkdir(parents=True, exist_ok=True)
        r = requests.get(url)
        if r.status_code != 200:
            return f"Failed to download: HTTP {r.status_code}"

        safe = ''.join(c for c in name if c.isalnum() or c in ' ._-')
        path = out / f"{asset_id}_{safe}.jpg"
        path.write_bytes(r.content)
        return f"Downloaded asset {asset_id} to {path}"

    except GoogleAdsException as ex:
        return "Google Ads API error:\n" + "\n".join(str(e.message) for e in ex.failure.errors)
    except Exception as e:
        return f"Error downloading image asset: {str(e)}"


# ---------------------------------------------------------------------------
# Resources & Prompts
# ---------------------------------------------------------------------------

@mcp.resource("gaql://reference")
def gaql_reference() -> str:
    """Google Ads Query Language (GAQL) quick reference."""
    return """
# GAQL Reference

SELECT field1, field2 FROM resource WHERE condition ORDER BY field LIMIT n

## Common resources
campaign, ad_group, ad_group_ad, keyword_view, campaign_asset, asset, customer

## Common metrics
metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions,
metrics.ctr, metrics.average_cpc, metrics.conversion_rate

## Date ranges
WHERE segments.date DURING LAST_7_DAYS
WHERE segments.date DURING LAST_30_DAYS
WHERE segments.date BETWEEN '2024-01-01' AND '2024-01-31'

## Note
Cost values are in micros: 1,000,000 = 1 unit of currency.
"""


@mcp.prompt("google_ads_workflow")
def google_ads_workflow() -> str:
    return """
Recommended workflow:
1. list_accounts() — find available account IDs
2. get_account_currency(customer_id="...") — check currency
3. get_campaign_performance(customer_id="...", days=30) — overview
4. run_gaql(customer_id="...", query="...") — custom queries (default csv)

Always pass customer_id as digits only: e.g. "5916996729"
"""


@mcp.prompt("gaql_help")
def gaql_help() -> str:
    return """
Example GAQL queries:

Campaign performance:
  SELECT campaign.name, metrics.impressions, metrics.clicks, metrics.cost_micros
  FROM campaign WHERE segments.date DURING LAST_30_DAYS ORDER BY metrics.cost_micros DESC

Keyword performance:
  SELECT keyword_view.resource_name, metrics.impressions, metrics.clicks
  FROM keyword_view WHERE segments.date DURING LAST_30_DAYS

Use with: run_gaql(customer_id="5916996729", query="...")  # default format='csv'
"""


if __name__ == "__main__":
    mcp.run(transport="stdio")
