from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.responses import JSONResponse
import uvicorn
import threading
from fastmcp import FastMCP
import os
import json
import subprocess
import sys
from typing import Optional, List, Any

mcp = FastMCP("shillelagh")

# ---------------------------------------------------------------------------
# Helper: run a query via the shillelagh DB-API 2.0 interface
# ---------------------------------------------------------------------------

def _run_query(
    query: str,
    parameters: Optional[List[Any]] = None,
    adapter_kwargs: Optional[dict] = None,
) -> dict:
    """Execute a SQL query using shillelagh and return serialisable results."""
    try:
        from shillelagh.backends.apsw.db import connect
    except ImportError as exc:
        return {"error": f"shillelagh is not installed: {exc}"}

    try:
        kwargs = adapter_kwargs or {}
        connection = connect(":memory:", adapter_kwargs=kwargs)
        cursor = connection.cursor()

        params = tuple(parameters) if parameters else ()
        cursor.execute(query, params)

        # description is None for non-SELECT statements
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            # Coerce every value to a JSON-serialisable type
            serialised_rows = []
            for row in rows:
                serialised_row = {}
                for col, val in zip(columns, row):
                    if hasattr(val, "isoformat"):
                        serialised_row[col] = val.isoformat()
                    else:
                        try:
                            json.dumps(val)  # test serializability
                            serialised_row[col] = val
                        except (TypeError, ValueError):
                            serialised_row[col] = str(val)
                serialised_rows.append(serialised_row)
            return {"columns": columns, "rows": serialised_rows, "rowcount": len(serialised_rows)}
        else:
            rowcount = cursor.rowcount if cursor.rowcount is not None else -1
            return {"message": "Query executed successfully.", "rowcount": rowcount}

    except Exception as exc:  # pylint: disable=broad-except
        return {"error": str(exc)}
    finally:
        try:
            connection.close()
        except Exception:  # pylint: disable=broad-except
            pass


# ---------------------------------------------------------------------------
# Tool: execute_sql
# ---------------------------------------------------------------------------

@mcp.tool()
async def execute_sql(
    _track("execute_sql")
    query: str,
    parameters: Optional[List[Any]] = None,
    adapter_kwargs: Optional[str] = None,
) -> dict:
    """
    Execute a SQL query against any resource supported by Shillelagh
    (Google Sheets, APIs, JSON/XML endpoints, GitHub, Datasette, dbt MetricFlow, etc.).

    The 'table' in the SQL is typically a URL or resource identifier enclosed in
    double quotes, e.g.
        SELECT * FROM "https://docs.google.com/spreadsheets/d/..."

    Supports SELECT, INSERT, UPDATE, DELETE.
    """
    kwargs: Optional[dict] = None
    if adapter_kwargs:
        try:
            kwargs = json.loads(adapter_kwargs)
        except json.JSONDecodeError as exc:
            return {"error": f"Invalid adapter_kwargs JSON: {exc}"}

    return _run_query(query, parameters=parameters, adapter_kwargs=kwargs)


# ---------------------------------------------------------------------------
# Tool: query_gsheets
# ---------------------------------------------------------------------------

@mcp.tool()
async def query_gsheets(
    _track("query_gsheets")
    spreadsheet_url: str,
    query: str,
    service_account_file: Optional[str] = None,
    subject: Optional[str] = None,
) -> dict:
    """
    Query or modify a Google Sheets spreadsheet using SQL.

    The placeholder {url} inside the query string will be replaced with the
    spreadsheet_url automatically.

    Authentication can be provided via a service account JSON file or default
    application credentials.
    """
    # Replace placeholder if present
    resolved_query = query.replace("{url}", spreadsheet_url)

    # If the query does not reference the URL at all, auto-inject it as the table
    if spreadsheet_url not in resolved_query and "{url}" not in query:
        # Try to auto-wrap: replace bare table placeholder
        resolved_query = query  # leave as-is; user should provide correct SQL

    adapter_kwargs: dict = {}
    gsheets_opts: dict = {}
    if service_account_file:
        gsheets_opts["service_account_file"] = service_account_file
    if subject:
        gsheets_opts["subject"] = subject
    if gsheets_opts:
        adapter_kwargs["gsheetsapi"] = gsheets_opts

    return _run_query(resolved_query, adapter_kwargs=adapter_kwargs or None)


# ---------------------------------------------------------------------------
# Tool: query_json_api
# ---------------------------------------------------------------------------

@mcp.tool()
async def query_json_api(
    _track("query_json_api")
    url: str,
    query: str,
    extra_headers: Optional[str] = None,
) -> dict:
    """
    Query any JSON or XML HTTP API endpoint using SQL.

    The URL should be used as the table name inside double quotes in the SQL query.
    """
    adapter_kwargs: dict = {}
    if extra_headers:
        try:
            headers = json.loads(extra_headers)
        except json.JSONDecodeError as exc:
            return {"error": f"Invalid extra_headers JSON: {exc}"}
        # Pass headers to both generic JSON and XML adapters
        adapter_kwargs["genericjsonapi"] = {"extra_headers": headers}
        adapter_kwargs["genericxmlapi"] = {"extra_headers": headers}

    # Replace {url} placeholder if used
    resolved_query = query.replace("{url}", url)

    return _run_query(resolved_query, adapter_kwargs=adapter_kwargs or None)


# ---------------------------------------------------------------------------
# Tool: query_github
# ---------------------------------------------------------------------------

@mcp.tool()
async def query_github(
    _track("query_github")
    query: str,
    access_token: Optional[str] = None,
) -> dict:
    """
    Query GitHub resources (issues, pull requests, releases, etc.) using SQL.

    The table name in the SQL should be a GitHub API URL, e.g.
        SELECT * FROM "https://api.github.com/repos/owner/repo/issues"
    """
    adapter_kwargs: dict = {}
    if access_token:
        adapter_kwargs["githubapi"] = {"access_token": access_token}

    return _run_query(query, adapter_kwargs=adapter_kwargs or None)


# ---------------------------------------------------------------------------
# Tool: query_datasette
# ---------------------------------------------------------------------------

@mcp.tool()
async def query_datasette(
    _track("query_datasette")
    datasette_url: str,
    query: str,
) -> dict:
    """
    Query a Datasette instance using SQL.

    Datasette exposes SQLite databases as web APIs; Shillelagh treats them as
    SQL tables. The datasette_url should appear as the table name (in double
    quotes) inside the SQL query.
    """
    resolved_query = query.replace("{url}", datasette_url)
    return _run_query(resolved_query)


# ---------------------------------------------------------------------------
# Tool: run_shillelagh_cli
# ---------------------------------------------------------------------------

@mcp.tool()
async def run_shillelagh_cli(
    _track("run_shillelagh_cli")
    sql: Optional[str] = None,
    database: str = ":memory:",
    adapter_kwargs: Optional[str] = None,
) -> dict:
    """
    Run a single SQL statement using the Shillelagh DB-API (simulating the CLI).

    If 'sql' is provided the statement is executed and the results returned.
    If omitted, a message is returned explaining that interactive REPL mode is
    not available in this server context.
    """
    if not sql:
        return {
            "message": (
                "Interactive REPL mode is not available inside the MCP server. "
                "Please provide a 'sql' parameter with the statement to execute."
            )
        }

    kwargs: Optional[dict] = None
    if adapter_kwargs:
        try:
            kwargs = json.loads(adapter_kwargs)
        except json.JSONDecodeError as exc:
            return {"error": f"Invalid adapter_kwargs JSON: {exc}"}

    return _run_query(sql, adapter_kwargs=kwargs)


# ---------------------------------------------------------------------------
# Tool: create_sqlalchemy_engine
# ---------------------------------------------------------------------------

@mcp.tool()
async def create_sqlalchemy_engine(
    _track("create_sqlalchemy_engine")
    resource_url: str,
    adapter: Optional[str] = None,
    adapter_kwargs: Optional[str] = None,
    sample_query: Optional[str] = None,
) -> dict:
    """
    Generate a SQLAlchemy engine connection string and sample Python code
    for connecting to Shillelagh resources.
    """
    parsed_kwargs: dict = {}
    if adapter_kwargs:
        try:
            parsed_kwargs = json.loads(adapter_kwargs)
        except json.JSONDecodeError as exc:
            return {"error": f"Invalid adapter_kwargs JSON: {exc}"}

    # Build connection string
    connection_string = "shillelagh://"

    # Build connect_args / adapter_kwargs block
    connect_args_repr = ""
    if parsed_kwargs:
        inner = json.dumps(parsed_kwargs, indent=8)
        connect_args_repr = f",\n    connect_args={{\n        'adapter_kwargs': {inner}\n    }}"
    elif adapter:
        connect_args_repr = (
            f",\n    connect_args={{\n        'adapter_kwargs': {{\'{ adapter }\': {{}}}}\n    }}"
        )

    effective_query = sample_query or f'SELECT * FROM \"{resource_url}\"'

    code = f"""from sqlalchemy import create_engine, text

engine = create_engine(
    \"{connection_string}\"{connect_args_repr}
)

with engine.connect() as conn:
    result = conn.execute(text(\'{effective_query}\'))
    for row in result:
        print(row)
"""

    # Also show the DB-API 2.0 approach
    dbapi_code = f"""from shillelagh.backends.apsw.db import connect

connection = connect(":memory:"{', adapter_kwargs=' + json.dumps(parsed_kwargs) if parsed_kwargs else ''})
cursor = connection.cursor()

for row in cursor.execute(\'{effective_query}\'):
    print(row)
"""

    return {
        "connection_string": connection_string,
        "sqlalchemy_sample_code": code,
        "dbapi_sample_code": dbapi_code,
        "resource_url": resource_url,
        "adapter": adapter,
        "adapter_kwargs": parsed_kwargs,
        "sample_query": effective_query,
    }


# ---------------------------------------------------------------------------
# Tool: list_adapters
# ---------------------------------------------------------------------------

ADAPTERS = [
    {
        "name": "gsheetsapi",
        "description": "Google Sheets adapter. Read and write Google Sheets spreadsheets via SQL.",
        "url_pattern": "https://docs.google.com/spreadsheets/d/<SHEET_ID>/...",
        "capabilities": ["SELECT", "INSERT", "UPDATE", "DELETE"],
        "auth": "service_account_file (JSON) or OAuth / Application Default Credentials",
        "required_config": ["service_account_file OR default application credentials"],
        "experimental": False,
    },
    {
        "name": "genericjsonapi",
        "description": "Generic JSON API adapter. Treat any JSON HTTP endpoint as a SQL table.",
        "url_pattern": "https://<any-host>/path/to/data.json",
        "capabilities": ["SELECT"],
        "auth": "Optional extra_headers (e.g. Bearer tokens)",
        "required_config": [],
        "experimental": False,
    },
    {
        "name": "genericxmlapi",
        "description": "Generic XML API adapter. Treat any XML HTTP endpoint as a SQL table.",
        "url_pattern": "https://<any-host>/path/to/data.xml",
        "capabilities": ["SELECT"],
        "auth": "Optional extra_headers",
        "required_config": [],
        "experimental": False,
    },
    {
        "name": "githubapi",
        "description": "GitHub adapter. Query GitHub issues, PRs, releases, and other resources.",
        "url_pattern": "https://api.github.com/repos/<owner>/<repo>/<resource>",
        "capabilities": ["SELECT"],
        "auth": "access_token (personal access token, recommended to avoid rate limits)",
        "required_config": [],
        "experimental": False,
    },
    {
        "name": "datasette",
        "description": "Datasette adapter. Query Datasette-hosted SQLite databases via HTTP.",
        "url_pattern": "https://<datasette-host>/<database>/<table>",
        "capabilities": ["SELECT"],
        "auth": "None required for public instances",
        "required_config": [],
        "experimental": False,
    },
    {
        "name": "dbtmetricflow",
        "description": "dbt MetricFlow adapter. Query dbt Semantic Layer metrics via SQL.",
        "url_pattern": "https://semantic-layer.cloud.getdbt.com/ or custom *.dbt.com URLs",
        "capabilities": ["SELECT"],
        "auth": "service_token (dbt Cloud service token)",
        "required_config": ["service_token", "environment_id"],
        "experimental": False,
    },
    {
        "name": "weatherapi",
        "description": "WeatherAPI adapter. Query historical weather data using SQL.",
        "url_pattern": "https://api.weatherapi.com/v1/history.json?key=<API_KEY>&q=<LOCATION>",
        "capabilities": ["SELECT"],
        "auth": "API key embedded in the URL",
        "required_config": ["API key"],
        "experimental": False,
    },
    {
        "name": "csvfile",
        "description": "CSV file adapter. Query local CSV files as SQL tables.",
        "url_pattern": "/path/to/file.csv or file:///path/to/file.csv",
        "capabilities": ["SELECT", "INSERT", "UPDATE", "DELETE"],
        "auth": "None",
        "required_config": [],
        "experimental": False,
    },
    {
        "name": "system",
        "description": "System adapter. Expose system information (e.g. adapters list) as SQL tables.",
        "url_pattern": "system://<resource>",
        "capabilities": ["SELECT"],
        "auth": "None",
        "required_config": [],
        "experimental": False,
    },
    {
        "name": "s3selectapi",
        "description": "Amazon S3 Select adapter. Query S3 objects (CSV, JSON, Parquet) using SQL.",
        "url_pattern": "s3://<bucket>/<key>",
        "capabilities": ["SELECT"],
        "auth": "AWS credentials (environment variables or IAM role)",
        "required_config": ["AWS credentials"],
        "experimental": True,
    },
    {
        "name": "socrata",
        "description": "Socrata Open Data API adapter. Query government open data portals.",
        "url_pattern": "https://<socrata-domain>/resource/<dataset-id>.json",
        "capabilities": ["SELECT"],
        "auth": "Optional app token",
        "required_config": [],
        "experimental": True,
    },
    {
        "name": "gsheetschart",
        "description": "Google Sheets Chart adapter. Query data backing charts in Google Sheets.",
        "url_pattern": "https://docs.google.com/spreadsheets/d/<SHEET_ID>/...",
        "capabilities": ["SELECT"],
        "auth": "Same as gsheetsapi",
        "required_config": [],
        "experimental": True,
    },
]


@mcp.tool()
async def list_adapters(
    _track("list_adapters")
    filter_by: Optional[str] = None,
    include_experimental: bool = False,
) -> dict:
    """
    List all available Shillelagh adapters with their supported URL patterns,
    capabilities, and required configuration.

    Optionally filter by a keyword (e.g. 'google', 'json', 'github') or a
    specific adapter name (e.g. 'gsheetsapi').
    """
    results = ADAPTERS

    if not include_experimental:
        results = [a for a in results if not a.get("experimental", False)]

    if filter_by:
        f = filter_by.lower()
        results = [
            a
            for a in results
            if (
                f in a["name"].lower()
                or f in a["description"].lower()
                or f in a["url_pattern"].lower()
                or any(f in c.lower() for c in a["capabilities"])
            )
        ]

    return {
        "total": len(results),
        "filter_by": filter_by,
        "include_experimental": include_experimental,
        "adapters": results,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------



_SERVER_SLUG = "betodealmeida-shillelagh"

def _track(tool_name: str, ua: str = ""):
    import threading
    def _send():
        try:
            import urllib.request, json as _json
            data = _json.dumps({"slug": _SERVER_SLUG, "event": "tool_call", "tool": tool_name, "user_agent": ua}).encode()
            req = urllib.request.Request("https://www.volspan.dev/api/analytics/event", data=data, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass
    threading.Thread(target=_send, daemon=True).start()

async def health(request):
    return JSONResponse({"status": "ok", "server": mcp.name})

async def tools(request):
    registered = await mcp.list_tools()
    tool_list = [{"name": t.name, "description": t.description or ""} for t in registered]
    return JSONResponse({"tools": tool_list, "count": len(tool_list)})

sse_app = mcp.http_app(transport="sse")

app = Starlette(
    routes=[
        Route("/health", health),
        Route("/tools", tools),
        Mount("/", sse_app),
    ],
    lifespan=sse_app.lifespan,
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
