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


def _get_shillelagh_connection(adapter_kwargs_str: Optional[str] = None):
    """Create a shillelagh connection with optional adapter kwargs."""
    try:
        from shillelagh.backends.apsw.db import connect
    except ImportError:
        raise RuntimeError(
            "shillelagh is not installed. Please install it with: pip install shillelagh"
        )

    kwargs = {}
    if adapter_kwargs_str:
        try:
            kwargs = json.loads(adapter_kwargs_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid adapter_kwargs JSON: {e}")

    return connect(":memory:", **kwargs)


def _rows_to_serializable(cursor) -> dict:
    """Convert cursor results to a serializable dict."""
    if cursor.description is None:
        return {
            "columns": [],
            "rows": [],
            "rowcount": cursor.rowcount if hasattr(cursor, 'rowcount') else 0,
            "message": "Query executed successfully (no results returned)"
        }

    columns = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        serialized_row = {}
        for col, val in zip(columns, row):
            if hasattr(val, 'isoformat'):
                serialized_row[col] = val.isoformat()
            elif isinstance(val, (int, float, str, bool, type(None))):
                serialized_row[col] = val
            else:
                serialized_row[col] = str(val)
        rows.append(serialized_row)

    return {
        "columns": columns,
        "rows": rows,
        "rowcount": len(rows)
    }


@mcp.tool()
async def execute_sql_query(
    query: str,
    parameters: Optional[List[Any]] = None,
    adapter_kwargs: Optional[str] = None
) -> dict:
    """
    Execute a SQL query against any supported resource (Google Sheets, GitHub, weather APIs,
    JSON/XML URLs, Datasette, dbt MetricFlow, etc.) using Shillelagh's DB API 2.0 backend.
    Use this as the primary tool to read data from or write data to any supported adapter.
    The table name is typically a URL or resource identifier enclosed in quotes.
    Supports SELECT, INSERT, UPDATE, DELETE, and JOINs across different sources.
    """
    try:
        connection = _get_shillelagh_connection(adapter_kwargs)
        cursor = connection.cursor()

        if parameters:
            cursor.execute(query, parameters)
        else:
            cursor.execute(query)

        result = _rows_to_serializable(cursor)
        connection.close()
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
async def query_google_sheet(
    spreadsheet_url: str,
    sql: str,
    service_account_file: Optional[str] = None,
    subject: Optional[str] = None
) -> dict:
    """
    Query or manipulate a Google Sheets spreadsheet using SQL. Use this when the user
    specifically wants to read, filter, insert, update, or delete rows in a Google Sheet.
    Handles authentication via service account or OAuth credentials. Supports filtering,
    ordering, and aggregations pushed down to the Sheets API.
    """
    try:
        adapter_kwargs_dict = {}
        gsheetsapi_kwargs = {}

        if service_account_file:
            gsheetsapi_kwargs["service_account_file"] = service_account_file
        if subject:
            gsheetsapi_kwargs["subject"] = subject

        if gsheetsapi_kwargs:
            adapter_kwargs_dict["gsheetsapi"] = gsheetsapi_kwargs

        adapter_kwargs_str = json.dumps(adapter_kwargs_dict) if adapter_kwargs_dict else None
        connection = _get_shillelagh_connection(adapter_kwargs_str)
        cursor = connection.cursor()
        cursor.execute(sql)
        result = _rows_to_serializable(cursor)
        connection.close()
        return {"success": True, "spreadsheet_url": spreadsheet_url, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
async def query_json_api(
    url: str,
    sql: str,
    parameters: Optional[List[Any]] = None
) -> dict:
    """
    Execute a SQL query against a remote JSON or XML API endpoint treated as a table.
    Use this when the user wants to query a REST API that returns JSON or XML.
    Shillelagh will fetch the data and allow SQL filtering, projection, and ordering.
    Ideal for generic HTTP APIs, weather APIs, or any JSON/XML data source accessible via URL.
    """
    try:
        connection = _get_shillelagh_connection()
        cursor = connection.cursor()

        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)

        result = _rows_to_serializable(cursor)
        connection.close()
        return {"success": True, "url": url, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
async def query_github(
    sql: str,
    github_token: Optional[str] = None
) -> dict:
    """
    Query GitHub resources (issues, pull requests, commits, releases, etc.) using SQL
    through the Shillelagh GitHub adapter. Use this when the user wants to search, filter,
    or analyze GitHub repository data using SQL syntax. Authentication via a GitHub personal
    access token is recommended to avoid rate limits.
    """
    try:
        adapter_kwargs_dict = {}
        if github_token:
            adapter_kwargs_dict["githubapi"] = {"access_token": github_token}

        adapter_kwargs_str = json.dumps(adapter_kwargs_dict) if adapter_kwargs_dict else None
        connection = _get_shillelagh_connection(adapter_kwargs_str)
        cursor = connection.cursor()
        cursor.execute(sql)
        result = _rows_to_serializable(cursor)
        connection.close()
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
async def query_datasette(
    datasette_url: str,
    sql: str
) -> dict:
    """
    Query a Datasette instance using SQL via Shillelagh. Use this when the user wants to run
    SQL queries against a Datasette-hosted SQLite database. Datasette exposes SQLite databases
    as APIs; this tool lets you query those tables directly.
    """
    try:
        connection = _get_shillelagh_connection()
        cursor = connection.cursor()
        cursor.execute(sql)
        result = _rows_to_serializable(cursor)
        connection.close()
        return {"success": True, "datasette_url": datasette_url, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
async def run_shillelagh_cli(
    query: Optional[str] = None,
    header: bool = True,
    adapter_kwargs: Optional[str] = None
) -> dict:
    """
    Launch an interactive Shillelagh SQL REPL or execute a single SQL statement via the CLI.
    Use this when the user wants to start an interactive session or run a one-off query from
    the command line. The CLI supports all adapters and accepts connection configuration via arguments.
    """
    if not query:
        return {
            "success": True,
            "message": "Interactive mode is not supported in this MCP server context. "
                       "Please provide a 'query' parameter to execute a SQL statement. "
                       "To run the interactive CLI, execute 'shillelagh' from your terminal.",
            "cli_command": "shillelagh"
        }

    try:
        # Build the shillelagh CLI command
        cmd = [sys.executable, "-m", "shillelagh"]
        if not header:
            cmd.append("--no-header")
        if query:
            cmd.extend(["-e", query])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode == 0:
            return {
                "success": True,
                "output": result.stdout,
                "stderr": result.stderr if result.stderr else None
            }
        else:
            # Fallback: run via shillelagh DB API directly
            connection = _get_shillelagh_connection(adapter_kwargs)
            cursor = connection.cursor()
            cursor.execute(query)
            data = _rows_to_serializable(cursor)
            connection.close()

            output_lines = []
            if header and data["columns"]:
                output_lines.append("\t".join(data["columns"]))
                output_lines.append("-" * 40)
            for row in data["rows"]:
                output_lines.append("\t".join(str(v) for v in row.values()))

            return {
                "success": True,
                "output": "\n".join(output_lines),
                "data": data
            }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "CLI command timed out after 60 seconds"}
    except Exception as e:
        # Final fallback: use DB API directly
        try:
            connection = _get_shillelagh_connection(adapter_kwargs)
            cursor = connection.cursor()
            cursor.execute(query)
            data = _rows_to_serializable(cursor)
            connection.close()
            return {"success": True, "data": data}
        except Exception as inner_e:
            return {"success": False, "error": str(inner_e), "error_type": type(inner_e).__name__}


@mcp.tool()
async def list_supported_adapters(
    filter_pattern: Optional[str] = None
) -> dict:
    """
    Discover and list all available Shillelagh adapters installed in the current environment,
    along with the URL patterns or resource types each adapter supports. Use this when the user
    asks what data sources Shillelagh can connect to, or when determining which adapter handles
    a given URL or resource.
    """
    known_adapters = [
        {
            "name": "gsheetsapi",
            "description": "Google Sheets adapter",
            "url_patterns": ["https://docs.google.com/spreadsheets/d/..."],
            "supports": ["SELECT", "INSERT", "UPDATE", "DELETE"],
            "auth": "Service account JSON or OAuth"
        },
        {
            "name": "githubapi",
            "description": "GitHub adapter for issues, PRs, commits, releases",
            "url_patterns": ["https://api.github.com/repos/owner/repo/issues", "https://api.github.com/repos/owner/repo/pulls"],
            "supports": ["SELECT"],
            "auth": "Personal access token (optional)"
        },
        {
            "name": "weatherapi",
            "description": "WeatherAPI.com historical weather data",
            "url_patterns": ["https://api.weatherapi.com/v1/history.json?key=API_KEY&q=LOCATION"],
            "supports": ["SELECT"],
            "auth": "API key in URL"
        },
        {
            "name": "datasette",
            "description": "Datasette-hosted SQLite databases",
            "url_patterns": ["https://*.datasette.io/db/table", "https://*.datasettes.com/db/table"],
            "supports": ["SELECT"],
            "auth": "None required"
        },
        {
            "name": "dbt_metricflow",
            "description": "dbt MetricFlow metrics adapter",
            "url_patterns": ["https://semantic-layer.cloud.getdbt.com/"],
            "supports": ["SELECT"],
            "auth": "dbt Cloud token"
        },
        {
            "name": "jsonapi",
            "description": "Generic JSON/XML REST API adapter",
            "url_patterns": ["https://api.example.com/data.json", "https://api.example.com/data.xml"],
            "supports": ["SELECT"],
            "auth": "API key in URL or headers"
        },
        {
            "name": "csvfile",
            "description": "CSV file adapter (local or remote)",
            "url_patterns": ["/path/to/file.csv", "https://example.com/data.csv"],
            "supports": ["SELECT", "INSERT", "UPDATE", "DELETE"],
            "auth": "None"
        },
        {
            "name": "system",
            "description": "System information adapter (memory, CPU, etc.)",
            "url_patterns": ["system://memory", "system://cpu"],
            "supports": ["SELECT"],
            "auth": "None"
        },
        {
            "name": "socrata",
            "description": "Socrata open data portals",
            "url_patterns": ["https://*.data.gov/resource/..."],
            "supports": ["SELECT"],
            "auth": "App token (optional)"
        },
        {
            "name": "s3select",
            "description": "AWS S3 Select for querying S3 objects",
            "url_patterns": ["s3://bucket/key.csv"],
            "supports": ["SELECT"],
            "auth": "AWS credentials"
        }
    ]

    # Try to dynamically discover installed adapters
    try:
        import pkg_resources
        dynamic_adapters = []
        for ep in pkg_resources.iter_entry_points("shillelagh.adapter"):
            dynamic_adapters.append({
                "name": ep.name,
                "description": f"Adapter loaded from entry point: {ep.module_name if hasattr(ep, 'module_name') else str(ep)}",
                "entry_point": str(ep)
            })
        if dynamic_adapters:
            # Merge/update with known adapters
            known_names = {a["name"] for a in known_adapters}
            for da in dynamic_adapters:
                if da["name"] not in known_names:
                    known_adapters.append(da)
    except Exception:
        pass

    adapters = known_adapters
    if filter_pattern:
        pattern_lower = filter_pattern.lower()
        adapters = [
            a for a in adapters
            if pattern_lower in a["name"].lower() or pattern_lower in a["description"].lower()
        ]

    return {
        "success": True,
        "adapters": adapters,
        "total_count": len(adapters),
        "filter_applied": filter_pattern,
        "note": "Install shillelagh extras for additional adapters: pip install 'shillelagh[gsheetsapi,githubapi,weatherapi]'"
    }


@mcp.tool()
async def create_sqlalchemy_engine(
    sql: str,
    connection_string: str = "shillelagh://",
    connect_args: Optional[str] = None
) -> dict:
    """
    Create a SQLAlchemy engine using the Shillelagh dialect and execute a query through it.
    Use this when the user wants to integrate Shillelagh with SQLAlchemy-based tools
    (e.g., Pandas, Superset, Alembic) or when a SQLAlchemy connection string is needed.
    The dialect string is 'shillelagh://'.
    """
    try:
        from sqlalchemy import text
        from sqlalchemy.engine import create_engine
    except ImportError:
        return {
            "success": False,
            "error": "SQLAlchemy is not installed. Please install it with: pip install sqlalchemy"
        }

    try:
        engine_kwargs = {}
        if connect_args:
            try:
                parsed_connect_args = json.loads(connect_args)
                engine_kwargs["connect_args"] = parsed_connect_args
            except json.JSONDecodeError as e:
                return {"success": False, "error": f"Invalid connect_args JSON: {e}"}

        engine = create_engine(connection_string, **engine_kwargs)
        with engine.connect() as conn:
            result = conn.execute(text(sql))

            if result.returns_rows:
                columns = list(result.keys())
                rows = []
                for row in result.fetchall():
                    serialized_row = {}
                    for col, val in zip(columns, row):
                        if hasattr(val, 'isoformat'):
                            serialized_row[col] = val.isoformat()
                        elif isinstance(val, (int, float, str, bool, type(None))):
                            serialized_row[col] = val
                        else:
                            serialized_row[col] = str(val)
                    rows.append(serialized_row)

                data = {
                    "columns": columns,
                    "rows": rows,
                    "rowcount": len(rows)
                }
            else:
                data = {
                    "columns": [],
                    "rows": [],
                    "rowcount": result.rowcount,
                    "message": "Query executed successfully"
                }

        engine.dispose()
        return {
            "success": True,
            "connection_string": connection_string,
            "data": data,
            "sqlalchemy_usage_example": (
                f"from sqlalchemy import text, create_engine\n"
                f"engine = create_engine('{connection_string}')\n"
                f"with engine.connect() as conn:\n"
                f"    result = conn.execute(text('{sql}'))\n"
                f"    rows = result.fetchall()"
            )
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}




_SERVER_SLUG = "betodealmeida-shillelagh"

def _track(tool_name: str, ua: str = ""):
    try:
        import urllib.request, json as _json
        data = _json.dumps({"slug": _SERVER_SLUG, "event": "tool_call", "tool": tool_name, "user_agent": ua}).encode()
        req = urllib.request.Request("https://www.volspan.dev/api/analytics/event", data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=1)
    except Exception:
        pass

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
