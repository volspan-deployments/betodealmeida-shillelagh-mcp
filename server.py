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


def _get_shillelagh_connection(adapter_kwargs: Optional[str] = None):
    """Create a shillelagh DB API 2.0 connection."""
    from shillelagh.backends.apsw.db import connect

    kwargs = {}
    if adapter_kwargs:
        try:
            kwargs = json.loads(adapter_kwargs)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid adapter_kwargs JSON: {e}")

    return connect(":memory:", adapter_kwargs=kwargs)


def _rows_to_serializable(cursor, rows):
    """Convert rows and cursor description to a serializable dict."""
    if cursor.description is None:
        return {"columns": [], "rows": [], "rowcount": cursor.rowcount}

    columns = [desc[0] for desc in cursor.description]
    result_rows = []
    for row in rows:
        serialized_row = {}
        for col, val in zip(columns, row):
            if hasattr(val, 'isoformat'):
                serialized_row[col] = val.isoformat()
            elif isinstance(val, (int, float, str, bool, type(None))):
                serialized_row[col] = val
            else:
                serialized_row[col] = str(val)
        result_rows.append(serialized_row)

    return {
        "columns": columns,
        "rows": result_rows,
        "rowcount": len(result_rows)
    }


@mcp.tool()
def execute_sql(
    query: str,
    parameters: Optional[List[Any]] = None,
    adapter_kwargs: Optional[str] = None
) -> dict:
    """
    Execute a SQL query against any supported resource (Google Sheets, GitHub, APIs, files,
    databases, JSON/XML URLs, etc.) using Shillelagh's DB API 2.0 interface.
    Table names are typically URLs or resource identifiers wrapped in quotes.
    """
    try:
        connection = _get_shillelagh_connection(adapter_kwargs)
        cursor = connection.cursor()

        if parameters:
            rows = list(cursor.execute(query, parameters))
        else:
            rows = list(cursor.execute(query))

        result = _rows_to_serializable(cursor, rows)
        connection.close()
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
def query_google_sheet(
    spreadsheet_url: str,
    query: str,
    service_account_file: Optional[str] = None,
    subject: Optional[str] = None
) -> dict:
    """
    Query or manipulate a Google Sheets spreadsheet using SQL.
    Supports SELECT, INSERT, UPDATE, DELETE.
    """
    try:
        from shillelagh.backends.apsw.db import connect

        gsheets_kwargs = {}
        if service_account_file:
            gsheets_kwargs["service_account_file"] = service_account_file
        if subject:
            gsheets_kwargs["subject"] = subject

        adapter_kwargs_dict = {}
        if gsheets_kwargs:
            adapter_kwargs_dict["gsheets"] = gsheets_kwargs

        connection = connect(":memory:", adapter_kwargs=adapter_kwargs_dict)
        cursor = connection.cursor()
        rows = list(cursor.execute(query))
        result = _rows_to_serializable(cursor, rows)
        connection.close()
        return {"success": True, "spreadsheet_url": spreadsheet_url, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
def query_api_url(
    url: str,
    query_filter: Optional[str] = None,
    columns: Optional[List[str]] = None,
    extra_params: Optional[str] = None
) -> dict:
    """
    Query a remote JSON or XML API endpoint as if it were a SQL table.
    Shillelagh auto-detects the format and exposes fields as columns.
    """
    try:
        from shillelagh.backends.apsw.db import connect

        # Build the URL with extra params if provided
        target_url = url
        if extra_params:
            try:
                params = json.loads(extra_params)
                if params:
                    separator = "&" if "?" in url else "?"
                    param_str = "&".join(f"{k}={v}" for k, v in params.items())
                    target_url = f"{url}{separator}{param_str}"
            except json.JSONDecodeError as e:
                return {"success": False, "error": f"Invalid extra_params JSON: {e}"}

        # Build SELECT clause
        if columns:
            select_clause = ", ".join(columns)
        else:
            select_clause = "*"

        # Build WHERE clause
        where_clause = f" WHERE {query_filter}" if query_filter else ""

        sql = f'SELECT {select_clause} FROM "{target_url}"{where_clause}'

        connection = connect(":memory:")
        cursor = connection.cursor()
        rows = list(cursor.execute(sql))
        result = _rows_to_serializable(cursor, rows)
        connection.close()
        return {"success": True, "url": target_url, "query": sql, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
def query_github(
    query: str,
    access_token: Optional[str] = None
) -> dict:
    """
    Query GitHub resources (issues, pull requests, repositories, etc.) using SQL
    via the Shillelagh GitHub adapter.
    """
    try:
        from shillelagh.backends.apsw.db import connect

        adapter_kwargs_dict = {}
        if access_token:
            adapter_kwargs_dict["github"] = {"access_token": access_token}

        connection = connect(":memory:", adapter_kwargs=adapter_kwargs_dict)
        cursor = connection.cursor()
        rows = list(cursor.execute(query))
        result = _rows_to_serializable(cursor, rows)
        connection.close()
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
def query_datasette(
    datasette_url: str,
    query: str
) -> dict:
    """
    Query a Datasette instance using SQL via Shillelagh.
    Access published datasets hosted on any Datasette server as SQL tables.
    """
    try:
        from shillelagh.backends.apsw.db import connect

        connection = connect(":memory:")
        cursor = connection.cursor()
        rows = list(cursor.execute(query))
        result = _rows_to_serializable(cursor, rows)
        connection.close()
        return {"success": True, "datasette_url": datasette_url, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
def run_cli_query(
    query: str,
    output_format: str = "table",
    config_file: Optional[str] = None
) -> dict:
    """
    Run a SQL query using the Shillelagh command-line interface.
    Supports output formats: 'table', 'csv', 'json'.
    """
    try:
        # Try using shillelagh programmatically to simulate CLI behavior
        from shillelagh.backends.apsw.db import connect
        import io
        import csv as csv_module

        connection = connect(":memory:")
        cursor = connection.cursor()
        rows = list(cursor.execute(query))

        if cursor.description is None:
            connection.close()
            return {
                "success": True,
                "output": f"Query executed. Rows affected: {cursor.rowcount}",
                "format": output_format
            }

        columns = [desc[0] for desc in cursor.description]

        if output_format == "json":
            result_rows = []
            for row in rows:
                serialized_row = {}
                for col, val in zip(columns, row):
                    if hasattr(val, 'isoformat'):
                        serialized_row[col] = val.isoformat()
                    else:
                        serialized_row[col] = val
                result_rows.append(serialized_row)
            output = json.dumps(result_rows, indent=2, default=str)

        elif output_format == "csv":
            buf = io.StringIO()
            writer = csv_module.writer(buf)
            writer.writerow(columns)
            for row in rows:
                serialized = []
                for val in row:
                    if hasattr(val, 'isoformat'):
                        serialized.append(val.isoformat())
                    else:
                        serialized.append(val)
                writer.writerow(serialized)
            output = buf.getvalue()

        else:  # table format
            col_widths = [len(c) for c in columns]
            str_rows = []
            for row in rows:
                str_row = []
                for val in row:
                    if hasattr(val, 'isoformat'):
                        s = val.isoformat()
                    else:
                        s = str(val) if val is not None else "NULL"
                    str_row.append(s)
                str_rows.append(str_row)
                for i, s in enumerate(str_row):
                    col_widths[i] = max(col_widths[i], len(s))

            separator = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
            header = "|" + "|".join(f" {c:<{col_widths[i]}} " for i, c in enumerate(columns)) + "|"
            lines = [separator, header, separator]
            for str_row in str_rows:
                line = "|" + "|".join(f" {v:<{col_widths[i]}} " for i, v in enumerate(str_row)) + "|"
                lines.append(line)
            lines.append(separator)
            output = "\n".join(lines)

        connection.close()
        return {"success": True, "output": output, "format": output_format, "row_count": len(rows)}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
def list_adapter_capabilities(
    resource_url: Optional[str] = None,
    adapter_kwargs: Optional[str] = None
) -> dict:
    """
    Introspect and list available Shillelagh adapters, their supported URL patterns,
    and columns/fields they expose for a given resource URL.
    """
    try:
        import importlib
        import pkgutil
        import shillelagh.adapters.api as api_adapters
        import shillelagh.adapters.file as file_adapters

        adapter_info = []

        # List all known adapter modules
        adapter_packages = []
        try:
            adapter_packages += [
                name for _, name, _ in pkgutil.iter_modules(api_adapters.__path__)
            ]
        except Exception:
            pass
        try:
            adapter_packages += [
                name for _, name, _ in pkgutil.iter_modules(file_adapters.__path__)
            ]
        except Exception:
            pass

        if not resource_url:
            # Just list what we know about
            known_adapters = [
                {
                    "name": "gsheets",
                    "description": "Google Sheets adapter",
                    "url_pattern": "https://docs.google.com/spreadsheets/d/...",
                    "supports": ["SELECT", "INSERT", "UPDATE", "DELETE"]
                },
                {
                    "name": "github",
                    "description": "GitHub API adapter (issues, PRs, repos)",
                    "url_pattern": "https://api.github.com/repos/{owner}/{repo}/issues",
                    "supports": ["SELECT"]
                },
                {
                    "name": "datasette",
                    "description": "Datasette instance adapter",
                    "url_pattern": "https://{datasette-host}/{database}/{table}",
                    "supports": ["SELECT"]
                },
                {
                    "name": "weatherapi",
                    "description": "WeatherAPI.com historical weather data",
                    "url_pattern": "https://api.weatherapi.com/v1/history.json?key={API_KEY}&q={location}",
                    "supports": ["SELECT"]
                },
                {
                    "name": "s3select",
                    "description": "AWS S3 Select for querying S3 objects",
                    "url_pattern": "s3://{bucket}/{key}",
                    "supports": ["SELECT"]
                },
                {
                    "name": "dbt_metricflow",
                    "description": "dbt Metric Flow metrics adapter",
                    "url_pattern": "https://semantic-layer.cloud.getdbt.com/",
                    "supports": ["SELECT"]
                },
                {
                    "name": "csv",
                    "description": "CSV file adapter",
                    "url_pattern": "file:///path/to/file.csv or http(s)://...",
                    "supports": ["SELECT", "INSERT", "DELETE"]
                }
            ]
            return {
                "success": True,
                "adapters": known_adapters,
                "note": "Pass a resource_url to inspect a specific resource's columns."
            }

        # Inspect a specific resource URL
        from shillelagh.backends.apsw.db import connect

        kwargs = {}
        if adapter_kwargs:
            try:
                kwargs = json.loads(adapter_kwargs)
            except json.JSONDecodeError as e:
                return {"success": False, "error": f"Invalid adapter_kwargs JSON: {e}"}

        connection = connect(":memory:", adapter_kwargs=kwargs)
        cursor = connection.cursor()

        # Use PRAGMA to get table info
        try:
            pragma_query = f'PRAGMA table_info("{resource_url}")'
            rows = list(cursor.execute(pragma_query))
            columns = []
            for row in rows:
                columns.append({
                    "cid": row[0],
                    "name": row[1],
                    "type": row[2],
                    "notnull": bool(row[3]),
                    "default_value": row[4],
                    "primary_key": bool(row[5])
                })
            connection.close()
            return {
                "success": True,
                "resource_url": resource_url,
                "columns": columns,
                "column_count": len(columns)
            }
        except Exception as pragma_err:
            # Fall back to SELECT * LIMIT 0 to get schema
            try:
                schema_query = f'SELECT * FROM "{resource_url}" LIMIT 1'
                rows = list(cursor.execute(schema_query))
                cols = []
                if cursor.description:
                    cols = [{"name": desc[0], "type": str(desc[1])} for desc in cursor.description]
                connection.close()
                return {
                    "success": True,
                    "resource_url": resource_url,
                    "columns": cols,
                    "column_count": len(cols),
                    "sample_rows": _rows_to_serializable(cursor, rows)["rows"]
                }
            except Exception as e:
                connection.close()
                return {
                    "success": False,
                    "error": str(e),
                    "pragma_error": str(pragma_err),
                    "error_type": type(e).__name__
                }

    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
def create_sqlalchemy_engine(
    query: str,
    connection_string: str = "shillelagh://",
    adapter_kwargs: Optional[str] = None
) -> dict:
    """
    Create and test a SQLAlchemy engine using the Shillelagh dialect,
    then execute a SQL query through it. Useful for integrating with
    Pandas, Superset, or ORM frameworks.
    """
    try:
        from sqlalchemy import text
        from sqlalchemy.engine import create_engine

        connect_args = {}
        if adapter_kwargs:
            try:
                parsed_kwargs = json.loads(adapter_kwargs)
                connect_args["adapter_kwargs"] = parsed_kwargs
            except json.JSONDecodeError as e:
                return {"success": False, "error": f"Invalid adapter_kwargs JSON: {e}"}

        if connect_args:
            engine = create_engine(connection_string, connect_args=connect_args)
        else:
            engine = create_engine(connection_string)

        with engine.connect() as conn:
            result = conn.execute(text(query))

            if result.returns_rows:
                columns = list(result.keys())
                rows = result.fetchall()
                result_rows = []
                for row in rows:
                    serialized_row = {}
                    for col, val in zip(columns, row):
                        if hasattr(val, 'isoformat'):
                            serialized_row[col] = val.isoformat()
                        elif isinstance(val, (int, float, str, bool, type(None))):
                            serialized_row[col] = val
                        else:
                            serialized_row[col] = str(val)
                    result_rows.append(serialized_row)

                return {
                    "success": True,
                    "connection_string": connection_string,
                    "data": {
                        "columns": columns,
                        "rows": result_rows,
                        "rowcount": len(result_rows)
                    }
                }
            else:
                return {
                    "success": True,
                    "connection_string": connection_string,
                    "data": {
                        "columns": [],
                        "rows": [],
                        "rowcount": result.rowcount
                    }
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
