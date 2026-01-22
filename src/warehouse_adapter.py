"""
Data Warehouse Adapter for Fabric AI Gateway

Provides connectivity to Microsoft Fabric Data Warehouses via:
- ODBC Driver 18 with Azure AD token authentication
- SQL queries with result limiting
- Table profiling and sampling
"""

import struct
from typing import Optional, Any
from dataclasses import dataclass

from tabulate import tabulate

from .auth import FabricAuthManager
from .context_manager import get_context

# ODBC attribute for SQL Server Access Token
SQL_COPT_SS_ACCESS_TOKEN = 1256


@dataclass
class TableInfo:
    """Information about a warehouse table."""
    schema: str
    name: str
    row_count: Optional[int] = None
    size_mb: Optional[float] = None
    column_count: Optional[int] = None


class WarehouseAdapter:
    """
    Adapter for interacting with Fabric Data Warehouses.
    
    Supports:
    - Connection via ODBC with Azure AD token
    - Overview scanning (schemas, tables, row counts)
    - Table profiling (columns, types, null ratios, distinct counts)
    - Data sampling
    """
    
    def __init__(self, auth: FabricAuthManager):
        """
        Initialize the adapter.
        
        Args:
            auth: Authenticated FabricAuthManager instance
        """
        self.auth = auth
        self._connection = None
        self._sql_endpoint: Optional[str] = None
        self._database: Optional[str] = None
    
    def _get_connection(self):
        """
        Get or create ODBC connection with Azure AD token auth.
        
        Uses binary access token format required by ODBC Driver 18.
        """
        import pyodbc
        
        if self._connection:
            try:
                # Test connection is still valid
                self._connection.execute("SELECT 1")
                return self._connection
            except Exception:
                self._connection = None
        
        if not self._sql_endpoint:
            raise RuntimeError("SQL endpoint not set. Call connect() first.")
        
        # Get binary token
        token_bytes = self.auth.get_sql_token_bytes()
        if not token_bytes:
            raise RuntimeError("Failed to get SQL access token")
        
        # Build connection string
        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={self._sql_endpoint};"
            f"Database={self._database or 'master'};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )
        
        # Connect with binary token in attrs_before
        self._connection = pyodbc.connect(
            conn_str,
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_bytes}
        )
        
        return self._connection
    
    async def connect(self, sql_endpoint: str, database: Optional[str] = None) -> bool:
        """
        Connect to a Fabric Data Warehouse.
        
        Args:
            sql_endpoint: SQL endpoint (e.g., workspace.datawarehouse.fabric.microsoft.com)
            database: Optional database name
            
        Returns:
            True if connection successful
        """
        self._sql_endpoint = sql_endpoint
        self._database = database
        
        # Update context
        ctx = get_context()
        ctx.set_warehouse(sql_endpoint, database)
        
        # Test connection
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            raise RuntimeError(f"Connection failed: {e}")
    
    async def get_overview(self) -> dict[str, Any]:
        """
        Get overview of the warehouse (Layer 1 - Light Scan).
        
        Returns:
            Dict with schemas, tables, and basic stats
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        ctx = get_context()
        max_tables = ctx.limits.max_tables_in_context
        
        # Get schemas
        cursor.execute("""
            SELECT DISTINCT TABLE_SCHEMA
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA
        """)
        schemas = [row[0] for row in cursor.fetchall()]
        
        # Get tables with row counts
        cursor.execute(f"""
            SELECT TOP {max_tables}
                t.TABLE_SCHEMA,
                t.TABLE_NAME,
                p.rows as row_count
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.tables st 
                ON st.name = t.TABLE_NAME 
                AND SCHEMA_NAME(st.schema_id) = t.TABLE_SCHEMA
            LEFT JOIN sys.partitions p 
                ON p.object_id = st.object_id 
                AND p.index_id IN (0, 1)
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
        """)
        
        tables = []
        for row in cursor.fetchall():
            tables.append({
                "schema": row[0],
                "name": row[1],
                "full_name": f"{row[0]}.{row[1]}",
                "row_count": row[2] or 0
            })
        
        cursor.close()
        
        # Update context
        ctx.update_warehouse_overview(schemas, tables)
        
        return {
            "endpoint": self._sql_endpoint,
            "database": self._database,
            "schemas": schemas,
            "tables": tables,
            "table_count": len(tables),
            "truncated": len(tables) >= max_tables
        }
    
    async def profile_table(self, schema_table: str) -> dict[str, Any]:
        """
        Get detailed profile of a table (Layer 2 - Deep Dive).
        
        Args:
            schema_table: Table in format schema.table
            
        Returns:
            Dict with columns, types, null ratios, distinct counts
        """
        # Parse schema.table
        parts = schema_table.split(".")
        if len(parts) != 2:
            raise ValueError("Table must be in format schema.table")
        
        schema_name, table_name = parts
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        ctx = get_context()
        max_columns = ctx.limits.max_columns_per_table
        
        # Get column info
        cursor.execute(f"""
            SELECT TOP {max_columns}
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = ?
                AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """, (schema_name, table_name))
        
        columns = []
        for row in cursor.fetchall():
            col = {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES",
                "max_length": row[3],
                "precision": row[4],
                "scale": row[5]
            }
            columns.append(col)
        
        # Get null and distinct counts for each column
        # (This can be expensive, so we limit to important columns)
        profile_columns = columns[:10]  # Profile first 10 columns
        
        for col in profile_columns:
            try:
                col_name = col["name"]
                # Null ratio
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN [{col_name}] IS NULL THEN 1 ELSE 0 END) as nulls
                    FROM [{schema_name}].[{table_name}]
                """)
                row = cursor.fetchone()
                if row and row[0] > 0:
                    col["null_ratio"] = round(row[1] / row[0], 4)
                
                # Distinct count (approximate for large tables)
                cursor.execute(f"""
                    SELECT COUNT(DISTINCT [{col_name}])
                    FROM [{schema_name}].[{table_name}]
                """)
                col["distinct_count"] = cursor.fetchone()[0]
                
            except Exception as e:
                # Skip columns that fail profiling
                col["profile_error"] = str(e)
        
        cursor.close()
        
        return {
            "table": schema_table,
            "column_count": len(columns),
            "columns": columns,
            "profiled_columns": len(profile_columns)
        }
    
    async def sample_rows(
        self, 
        schema_table: str, 
        n: Optional[int] = None
    ) -> str:
        """
        Get sample rows from a table.
        
        Args:
            schema_table: Table in format schema.table
            n: Number of rows (default from config, max 50)
            
        Returns:
            Markdown table of sample rows
        """
        # Parse schema.table
        parts = schema_table.split(".")
        if len(parts) != 2:
            raise ValueError("Table must be in format schema.table")
        
        schema_name, table_name = parts
        
        ctx = get_context()
        if n is None:
            n = ctx.limits.sample_rows
        n = min(n, 50)  # Hard limit
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT TOP {n} *
            FROM [{schema_name}].[{table_name}]
        """)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch rows
        rows = cursor.fetchall()
        cursor.close()
        
        # Format as markdown
        if rows:
            data = [list(row) for row in rows]
            return tabulate(data, headers=columns, tablefmt="github")
        else:
            return "No rows found."
    
    async def execute_sql(
        self, 
        query: str, 
        max_rows: Optional[int] = None
    ) -> str:
        """
        Execute a SQL query (SELECT only).
        
        Args:
            query: SQL SELECT query
            max_rows: Maximum rows to return
            
        Returns:
            Markdown table of results
        """
        # Validate query is SELECT only
        query_upper = query.strip().upper()
        if not query_upper.startswith("SELECT"):
            raise ValueError("Only SELECT queries are allowed")
        
        # Block dangerous keywords
        dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "EXEC"]
        for keyword in dangerous:
            if keyword in query_upper:
                raise ValueError(f"Query contains forbidden keyword: {keyword}")
        
        ctx = get_context()
        if max_rows is None:
            max_rows = ctx.limits.max_sql_result_rows
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Execute with row limit
        cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch limited rows
        rows = cursor.fetchmany(max_rows)
        cursor.close()
        
        # Format as markdown
        if rows:
            data = [list(row) for row in rows]
            return tabulate(data, headers=columns, tablefmt="github")
        else:
            return "No results returned."
    
    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
