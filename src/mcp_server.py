#!/usr/bin/env python3
"""
MCP Server for Fabric AI Gateway
Supports both Semantic Models (Power BI) and Data Warehouse (SQL).
"""

import sys
import os
import json
import warnings

# Suppress warnings to stderr to avoid polluting stdout (MCP protocol)
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"

src_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(src_dir)
sys.path.insert(0, src_dir)
sys.path.insert(0, project_root)  # Add project root for package imports
os.chdir(project_root)

import requests
from auth import get_auth_manager, load_config

# Import pyodbc for Warehouse mode
try:
    import pyodbc
except ImportError:
    pyodbc = None

# Import XMLA client for direct TMSL execution
try:
    from src.utils.xmla_client import XMLAClient
    XMLA_AVAILABLE = True
except ImportError:
    XMLAClient = None
    XMLA_AVAILABLE = False

import asyncio

class FabricMCPServer:
    def __init__(self):
        self.mode = "semantic"
        self.sql_endpoint = None
        self.workspace_id = None
        self.workspace_name = None
        self.dataset_id = None
        self.dataset_name = None
        self.database_name = None
        self.token = None
        self.auth = None
        self.config = None
        self.xmla_client = None
        
    def initialize(self):
        """Load context and connection."""
        try:
            # 1. Auth
            self.config = load_config()
            self.auth = get_auth_manager(self.config)
            self.token = self.auth.get_token(service="powerbi")
            
            # 2. Load Context
            context_path = os.path.expanduser("~/.fabric-gateway/context.json")
            if os.path.exists(context_path):
                with open(context_path) as f:
                    ctx = json.load(f)
                    self.mode = ctx.get("mode", "semantic")
                    self.workspace_id = ctx.get("workspace_id")
                    self.workspace_name = ctx.get("workspace_name")
                    self.dataset_id = ctx.get("item_id")
                    self.dataset_name = ctx.get("item_name")
                    self.sql_endpoint = ctx.get("sql_endpoint")
                    self.database_name = ctx.get("item_name")
            
            # Note: XMLA client disabled on macOS
            # Power BI XMLA endpoint requires Analysis Services protocol (not REST API)
            # which is only available via Windows tools (SSMS, Tabular Editor) or .NET TOM library.
            # TMSL scripts will be returned for manual execution in SSMS/Tabular Editor.
            # if XMLA_AVAILABLE and self.mode == "semantic" and self.workspace_name:
            #     xmla_endpoint = f"powerbi://api.powerbi.com/v1.0/myorg/{self.workspace_name}"
            #     self.xmla_client = XMLAClient(xmla_endpoint, self.auth, self.dataset_name)
            
            return True
        except Exception as e:
            sys.stderr.write(f"Init Error: {e}\n")
            return False
    
    def execute_tmsl(self, tmsl_dict):
        """Execute TMSL command via XMLA endpoint."""
        if not self.xmla_client:
            return {"error": "XMLA client not initialized. Workspace name may be missing."}
        
        try:
            tmsl_json = json.dumps(tmsl_dict)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.xmla_client.execute_tmsl(tmsl_json))
            loop.close()
            return result
        except Exception as e:
            return {"error": f"TMSL execution failed: {str(e)}"}

    # ================= SEMANTIC TOOLS =================
    def semantic_request(self, method, path, data=None):
        headers = {"Authorization": f"Bearer {self.token}"}
        url = f"https://api.powerbi.com/v1.0/myorg{path}"
        if method == "GET":
            r = requests.get(url, headers=headers)
        else:
            r = requests.post(url, json=data, headers=headers)
        r.raise_for_status()
        return r.json()

    def handle_semantic(self, name, args):
        try:
            if name == "list_workspaces":
                res = self.semantic_request("GET", "/groups")
                workspaces = [{"id": w["id"], "name": w["name"]} for w in res.get("value", [])]
                return {"workspaces": workspaces}
            
            elif name == "list_datasets":
                ws_id = args.get("workspace_id") or self.workspace_id
                if not ws_id:
                    return {"error": "No workspace_id. Use list_workspaces first."}
                res = self.semantic_request("GET", f"/groups/{ws_id}/datasets")
                datasets = [{"id": d["id"], "name": d["name"]} for d in res.get("value", [])]
                return {"datasets": datasets}
            
            elif name == "connect_dataset":
                ws_id = args.get("workspace_id") or self.workspace_id
                ds_id = args.get("dataset_id")
                if not ws_id or not ds_id:
                    return {"error": "workspace_id and dataset_id required"}
                self.workspace_id = ws_id
                self.dataset_id = ds_id
                # Save to context
                context_path = os.path.expanduser("~/.fabric-gateway/context.json")
                ctx = {"mode": "semantic", "workspace_id": ws_id, "item_id": ds_id}
                with open(context_path, "w") as f:
                    json.dump(ctx, f)
                return {"status": "connected", "workspace_id": ws_id, "dataset_id": ds_id}
            
            elif name == "get_tables":
                if not self.dataset_id:
                    return {"error": "No dataset connected. Use connect_dataset first."}
                data = {"queries": [{"query": "EVALUATE COLUMNSTATISTICS()"}], "serializerSettings": {"includeNulls": True}}
                res = self.semantic_request("POST", f"/groups/{self.workspace_id}/datasets/{self.dataset_id}/executeQueries", data)
                rows = res["results"][0]["tables"][0]["rows"]
                tables = {}
                for r in rows:
                    t = r.get("[Table Name]")
                    if t:
                        tables[t] = tables.get(t, 0) + 1
                return {"tables": list(tables.keys()), "column_counts": tables}
                
            elif name == "execute_dax":
                if not self.dataset_id:
                    return {"error": "No dataset connected. Use connect_dataset first."}
                q = args.get("query", "")
                if not q.strip().upper().startswith("EVALUATE"):
                    q = f"EVALUATE {q}"
                data = {"queries": [{"query": q}], "serializerSettings": {"includeNulls": True}}
                res = self.semantic_request("POST", f"/groups/{self.workspace_id}/datasets/{self.dataset_id}/executeQueries", data)
                return res
            
            elif name == "get_dataset_info":
                if not self.dataset_id:
                    return {"error": "No dataset connected"}
                res = self.semantic_request("GET", f"/groups/{self.workspace_id}/datasets/{self.dataset_id}")
                return res
            
            elif name == "refresh_dataset":
                if not self.dataset_id:
                    return {"error": "No dataset connected"}
                self.semantic_request("POST", f"/groups/{self.workspace_id}/datasets/{self.dataset_id}/refreshes", {})
                return {"status": "refresh_triggered"}
            
            elif name == "get_measures":
                if not self.dataset_id:
                    return {"error": "No dataset connected"}
                # Use SELECTCOLUMNS format - works better than simple INFO.MEASURES()
                dax = """EVALUATE SELECTCOLUMNS(
                    INFO.MEASURES(),
                    "TableID", [TableID],
                    "Name", [Name],
                    "Expression", [Expression],
                    "Description", [Description],
                    "DataType", [DataType],
                    "IsHidden", [IsHidden]
                )"""
                try:
                    data = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
                    res = self.semantic_request("POST", f"/groups/{self.workspace_id}/datasets/{self.dataset_id}/executeQueries", data)
                    measures = res["results"][0]["tables"][0]["rows"]
                    return {"measures": measures}
                except requests.exceptions.HTTPError as e:
                    error_detail = e.response.text if e.response else str(e)
                    return {"error": f"INFO.MEASURES() failed: {error_detail}"}
                except Exception as e:
                    return {"error": f"INFO.MEASURES() failed: {str(e)}"}
            
            elif name == "get_relationships":
                if not self.dataset_id:
                    return {"error": "No dataset connected"}
                # Use SELECTCOLUMNS format - discovered by Gemini to work!
                dax = """EVALUATE SELECTCOLUMNS(
                    INFO.RELATIONSHIPS(),
                    "ID", [ID],
                    "FromTableID", [FromTableID],
                    "FromColumnID", [FromColumnID],
                    "ToTableID", [ToTableID],
                    "ToColumnID", [ToColumnID],
                    "IsActive", [IsActive],
                    "CrossFilteringBehavior", [CrossFilteringBehavior]
                )"""
                try:
                    data = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
                    res = self.semantic_request("POST", f"/groups/{self.workspace_id}/datasets/{self.dataset_id}/executeQueries", data)
                    relationships = res["results"][0]["tables"][0]["rows"]
                    return {"relationships": relationships}
                except requests.exceptions.HTTPError as e:
                    error_detail = e.response.text if e.response else str(e)
                    return {"error": f"INFO.RELATIONSHIPS() failed: {error_detail}"}
                except Exception as e:
                    return {"error": f"INFO.RELATIONSHIPS() failed: {str(e)}"}
            
            elif name == "get_columns":
                if not self.dataset_id:
                    return {"error": "No dataset connected"}
                table_name = args.get("table_name")
                # Use COLUMNSTATISTICS() which works on Pro
                data = {"queries": [{"query": "EVALUATE COLUMNSTATISTICS()"}], "serializerSettings": {"includeNulls": True}}
                res = self.semantic_request("POST", f"/groups/{self.workspace_id}/datasets/{self.dataset_id}/executeQueries", data)
                rows = res["results"][0]["tables"][0]["rows"]
                columns = []
                for r in rows:
                    t = r.get("[Table Name]")
                    if table_name and t != table_name:
                        continue
                    columns.append({
                        "table": t,
                        "column": r.get("[Column Name]"),
                        "cardinality": r.get("[Column Cardinality]"),
                        "max_length": r.get("[Max Length]")
                    })
                return {"columns": columns}
            
            elif name == "create_measure":
                if not self.dataset_id:
                    return {"error": "No dataset connected"}
                table_name = args.get("table_name")
                measure_name = args.get("name")
                expression = args.get("expression")
                description = args.get("description", "")
                execute_now = args.get("execute", True)  # Default to execute
                
                if not all([table_name, measure_name, expression]):
                    return {"error": "table_name, name, and expression are required"}
                
                tmsl = {
                    "createOrReplace": {
                        "object": {
                            "database": self.dataset_name or self.dataset_id,
                            "table": table_name,
                            "measure": measure_name
                        },
                        "measure": {
                            "name": measure_name,
                            "expression": expression,
                            "description": description
                        }
                    }
                }
                
                # Try to execute via XMLA if available
                if execute_now and self.xmla_client:
                    result = self.execute_tmsl(tmsl)
                    if result.get("status") == "success":
                        return {"status": "success", "message": f"Measure '{measure_name}' created successfully!"}
                    else:
                        return {"status": "error", "message": result.get("error") or result.get("message"), "tmsl": tmsl}
                else:
                    return {
                        "status": "tmsl_generated",
                        "note": "XMLA client not available. Execute this TMSL manually in SSMS or Tabular Editor.",
                        "tmsl": tmsl
                    }
            
            elif name == "delete_measure":
                if not self.dataset_id:
                    return {"error": "No dataset connected"}
                table_name = args.get("table_name")
                measure_name = args.get("name")
                execute_now = args.get("execute", True)
                
                if not all([table_name, measure_name]):
                    return {"error": "table_name and name are required"}
                
                tmsl = {
                    "delete": {
                        "object": {
                            "database": self.dataset_name or self.dataset_id,
                            "table": table_name,
                            "measure": measure_name
                        }
                    }
                }
                
                if execute_now and self.xmla_client:
                    result = self.execute_tmsl(tmsl)
                    if result.get("status") == "success":
                        return {"status": "success", "message": f"Measure '{measure_name}' deleted successfully!"}
                    else:
                        return {"status": "error", "message": result.get("error") or result.get("message"), "tmsl": tmsl}
                else:
                    return {
                        "status": "tmsl_generated",
                        "note": "XMLA client not available. Execute this TMSL manually.",
                        "tmsl": tmsl
                    }
            
            elif name == "create_relationship":
                from_table = args.get("from_table")
                from_column = args.get("from_column")
                to_table = args.get("to_table")
                to_column = args.get("to_column")
                is_active = args.get("is_active", True)
                execute_now = args.get("execute", True)
                
                if not all([from_table, from_column, to_table, to_column]):
                    return {"error": "from_table, from_column, to_table, to_column are required"}
                
                rel_name = f"{from_table}_{from_column}_{to_table}_{to_column}"
                tmsl = {
                    "createOrReplace": {
                        "object": {
                            "database": self.dataset_name or self.dataset_id,
                            "relationship": rel_name
                        },
                        "relationship": {
                            "name": rel_name,
                            "fromTable": from_table,
                            "fromColumn": from_column,
                            "toTable": to_table,
                            "toColumn": to_column,
                            "isActive": is_active
                        }
                    }
                }
                
                if execute_now and self.xmla_client:
                    result = self.execute_tmsl(tmsl)
                    if result.get("status") == "success":
                        return {"status": "success", "message": f"Relationship '{rel_name}' created successfully!"}
                    else:
                        return {"status": "error", "message": result.get("error") or result.get("message"), "tmsl": tmsl}
                else:
                    return {
                        "status": "tmsl_generated",
                        "note": "XMLA client not available. Execute this TMSL manually.",
                        "tmsl": tmsl
                    }
            
            else:
                return {"error": f"Unknown tool: {name}"}
                
        except requests.exceptions.HTTPError as e:
            return {"error": str(e), "response": e.response.text if e.response else None}
        except Exception as e:
            return {"error": str(e)}

    # ================= WAREHOUSE TOOLS =================
    def get_sql_connection(self):
        if not pyodbc:
            raise ImportError("pyodbc not installed")
        if not self.sql_endpoint:
            raise ValueError("No SQL endpoint provided")
        
        sql_token = self.auth.get_token(service="sql")
        token_bytes = bytes(sql_token, "UTF-8")
        exptoken = b""
        for i in token_bytes:
            exptoken += bytes({i})
            exptoken += bytes(1)
        import struct
        tokenstruct = struct.pack("=i", len(exptoken)) + exptoken
        
        db_param = f"Database={self.database_name};" if self.database_name else ""
        conn_str = f"Driver={{ODBC Driver 18 for SQL Server}};Server={self.sql_endpoint},1433;{db_param}Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        
        conn = pyodbc.connect(conn_str, attrs_before={1256: tokenstruct})
        return conn

    def handle_warehouse(self, name, args):
        try:
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            
            if name == "get_warehouse_tables":
                schema_filter = args.get("schema")
                if schema_filter:
                    cursor.execute(f"SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='{schema_filter}'")
                else:
                    cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
                tables = [{"schema": row[0], "table": row[1]} for row in cursor.fetchall()]
                conn.close()
                return {"tables": tables}
                
            elif name == "execute_sql":
                query = args.get("query")
                cursor.execute(query)
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    results = []
                    for row in cursor.fetchmany(100):
                        results.append(dict(zip(columns, [str(v) if v is not None else None for v in row])))
                    conn.close()
                    return {"columns": columns, "rows": results}
                else:
                    conn.commit()
                    affected = cursor.rowcount
                    conn.close()
                    return {"status": "success", "rows_affected": affected}
            
            elif name == "describe_table":
                t = args.get("table_name")
                schema = args.get("schema", "dbo")
                cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{t}' ORDER BY ORDINAL_POSITION")
                cols = [{"column": r[0], "type": r[1], "nullable": r[2]} for r in cursor.fetchall()]
                conn.close()
                return {"columns": cols}
            
            else:
                conn.close()
                return {"error": f"Unknown tool: {name}"}
                
        except Exception as e:
            return {"error": str(e)}

    # ================= MAIN HANDLER =================
    def handle_tool(self, name, args):
        if self.mode == "semantic":
            return self.handle_semantic(name, args)
        elif self.mode == "warehouse":
            return self.handle_warehouse(name, args)
        else:
            return {"error": f"Unknown mode: {self.mode}"}

    def get_tools(self):
        """Return tool definitions based on current mode."""
        if self.mode == "semantic":
            return [
                {"name": "list_workspaces", "description": "List all Power BI workspaces accessible to the user", 
                 "inputSchema": {"type": "object", "properties": {}}},
                {"name": "list_datasets", "description": "List semantic models (datasets) in a workspace", 
                 "inputSchema": {"type": "object", "properties": {"workspace_id": {"type": "string", "description": "Optional workspace ID"}}}},
                {"name": "connect_dataset", "description": "Connect to a specific semantic model for DAX queries", 
                 "inputSchema": {"type": "object", "properties": {"workspace_id": {"type": "string"}, "dataset_id": {"type": "string"}}, "required": ["dataset_id"]}},
                {"name": "get_tables", "description": "Get all tables in the connected model with column counts", 
                 "inputSchema": {"type": "object", "properties": {}}},
                {"name": "get_columns", "description": "Get detailed column info. Optionally filter by table_name.", 
                 "inputSchema": {"type": "object", "properties": {"table_name": {"type": "string", "description": "Optional table name filter"}}}},
                {"name": "get_measures", "description": "Get all measures in the model (requires Premium/Fabric)", 
                 "inputSchema": {"type": "object", "properties": {}}},
                {"name": "get_relationships", "description": "Get all relationships in the model (requires Premium/Fabric)", 
                 "inputSchema": {"type": "object", "properties": {}}},
                {"name": "execute_dax", "description": "Execute a DAX query on the connected model", 
                 "inputSchema": {"type": "object", "properties": {"query": {"type": "string", "description": "DAX query starting with EVALUATE"}}, "required": ["query"]}},
                {"name": "get_dataset_info", "description": "Get metadata about the connected dataset", 
                 "inputSchema": {"type": "object", "properties": {}}},
                {"name": "refresh_dataset", "description": "Trigger a refresh of the connected dataset", 
                 "inputSchema": {"type": "object", "properties": {}}},
                {"name": "create_measure", "description": "Generate TMSL to create a new measure (requires Premium/Fabric for execution)", 
                 "inputSchema": {"type": "object", "properties": {
                     "table_name": {"type": "string", "description": "Table to add measure to"},
                     "name": {"type": "string", "description": "Measure name"},
                     "expression": {"type": "string", "description": "DAX expression"},
                     "description": {"type": "string", "description": "Optional description"}
                 }, "required": ["table_name", "name", "expression"]}},
                {"name": "delete_measure", "description": "Generate TMSL to delete a measure", 
                 "inputSchema": {"type": "object", "properties": {
                     "table_name": {"type": "string"},
                     "name": {"type": "string", "description": "Measure name to delete"}
                 }, "required": ["table_name", "name"]}},
                {"name": "create_relationship", "description": "Generate TMSL to create a new relationship", 
                 "inputSchema": {"type": "object", "properties": {
                     "from_table": {"type": "string"},
                     "from_column": {"type": "string"},
                     "to_table": {"type": "string"},
                     "to_column": {"type": "string"},
                     "is_active": {"type": "boolean", "description": "Default true"}
                 }, "required": ["from_table", "from_column", "to_table", "to_column"]}}
            ]
        else:
            return [
                {"name": "get_warehouse_tables", "description": "List all tables in the warehouse. Use schema param to filter.", 
                 "inputSchema": {"type": "object", "properties": {"schema": {"type": "string", "description": "Optional schema filter"}}}},
                {"name": "execute_sql", "description": "Execute a SQL SELECT query", 
                 "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}},
                {"name": "describe_table", "description": "Get column definitions for a table", 
                 "inputSchema": {"type": "object", "properties": {"table_name": {"type": "string"}, "schema": {"type": "string", "description": "Default: dbo"}}, "required": ["table_name"]}}
            ]

    def run(self):
        if not self.initialize():
            return
        
        tools = self.get_tools()

        # Stdio Loop - MCP Protocol
        for line in sys.stdin:
            try:
                msg = json.loads(line.strip())
                method = msg.get("method")
                mid = msg.get("id")
                
                if method == "initialize":
                    response = {
                        "jsonrpc": "2.0",
                        "id": mid,
                        "result": {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {"tools": {}},
                            "serverInfo": {"name": "fabric", "version": "1.0"}
                        }
                    }
                    print(json.dumps(response), flush=True)
                    
                elif method == "tools/list":
                    response = {
                        "jsonrpc": "2.0",
                        "id": mid,
                        "result": {"tools": tools}
                    }
                    print(json.dumps(response), flush=True)
                    
                elif method == "tools/call":
                    tool_name = msg["params"]["name"]
                    tool_args = msg["params"].get("arguments", {})
                    result = self.handle_tool(tool_name, tool_args)
                    response = {
                        "jsonrpc": "2.0",
                        "id": mid,
                        "result": {
                            "content": [{"type": "text", "text": json.dumps(result, default=str, ensure_ascii=False)}]
                        }
                    }
                    print(json.dumps(response), flush=True)
                    
                elif method == "notifications/initialized":
                    # Client notification, no response needed
                    pass
                    
                else:
                    # Unknown method
                    if mid:
                        response = {
                            "jsonrpc": "2.0",
                            "id": mid,
                            "error": {"code": -32601, "message": f"Method not found: {method}"}
                        }
                        print(json.dumps(response), flush=True)
                        
            except json.JSONDecodeError:
                pass
            except Exception as e:
                sys.stderr.write(f"Error: {e}\n")

if __name__ == "__main__":
    FabricMCPServer().run()
