"""
Semantic Model Adapter for Fabric AI Gateway

Provides connectivity to Power BI Semantic Models via:
- Power BI REST API (for listing workspaces/models)
- XMLA Endpoint (for schema discovery and DAX execution)
- TMSL over XMLA (for measure write-back)
"""

import json
from typing import Optional, Any
from dataclasses import dataclass

import requests
from tabulate import tabulate

from .auth import FabricAuthManager
from .context_manager import get_context


# Power BI REST API base URL
POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"


@dataclass
class SemanticModelInfo:
    """Information about a semantic model."""
    id: str
    name: str
    workspace_id: str
    configured_by: Optional[str] = None
    endorsement: Optional[str] = None
    last_refresh: Optional[str] = None


class SemanticModelAdapter:
    """
    Adapter for interacting with Power BI Semantic Models.
    
    Supports:
    - Listing workspaces and models
    - Schema discovery via XMLA DMV queries
    - DAX query execution
    - Measure CRUD via TMSL
    """
    
    def __init__(self, auth: FabricAuthManager):
        """
        Initialize the adapter.
        
        Args:
            auth: Authenticated FabricAuthManager instance
        """
        self.auth = auth
        self._connected_model_id: Optional[str] = None
        self._connected_model_name: Optional[str] = None
        self._xmla_endpoint: Optional[str] = None
    
    def _get_headers(self) -> dict[str, str]:
        """Get authorization headers for REST API calls."""
        token = self.auth.get_token(service="powerbi")
        if not token:
            raise RuntimeError("Not authenticated to Power BI")
        
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    async def list_workspaces(self) -> list[dict[str, str]]:
        """
        List all workspaces the user has access to.
        
        Returns:
            List of dicts with 'id' and 'name' keys
        """
        url = f"{POWERBI_API_BASE}/groups"
        
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            
            data = response.json()
            workspaces = []
            
            for ws in data.get("value", []):
                workspaces.append({
                    "id": ws.get("id"),
                    "name": ws.get("name"),
                    "type": ws.get("type", "Workspace")
                })
            
            return workspaces
            
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to list workspaces: {e}")
    
    async def list_models(self, workspace_id: str) -> list[dict[str, Any]]:
        """
        List semantic models (datasets) in a workspace.
        
        Args:
            workspace_id: Workspace GUID
            
        Returns:
            List of model info dicts
        """
        url = f"{POWERBI_API_BASE}/groups/{workspace_id}/datasets"
        
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            
            data = response.json()
            models = []
            
            for ds in data.get("value", []):
                models.append({
                    "id": ds.get("id"),
                    "name": ds.get("name"),
                    "configured_by": ds.get("configuredBy"),
                    "is_refreshable": ds.get("isRefreshable", False),
                    "is_effective_identity_required": ds.get("isEffectiveIdentityRequired", False)
                })
            
            return models
            
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to list models: {e}")
    
    async def connect(self, model_id: str) -> bool:
        """
        Connect to a semantic model for subsequent operations.
        
        Args:
            model_id: Semantic model (dataset) GUID
            
        Returns:
            True if connection successful
        """
        self._connected_model_id = model_id
        
        # Build XMLA endpoint
        ctx = get_context()
        if ctx.semantic_context:
            workspace_name = ctx.semantic_context.workspace_name
            workspace_id = ctx.semantic_context.workspace_id
            self._xmla_endpoint = f"powerbi://api.powerbi.com/v1.0/myorg/{workspace_name}"
            
            # Resolve model name (required for Catalog and TMSL)
            try:
                models = await self.list_models(workspace_id)
                for m in models:
                    if m["id"] == model_id:
                        self._connected_model_name = m["name"]
                        break
            except Exception as e:
                # Fallback if listing fails or name not found (unlikely)
                print(f"Warning: Could not resolve model name: {e}")
                self._connected_model_name = model_id # Fallback to ID
        
        return True
    
    async def get_schema(self) -> dict[str, Any]:
        """
        Get the schema of the connected semantic model.
        
        Uses XMLA DMV queries to retrieve:
        - Tables and columns
        - Measures
        - Relationships
        
        Returns:
            Schema dict with tables, measures, relationships
        """
        if not self._connected_model_id:
            raise RuntimeError("Not connected to a model. Call connect() first.")
        
        # TODO: Implement XMLA DMV queries
        # For now, return placeholder
        from .utils.xmla_client import XMLAClient
        
        try:
            # Use model name as Catalog
            client = XMLAClient(self._xmla_endpoint, self.auth, catalog=self._connected_model_name)
            
            # Query tables
            tables = await client.query_dmv(
                "SELECT * FROM $SYSTEM.TMSCHEMA_TABLES WHERE [IsHidden] = FALSE"
            )
            
            # Query columns
            columns = await client.query_dmv(
                "SELECT * FROM $SYSTEM.TMSCHEMA_COLUMNS WHERE [IsHidden] = FALSE"
            )
            
            # Query measures
            measures = await client.query_dmv(
                "SELECT * FROM $SYSTEM.TMSCHEMA_MEASURES"
            )
            
            # Query relationships
            relationships = await client.query_dmv(
                "SELECT * FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS"
            )
            
            return {
                "tables": tables,
                "columns": columns,
                "measures": measures,
                "relationships": relationships
            }
            
        except Exception as e:
            raise RuntimeError(f"Failed to get schema: {e}")
    
    async def execute_dax(self, query: str, max_rows: Optional[int] = None) -> str:
        """
        Execute a DAX query and return results as markdown table.
        
        Args:
            query: DAX query (must start with EVALUATE)
            max_rows: Maximum rows to return (default from config)
            
        Returns:
            Results as markdown table string
        """
        if not self._connected_model_id:
            raise RuntimeError("Not connected to a model. Call connect() first.")
        
        # Validate query starts with EVALUATE
        query_upper = query.strip().upper()
        if not query_upper.startswith("EVALUATE"):
            raise ValueError("DAX query must start with EVALUATE")
        
        # Get max rows from context if not specified
        if max_rows is None:
            ctx = get_context()
            max_rows = ctx.limits.max_dax_rows
        
        # TODO: Implement XMLA Execute for DAX
        from .utils.xmla_client import XMLAClient
        
        try:
            client = XMLAClient(self._xmla_endpoint, self.auth, catalog=self._connected_model_name)
            results = await client.execute_dax(query, max_rows)
            
            # Format as markdown table
            if results:
                headers = list(results[0].keys())
                rows = [list(row.values()) for row in results]
                return tabulate(rows, headers=headers, tablefmt="github")
            else:
                return "No results returned."
                
        except Exception as e:
            raise RuntimeError(f"DAX execution failed: {e}")
    
    async def upsert_measure(
        self,
        name: str,
        formula: str,
        table: Optional[str] = None,
        description: Optional[str] = None,
        format_string: Optional[str] = None,
        dry_run: bool = True
    ) -> dict[str, Any]:
        """
        Create or update a measure in the semantic model.
        
        Args:
            name: Measure name
            formula: DAX formula
            table: Table to add measure to (uses first table if not specified)
            description: Optional description
            format_string: Optional format string
            dry_run: If True, only preview the change (default: True)
            
        Returns:
            Dict with operation status and TMSL payload
        """
        if not self._connected_model_id:
            raise RuntimeError("Not connected to a model. Call connect() first.")
        
        # Generate TMSL payload
        from .utils.tmsl_generator import generate_measure_upsert
        
        tmsl_payload = generate_measure_upsert(
            name=name,
            formula=formula,
            table=table,
            description=description,
            format_string=format_string
        )
        
        result = {
            "operation": "upsert_measure",
            "measure_name": name,
            "dry_run": dry_run,
            "tmsl": tmsl_payload
        }
        
        if dry_run:
            result["status"] = "preview"
            result["message"] = "Dry run - no changes made. Set dry_run=False to apply."
        else:
            try:
                from .utils.xmla_client import XMLAClient
                client = XMLAClient(self._xmla_endpoint, self.auth, catalog=self._connected_model_name)
                
                # Replace <database> placeholder with actual model name
                db_name = self._connected_model_name or self._connected_model_id
                final_tmsl = tmsl_payload.replace("<database>", db_name)
                
                # Execute
                execution_result = await client.execute_tmsl(final_tmsl)
                result["status"] = execution_result["status"]
                result["message"] = execution_result["message"]
                if "details" in execution_result:
                    result["details"] = execution_result["details"]
                    
            except Exception as e:
                result["status"] = "error"
                result["message"] = str(e)
        
        return result
    
    async def delete_measure(
        self,
        name: str,
        dry_run: bool = True
    ) -> dict[str, Any]:
        """
        Delete a measure from the semantic model.
        
        Args:
            name: Measure name to delete
            dry_run: If True, only preview the change (default: True)
            
        Returns:
            Dict with operation status and TMSL payload
        """
        if not self._connected_model_id:
            raise RuntimeError("Not connected to a model. Call connect() first.")
        
        from .utils.tmsl_generator import generate_measure_delete
        
        tmsl_payload = generate_measure_delete(name)
        
        result = {
            "operation": "delete_measure",
            "measure_name": name,
            "dry_run": dry_run,
            "tmsl": tmsl_payload
        }
        
        if dry_run:
            result["status"] = "preview"
            result["message"] = "Dry run - no changes made. Set dry_run=False to apply."
        else:
            try:
                from .utils.xmla_client import XMLAClient
                client = XMLAClient(self._xmla_endpoint, self.auth, catalog=self._connected_model_name)
                
                # Replace <database> placeholder
                db_name = self._connected_model_name or self._connected_model_id
                final_tmsl = tmsl_payload.replace("<database>", db_name)
                
                # Execute
                execution_result = await client.execute_tmsl(final_tmsl)
                result["status"] = execution_result["status"]
                result["message"] = execution_result["message"]
                if "details" in execution_result:
                    result["details"] = execution_result["details"]

            except Exception as e:
                result["status"] = "error"
                result["message"] = str(e)
        
        return result
