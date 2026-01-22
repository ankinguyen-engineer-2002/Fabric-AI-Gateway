"""
Context Manager for Fabric AI Gateway

Manages application state including:
- Current connection mode (Semantic Model or Data Warehouse)
- Active workspace and model selection
- Configuration and limits
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional, Any
import yaml


class ConnectionMode(Enum):
    """Available connection modes."""
    NONE = "none"
    SEMANTIC_MODEL = "semantic_model"
    DATA_WAREHOUSE = "data_warehouse"


@dataclass
class SemanticModelContext:
    """Context for Semantic Model connection."""
    workspace_id: str
    workspace_name: str
    model_id: str
    model_name: str
    schema_loaded: bool = False
    tables: list[dict] = field(default_factory=list)
    measures: list[dict] = field(default_factory=list)
    relationships: list[dict] = field(default_factory=list)


@dataclass
class WarehouseContext:
    """Context for Data Warehouse connection."""
    sql_endpoint: str
    database_name: Optional[str] = None
    schemas: list[str] = field(default_factory=list)
    tables_overview: list[dict] = field(default_factory=list)


@dataclass
class Limits:
    """Resource limits for context protection."""
    max_dax_rows: int = 1000
    max_tables_in_context: int = 50
    max_columns_per_table: int = 100
    sample_rows: int = 10
    max_sql_result_rows: int = 500


class ContextManager:
    """
    Manages application state and context.
    
    Ensures:
    - Only one connection mode active at a time
    - Resource limits are enforced
    - State is consistent across tools
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        Initialize context manager.
        
        Args:
            config: Configuration dict (loaded from config.yaml if not provided)
        """
        self.config = config or self._load_config()
        self.limits = self._parse_limits()
        
        # Connection state
        self.mode = ConnectionMode.NONE
        self.semantic_context: Optional[SemanticModelContext] = None
        self.warehouse_context: Optional[WarehouseContext] = None
    
    def _load_config(self) -> dict:
        """Load configuration from file."""
        search_paths = [
            Path.cwd() / "config.yaml",
            Path(__file__).parent.parent / "config.yaml",
            Path.home() / ".fabric-gateway" / "config.yaml"
        ]
        
        for path in search_paths:
            if path.exists():
                with open(path) as f:
                    return yaml.safe_load(f)
        
        return {}
    
    def _parse_limits(self) -> Limits:
        """Parse limits from config."""
        limits_config = self.config.get("limits", {})
        return Limits(
            max_dax_rows=limits_config.get("max_dax_rows", 1000),
            max_tables_in_context=limits_config.get("max_tables_in_context", 50),
            max_columns_per_table=limits_config.get("max_columns_per_table", 100),
            sample_rows=limits_config.get("sample_rows", 10),
            max_sql_result_rows=limits_config.get("max_sql_result_rows", 500)
        )
    
    def set_semantic_model(
        self,
        workspace_id: str,
        workspace_name: str,
        model_id: str,
        model_name: str
    ) -> None:
        """
        Set active Semantic Model connection.
        
        Clears any existing warehouse connection.
        """
        self.mode = ConnectionMode.SEMANTIC_MODEL
        self.warehouse_context = None
        self.semantic_context = SemanticModelContext(
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            model_id=model_id,
            model_name=model_name
        )
    
    def set_warehouse(self, sql_endpoint: str, database_name: Optional[str] = None) -> None:
        """
        Set active Data Warehouse connection.
        
        Clears any existing semantic model connection.
        """
        self.mode = ConnectionMode.DATA_WAREHOUSE
        self.semantic_context = None
        self.warehouse_context = WarehouseContext(
            sql_endpoint=sql_endpoint,
            database_name=database_name
        )
    
    def update_semantic_schema(
        self,
        tables: list[dict],
        measures: list[dict],
        relationships: list[dict]
    ) -> None:
        """Update loaded schema for semantic model."""
        if not self.semantic_context:
            raise ValueError("No semantic model connected")
        
        # Apply limits
        tables = tables[:self.limits.max_tables_in_context]
        for table in tables:
            if "columns" in table:
                table["columns"] = table["columns"][:self.limits.max_columns_per_table]
        
        self.semantic_context.tables = tables
        self.semantic_context.measures = measures
        self.semantic_context.relationships = relationships
        self.semantic_context.schema_loaded = True
    
    def update_warehouse_overview(
        self,
        schemas: list[str],
        tables_overview: list[dict]
    ) -> None:
        """Update loaded overview for warehouse."""
        if not self.warehouse_context:
            raise ValueError("No warehouse connected")
        
        # Apply limits
        tables_overview = tables_overview[:self.limits.max_tables_in_context]
        
        self.warehouse_context.schemas = schemas
        self.warehouse_context.tables_overview = tables_overview
    
    def get_context_summary(self) -> dict[str, Any]:
        """Get current context summary for MCP tools."""
        summary = {
            "mode": self.mode.value,
            "limits": {
                "max_dax_rows": self.limits.max_dax_rows,
                "sample_rows": self.limits.sample_rows
            }
        }
        
        if self.mode == ConnectionMode.SEMANTIC_MODEL and self.semantic_context:
            summary["semantic_model"] = {
                "workspace": self.semantic_context.workspace_name,
                "model": self.semantic_context.model_name,
                "schema_loaded": self.semantic_context.schema_loaded,
                "table_count": len(self.semantic_context.tables),
                "measure_count": len(self.semantic_context.measures)
            }
        elif self.mode == ConnectionMode.DATA_WAREHOUSE and self.warehouse_context:
            summary["warehouse"] = {
                "endpoint": self.warehouse_context.sql_endpoint,
                "database": self.warehouse_context.database_name,
                "schema_count": len(self.warehouse_context.schemas),
                "table_count": len(self.warehouse_context.tables_overview)
            }
        
        return summary
    
    def clear(self) -> None:
        """Clear all connection state."""
        self.mode = ConnectionMode.NONE
        self.semantic_context = None
        self.warehouse_context = None


# Global context instance
_context: Optional[ContextManager] = None


def get_context() -> ContextManager:
    """Get or create the global context manager."""
    global _context
    if _context is None:
        _context = ContextManager()
    return _context
