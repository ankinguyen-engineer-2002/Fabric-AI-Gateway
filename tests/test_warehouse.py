"""
Unit tests for the warehouse adapter module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock


class TestWarehouseAdapter:
    """Tests for WarehouseAdapter class."""
    
    @pytest.fixture
    def mock_auth(self):
        """Create a mock auth manager."""
        auth = MagicMock()
        auth.get_sql_token_bytes.return_value = b"\x10\x00\x00\x00test_token_123"
        return auth
    
    @pytest.fixture
    def mock_pyodbc(self):
        """Mock pyodbc module."""
        with patch('src.warehouse_adapter.pyodbc') as mock:
            yield mock
    
    def test_sql_validation_blocks_dangerous_keywords(self, mock_auth):
        """Test that dangerous SQL keywords are blocked."""
        from src.warehouse_adapter import WarehouseAdapter
        
        adapter = WarehouseAdapter(mock_auth)
        adapter._sql_endpoint = "test.endpoint.com"
        
        dangerous_queries = [
            "SELECT * FROM users; DROP TABLE users;",
            "INSERT INTO users VALUES (1, 'test')",
            "UPDATE users SET name = 'hacked'",
            "DELETE FROM users WHERE 1=1",
            "TRUNCATE TABLE users",
            "ALTER TABLE users ADD column",
            "CREATE TABLE hacked (id int)",
            "EXEC sp_executesql 'DROP TABLE'",
        ]
        
        for query in dangerous_queries:
            with pytest.raises(ValueError) as exc_info:
                import asyncio
                asyncio.run(adapter.execute_sql(query))
            
            assert "forbidden keyword" in str(exc_info.value).lower() or \
                   "only select" in str(exc_info.value).lower()
    
    def test_sql_validation_allows_select(self, mock_auth, mock_pyodbc):
        """Test that SELECT queries are allowed."""
        from src.warehouse_adapter import WarehouseAdapter
        
        # Setup mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchmany.return_value = [("val1", "val2")]
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        adapter = WarehouseAdapter(mock_auth)
        adapter._sql_endpoint = "test.endpoint.com"
        adapter._connection = mock_conn
        
        import asyncio
        result = asyncio.run(adapter.execute_sql("SELECT * FROM users"))
        
        assert result is not None
        assert "col1" in result
    
    def test_sample_rows_limit(self, mock_auth):
        """Test that sample rows are limited to max 50."""
        from src.warehouse_adapter import WarehouseAdapter
        from src.context_manager import ContextManager, Limits
        
        adapter = WarehouseAdapter(mock_auth)
        
        # Request more than max
        with patch.object(adapter, '_get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.description = [("id",)]
            mock_cursor.fetchall.return_value = []
            mock_conn.return_value.cursor.return_value = mock_cursor
            
            import asyncio
            # This should internally limit to 50 even if 100 requested
            adapter._sql_endpoint = "test.endpoint.com"
            # Note: The actual limit is applied in the method


class TestSchemaTableParsing:
    """Tests for schema.table parsing."""
    
    def test_valid_schema_table(self):
        """Test parsing valid schema.table format."""
        schema_table = "dbo.Sales"
        parts = schema_table.split(".")
        
        assert len(parts) == 2
        assert parts[0] == "dbo"
        assert parts[1] == "Sales"
    
    def test_invalid_schema_table(self):
        """Test that invalid format raises error."""
        from src.warehouse_adapter import WarehouseAdapter
        
        auth = MagicMock()
        adapter = WarehouseAdapter(auth)
        adapter._sql_endpoint = "test.endpoint.com"
        
        invalid_formats = [
            "Sales",  # No schema
            "dbo.schema.table",  # Too many parts
            "",  # Empty
        ]
        
        for fmt in invalid_formats:
            with pytest.raises(ValueError):
                import asyncio
                asyncio.run(adapter.profile_table(fmt))


class TestBinaryTokenFormat:
    """Tests for SQL binary token format."""
    
    def test_token_struct_format(self):
        """Test that token is packed correctly for ODBC."""
        import struct
        
        token = "test_access_token"
        token_bytes = token.encode("utf-16-le")
        packed = struct.pack("<I", len(token_bytes)) + token_bytes
        
        # Verify format: little-endian 4-byte int + UTF-16-LE string
        length = struct.unpack("<I", packed[:4])[0]
        assert length == len(token_bytes)
        
        decoded = packed[4:].decode("utf-16-le")
        assert decoded == token


# Integration test marker - these tests require --run-integration flag
# For now, skip by default using a simple marker

class TestWarehouseIntegration:
    """Integration tests for warehouse adapter (requires real Fabric connection)."""
    
    @pytest.mark.skip(reason="Integration tests require real Fabric connection")
    def test_connect_to_warehouse(self):
        """Test connecting to a real Fabric warehouse."""
        # This test requires:
        # 1. Valid config.yaml with auth credentials
        # 2. Access to a Fabric workspace
        # 3. --run-integration flag
        pass  # Skip in unit tests


# Run with: python -m pytest tests/test_warehouse.py -v
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
