"""
Unit tests for the semantic adapter and TMSL generator.
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock


class TestTMSLGenerator:
    """Tests for TMSL JSON generation."""
    
    def test_generate_measure_upsert_basic(self):
        """Test basic measure upsert TMSL generation."""
        from src.utils.tmsl_generator import generate_measure_upsert
        
        tmsl = generate_measure_upsert(
            name="Total Sales",
            formula="SUM(Sales[Amount])",
            table="Sales"
        )
        
        parsed = json.loads(tmsl)
        
        assert "createOrReplace" in parsed
        assert parsed["createOrReplace"]["object"]["table"] == "Sales"
        assert parsed["createOrReplace"]["measure"]["name"] == "Total Sales"
        assert parsed["createOrReplace"]["measure"]["expression"] == "SUM(Sales[Amount])"
    
    def test_generate_measure_upsert_with_options(self):
        """Test measure upsert with all options."""
        from src.utils.tmsl_generator import generate_measure_upsert
        
        tmsl = generate_measure_upsert(
            name="Sales %",
            formula="DIVIDE([Total Sales], [Total Revenue])",
            table="Metrics",
            description="Percentage of sales vs revenue",
            format_string="0.00%",
            display_folder="KPIs"
        )
        
        parsed = json.loads(tmsl)
        measure = parsed["createOrReplace"]["measure"]
        
        assert measure["description"] == "Percentage of sales vs revenue"
        assert measure["formatString"] == "0.00%"
        assert measure["displayFolder"] == "KPIs"
    
    def test_generate_measure_delete(self):
        """Test measure delete TMSL generation."""
        from src.utils.tmsl_generator import generate_measure_delete
        
        tmsl = generate_measure_delete(name="Old Measure", table="Sales")
        parsed = json.loads(tmsl)
        
        assert "delete" in parsed
        assert parsed["delete"]["object"]["measure"] == "Old Measure"
        assert parsed["delete"]["object"]["table"] == "Sales"
    
    def test_validate_dax_balanced_parentheses(self):
        """Test DAX validation catches unbalanced parentheses."""
        from src.utils.tmsl_generator import validate_dax_expression
        
        valid, msg = validate_dax_expression("SUM(Sales[Amount])")
        assert valid is True
        
        valid, msg = validate_dax_expression("SUM(Sales[Amount]")
        assert valid is False
        assert "parentheses" in msg.lower()
    
    def test_validate_dax_balanced_brackets(self):
        """Test DAX validation catches unbalanced brackets."""
        from src.utils.tmsl_generator import validate_dax_expression
        
        valid, msg = validate_dax_expression("[Total Sales]")
        assert valid is True
        
        valid, msg = validate_dax_expression("[Total Sales")
        assert valid is False
        assert "brackets" in msg.lower()
    
    def test_validate_dax_empty(self):
        """Test DAX validation rejects empty formulas."""
        from src.utils.tmsl_generator import validate_dax_expression
        
        valid, msg = validate_dax_expression("")
        assert valid is False
        
        valid, msg = validate_dax_expression("   ")
        assert valid is False


class TestSemanticAdapter:
    """Tests for SemanticModelAdapter class."""
    
    @pytest.fixture
    def mock_auth(self):
        """Create a mock auth manager."""
        auth = MagicMock()
        auth.get_token.return_value = "test_token_123"
        return auth
    
    @pytest.fixture
    def mock_requests(self):
        """Mock requests module."""
        with patch('src.semantic_adapter.requests') as mock:
            yield mock
    
    def test_list_workspaces_success(self, mock_auth, mock_requests):
        """Test listing workspaces successfully."""
        from src.semantic_adapter import SemanticModelAdapter
        
        # Mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [
                {"id": "ws-1", "name": "Workspace 1", "type": "Workspace"},
                {"id": "ws-2", "name": "Workspace 2", "type": "Workspace"}
            ]
        }
        mock_requests.get.return_value = mock_response
        
        adapter = SemanticModelAdapter(mock_auth)
        
        import asyncio
        workspaces = asyncio.run(adapter.list_workspaces())
        
        assert len(workspaces) == 2
        assert workspaces[0]["name"] == "Workspace 1"
    
    def test_list_models_success(self, mock_auth, mock_requests):
        """Test listing models in a workspace."""
        from src.semantic_adapter import SemanticModelAdapter
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [
                {"id": "model-1", "name": "Sales Model", "configuredBy": "user@test.com"},
                {"id": "model-2", "name": "Finance Model", "configuredBy": "user@test.com"}
            ]
        }
        mock_requests.get.return_value = mock_response
        
        adapter = SemanticModelAdapter(mock_auth)
        
        import asyncio
        models = asyncio.run(adapter.list_models("workspace-id"))
        
        assert len(models) == 2
        assert models[0]["name"] == "Sales Model"
    
    def test_execute_dax_requires_evaluate(self, mock_auth):
        """Test that DAX queries must start with EVALUATE."""
        from src.semantic_adapter import SemanticModelAdapter
        
        adapter = SemanticModelAdapter(mock_auth)
        adapter._connected_model_id = "test-model"
        adapter._xmla_endpoint = "https://test.endpoint.com"
        
        with pytest.raises(ValueError) as exc_info:
            import asyncio
            asyncio.run(adapter.execute_dax("SELECT * FROM Table"))
        
        assert "EVALUATE" in str(exc_info.value)
    
    def test_connect_not_called_error(self, mock_auth):
        """Test that operations require connect() first."""
        from src.semantic_adapter import SemanticModelAdapter
        
        adapter = SemanticModelAdapter(mock_auth)
        
        with pytest.raises(RuntimeError) as exc_info:
            import asyncio
            asyncio.run(adapter.execute_dax("EVALUATE VALUES(Table)"))
        
        assert "Not connected" in str(exc_info.value)


class TestXMLAClient:
    """Tests for XMLA client."""
    
    def test_normalize_powerbi_endpoint(self):
        """Test converting powerbi:// URL to HTTPS."""
        from src.utils.xmla_client import XMLAClient
        
        auth = MagicMock()
        
        # Test powerbi:// conversion
        client = XMLAClient(
            "powerbi://api.powerbi.com/v1.0/myorg/TestWorkspace",
            auth
        )
        
        assert client.endpoint.startswith("https://")
        assert client.endpoint.endswith("/xmla")
    
    def test_extract_catalog_from_endpoint(self):
        """Test extracting catalog name from endpoint."""
        from src.utils.xmla_client import XMLAClient
        
        auth = MagicMock()
        
        client = XMLAClient(
            "powerbi://api.powerbi.com/v1.0/myorg/MyWorkspace",
            auth
        )
        
        assert client.catalog == "MyWorkspace"


# Run with: python -m pytest tests/test_semantic.py -v
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
