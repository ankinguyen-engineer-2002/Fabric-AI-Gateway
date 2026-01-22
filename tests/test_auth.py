"""
Unit tests for the authentication module.
"""

import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import struct


class TestFabricAuthManager:
    """Tests for FabricAuthManager class."""
    
    @pytest.fixture
    def mock_msal_app(self):
        """Create a mock MSAL application."""
        with patch('src.auth.msal.PublicClientApplication') as mock:
            yield mock
    
    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary directory for token cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_init_creates_cache_directory(self, mock_msal_app, temp_cache_dir):
        """Test that init creates the cache directory if it doesn't exist."""
        from src.auth import FabricAuthManager
        
        cache_path = temp_cache_dir / "subdir" / "token_cache.bin"
        
        auth = FabricAuthManager(
            client_id="test-client-id",
            tenant_id="test-tenant",
            cache_path=cache_path
        )
        
        assert cache_path.parent.exists()
    
    def test_get_sql_token_bytes_format(self, mock_msal_app, temp_cache_dir):
        """Test that SQL token is correctly formatted for pyodbc."""
        from src.auth import FabricAuthManager
        
        # Mock the token acquisition
        mock_app_instance = MagicMock()
        mock_msal_app.return_value = mock_app_instance
        
        mock_app_instance.get_accounts.return_value = [{"username": "test@test.com"}]
        mock_app_instance.acquire_token_silent.return_value = {
            "access_token": "test_token_123"
        }
        
        auth = FabricAuthManager(
            client_id="test-client-id",
            tenant_id="test-tenant",
            cache_path=temp_cache_dir / "cache.bin"
        )
        
        token_bytes = auth.get_sql_token_bytes()
        
        assert token_bytes is not None
        
        # Verify format: 4-byte length prefix + UTF-16-LE encoded token
        expected_token = "test_token_123"
        expected_bytes = expected_token.encode("utf-16-le")
        expected_struct = struct.pack("<I", len(expected_bytes)) + expected_bytes
        
        assert token_bytes == expected_struct
    
    def test_is_authenticated_with_cached_account(self, mock_msal_app, temp_cache_dir):
        """Test is_authenticated returns True when accounts exist."""
        from src.auth import FabricAuthManager
        
        mock_app_instance = MagicMock()
        mock_msal_app.return_value = mock_app_instance
        mock_app_instance.get_accounts.return_value = [{"username": "test@test.com"}]
        
        auth = FabricAuthManager(
            client_id="test-client-id",
            cache_path=temp_cache_dir / "cache.bin"
        )
        
        assert auth.is_authenticated() is True
    
    def test_is_authenticated_without_cached_account(self, mock_msal_app, temp_cache_dir):
        """Test is_authenticated returns False when no accounts."""
        from src.auth import FabricAuthManager
        
        mock_app_instance = MagicMock()
        mock_msal_app.return_value = mock_app_instance
        mock_app_instance.get_accounts.return_value = []
        
        auth = FabricAuthManager(
            client_id="test-client-id",
            cache_path=temp_cache_dir / "cache.bin"
        )
        
        assert auth.is_authenticated() is False
    
    def test_get_current_user(self, mock_msal_app, temp_cache_dir):
        """Test getting current username."""
        from src.auth import FabricAuthManager
        
        mock_app_instance = MagicMock()
        mock_msal_app.return_value = mock_app_instance
        mock_app_instance.get_accounts.return_value = [{"username": "test@domain.com"}]
        
        auth = FabricAuthManager(
            client_id="test-client-id",
            cache_path=temp_cache_dir / "cache.bin"
        )
        
        assert auth.get_current_user() == "test@domain.com"


class TestTokenScopes:
    """Tests for token scope configuration."""
    
    def test_powerbi_scope(self):
        """Test Power BI scope is correct."""
        from src.auth import POWER_BI_SCOPE
        assert POWER_BI_SCOPE == ["https://analysis.windows.net/powerbi/api/.default"]
    
    def test_sql_scope(self):
        """Test SQL scope is correct."""
        from src.auth import SQL_SCOPE
        assert SQL_SCOPE == ["https://database.windows.net/.default"]


class TestLoadConfig:
    """Tests for configuration loading."""
    
    def test_load_config_file_not_found(self):
        """Test that FileNotFoundError is raised when config doesn't exist."""
        from src.auth import load_config
        
        with patch('pathlib.Path.exists', return_value=False):
            with pytest.raises(FileNotFoundError):
                load_config()
    
    def test_load_config_from_file(self, tmp_path):
        """Test loading config from a valid YAML file."""
        from src.auth import load_config
        
        config_content = """
auth:
  client_id: "test-id"
  tenant_id: "test-tenant"
limits:
  max_dax_rows: 500
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)
        
        with patch('pathlib.Path.cwd', return_value=tmp_path):
            config = load_config()
        
        assert config["auth"]["client_id"] == "test-id"
        assert config["limits"]["max_dax_rows"] == 500


# Run with: python -m pytest tests/test_auth.py -v
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
