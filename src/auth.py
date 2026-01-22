"""
Authentication Module for Fabric AI Gateway

Handles Microsoft Entra ID (Azure AD) authentication using MSAL.
Supports both Power BI API and Fabric SQL endpoints with automatic token refresh.
"""

import os
import sys
import atexit
import struct
from pathlib import Path
from typing import Optional, Literal

import msal

# Scopes for different Fabric services
POWER_BI_SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
SQL_SCOPE = ["https://database.windows.net/.default"]

# Default cache location
DEFAULT_CACHE_PATH = Path.home() / ".fabric-gateway" / "token_cache.bin"


class FabricAuthManager:
    """
    Manages authentication to Microsoft Fabric services.
    
    Supports:
    - Interactive browser authentication
    - Device code flow (fallback)
    - Persistent token cache with auto-refresh
    """
    
    def __init__(
        self,
        client_id: str,
        tenant_id: str = "common",
        cache_path: Optional[Path] = None
    ):
        """
        Initialize the authentication manager.
        
        Args:
            client_id: Azure AD Application (client) ID
            tenant_id: Azure AD Tenant ID (or 'common' for multi-tenant)
            cache_path: Path to token cache file (default: ~/.fabric-gateway/token_cache.bin)
        """
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.cache_path = Path(cache_path) if cache_path else DEFAULT_CACHE_PATH
        
        # Ensure cache directory exists
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize token cache
        self._cache = msal.SerializableTokenCache()
        self._load_cache()
        
        # Register cache save on exit
        atexit.register(self._save_cache)
        
        # Initialize MSAL app
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        self._app = msal.PublicClientApplication(
            client_id=client_id,
            authority=authority,
            token_cache=self._cache
        )
        
        # Cache for current tokens
        self._current_account: Optional[dict] = None
    
    def _load_cache(self) -> None:
        """Load token cache from disk if exists."""
        if self.cache_path.exists():
            try:
                self._cache.deserialize(self.cache_path.read_text())
            except Exception as e:
                print(f"Warning: Could not load token cache: {e}")
    
    def _save_cache(self) -> None:
        """Save token cache to disk if modified."""
        if self._cache.has_state_changed:
            try:
                self.cache_path.write_text(self._cache.serialize())
            except Exception as e:
                print(f"Warning: Could not save token cache: {e}")
    
    def get_accounts(self) -> list[dict]:
        """Get all cached accounts."""
        return self._app.get_accounts()
    
    def _get_scope_for_service(
        self, 
        service: Literal["powerbi", "sql"]
    ) -> list[str]:
        """Get the appropriate scope for the specified service."""
        if service == "powerbi":
            return POWER_BI_SCOPE
        elif service == "sql":
            return SQL_SCOPE
        else:
            raise ValueError(f"Unknown service: {service}")
    
    def get_token(
        self,
        service: Literal["powerbi", "sql"] = "powerbi",
        force_refresh: bool = False
    ) -> Optional[str]:
        """
        Get an access token for the specified service.
        
        Tries in order:
        1. Silent token acquisition (from cache)
        2. Interactive browser authentication
        3. Device code flow (if browser fails)
        
        Args:
            service: Target service ('powerbi' or 'sql')
            force_refresh: Force interactive authentication
            
        Returns:
            Access token string or None if authentication fails
        """
        scopes = self._get_scope_for_service(service)
        
        # Try silent acquisition first (from cache)
        if not force_refresh:
            accounts = self.get_accounts()
            if accounts:
                # Use first account (could be improved to let user choose)
                account = accounts[0]
                result = self._app.acquire_token_silent(scopes, account=account)
                if result and "access_token" in result:
                    self._current_account = account
                    return result["access_token"]
        
        # Try interactive browser authentication
        try:
            result = self._app.acquire_token_interactive(
                scopes=scopes,
                prompt="select_account" if force_refresh else None
            )
            if result and "access_token" in result:
                # Update current account
                accounts = self.get_accounts()
                if accounts:
                    self._current_account = accounts[0]
                self._save_cache()
                return result["access_token"]
            elif "error" in result:
                print(f"Authentication error: {result.get('error_description', result['error'])}")
        except Exception as e:
            print(f"Browser authentication failed: {e}")
            print("Falling back to device code flow...")
        
        # Fallback to device code flow
        return self._device_code_flow(scopes)
    
    def _device_code_flow(self, scopes: list[str]) -> Optional[str]:
        """
        Authenticate using device code flow.
        
        This is useful when browser-based auth is not available.
        """
        flow = self._app.initiate_device_flow(scopes=scopes)
        
        if "error" in flow:
            print(f"Device code flow error: {flow.get('error_description', flow['error'])}")
            return None
        
        # Display the device code message to user
        print(f"\n{flow['message']}\n")
        
        # Wait for user to complete authentication
        result = self._app.acquire_token_by_device_flow(flow)
        
        if result and "access_token" in result:
            accounts = self.get_accounts()
            if accounts:
                self._current_account = accounts[0]
            self._save_cache()
            return result["access_token"]
        elif "error" in result:
            print(f"Authentication error: {result.get('error_description', result['error'])}")
        
        return None
    
    def get_current_user(self) -> Optional[str]:
        """Get the username of the currently authenticated user."""
        if self._current_account:
            return self._current_account.get("username")
        
        accounts = self.get_accounts()
        if accounts:
            self._current_account = accounts[0]
            return self._current_account.get("username")
        
        return None
    
    def is_authenticated(self) -> bool:
        """Check if there's a valid cached token."""
        accounts = self.get_accounts()
        return len(accounts) > 0
    
    def logout(self) -> None:
        """Clear all cached tokens."""
        for account in self.get_accounts():
            self._app.remove_account(account)
        
        # Delete cache file
        if self.cache_path.exists():
            self.cache_path.unlink()
        
        self._current_account = None
        print("Logged out successfully.")
    
    def get_sql_token_bytes(self) -> Optional[bytes]:
        """
        Get SQL access token in binary format for pyodbc.
        
        This is required for AAD token authentication with ODBC Driver 18.
        The token must be encoded as UTF-16-LE with a length prefix.
        
        Returns:
            Binary token suitable for SQL_COPT_SS_ACCESS_TOKEN attribute
        """
        token = self.get_token(service="sql")
        if not token:
            return None
        
        # Pack token for ODBC: 4-byte length prefix + UTF-16-LE encoded token
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack("<I", len(token_bytes)) + token_bytes
        
        return token_struct


def load_config() -> dict:
    """
    Load configuration from config.yaml file.
    
    Searches in order:
    1. Current directory
    2. Project root
    3. ~/.fabric-gateway/
    """
    import yaml
    
    search_paths = [
        Path.cwd() / "config.yaml",
        Path(__file__).parent.parent / "config.yaml",
        Path.home() / ".fabric-gateway" / "config.yaml"
    ]
    
    for path in search_paths:
        if path.exists():
            with open(path) as f:
                return yaml.safe_load(f)
    
    raise FileNotFoundError(
        "config.yaml not found. Please create one from config.yaml.template"
    )


def get_auth_manager(config: Optional[dict] = None) -> FabricAuthManager:
    """
    Create an FabricAuthManager instance from config.
    
    Args:
        config: Configuration dict (loaded from config.yaml if not provided)
        
    Returns:
        Configured FabricAuthManager instance
    """
    if config is None:
        config = load_config()
    
    auth_config = config.get("auth", {})
    paths_config = config.get("paths", {})
    
    client_id = auth_config.get("client_id")
    tenant_id = auth_config.get("tenant_id", "common")
    cache_path = paths_config.get("token_cache")
    
    if not client_id:
        raise ValueError("client_id is required in config.yaml")
    
    # Expand ~ in cache path
    if cache_path:
        cache_path = Path(cache_path).expanduser()
    
    return FabricAuthManager(
        client_id=client_id,
        tenant_id=tenant_id,
        cache_path=cache_path
    )


# CLI entry point for testing auth
if __name__ == "__main__":
    from rich.console import Console
    from rich.panel import Panel
    
    console = Console()
    
    console.print(Panel.fit(
        "[bold blue]Fabric AI Gateway - Authentication Test[/bold blue]"
    ))
    
    try:
        config = load_config()
        auth = get_auth_manager(config)
        
        console.print("\n[yellow]Authenticating to Power BI...[/yellow]")
        token = auth.get_token(service="powerbi")
        
        if token:
            user = auth.get_current_user()
            console.print(f"\n[green]✓ Authenticated as:[/green] {user}")
            console.print(f"[dim]Token length: {len(token)} chars[/dim]")
            
            # Test SQL token
            console.print("\n[yellow]Getting SQL token...[/yellow]")
            sql_token = auth.get_sql_token_bytes()
            if sql_token:
                console.print(f"[green]✓ SQL token ready[/green] ({len(sql_token)} bytes)")
        else:
            console.print("[red]✗ Authentication failed[/red]")
            sys.exit(1)
            
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        console.print("\n[yellow]Hint:[/yellow] Copy config.yaml.template to config.yaml and fill in your values.")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)
