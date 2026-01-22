#!/usr/bin/env python3
"""
MCP Cloud Fabric - Unified CLI
Start here: mcp_cloud_fabric
"""

import sys
import os
import json
import asyncio

src_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, src_dir)
os.chdir(os.path.dirname(src_dir))

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt
from rich.table import Table
import requests

console = Console()

# ============== AUTH ==============
def get_token():
    """Get Power BI token."""
    try:
        from src.auth import get_auth_manager, load_config
        config = load_config()
        if not config:
            console.print("[red]config.yaml not found[/red]")
            return None
        auth = get_auth_manager(config)
        return auth.get_token(service="powerbi")
    except ImportError:
        console.print("[red]Error: src.auth module not found.[/red]")
        return None

# ============== CLIENT ==============
class FabricClient:
    def __init__(self, token):
        self.token = token
        self.headers = {"Authorization": f"Bearer {token}"}
        self.base = "https://api.powerbi.com/v1.0/myorg"
        
        # Context
        self.mode = None # 'semantic' or 'warehouse'
        self.ws_id = None
        self.ws_name = None
        self.item_id = None # dataset_id or warehouse_id
        self.item_name = None
        self.sql_endpoint = None # for warehouse
    
    def get(self, path):
        r = requests.get(f"{self.base}{path}", headers=self.headers)
        r.raise_for_status()
        return r.json()
    
    def post(self, path, data):
        r = requests.post(f"{self.base}{path}", json=data, headers=self.headers)
        r.raise_for_status()
        return r.json()
    
    def workspaces(self):
        return self.get("/groups").get("value", [])
    
    def datasets(self, ws_id):
        return self.get(f"/groups/{ws_id}/datasets").get("value", [])

    def warehouses(self, ws_id):
        # List warehouses in workspace
        # API: https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items?type=Warehouse
        # Note: Power BI API token might not verify directly against Fabric API endpoint without correct audience
        # We try generic Get Items API if available or fall back to user input
        try:
            # Try Power BI generic items API if available, otherwise return empty
            return [] 
        except:
            return []

    def save_context(self):
        """Save context for MCP server."""
        context_dir = os.path.expanduser("~/.fabric-gateway")
        os.makedirs(context_dir, exist_ok=True)
        path = os.path.join(context_dir, "context.json")
        
        data = {
            "mode": self.mode,
            "workspace_id": self.ws_id,
            "workspace_name": self.ws_name,
            "item_id": self.item_id, # dataset or warehouse id
            "item_name": self.item_name,
            "sql_endpoint": self.sql_endpoint
        }
        
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

# ============== MAIN ==============
def main():
    console.print(Panel.fit(
        "[bold cyan]MCP Cloud Fabric[/bold cyan]",
        subtitle="Unified AI Gateway",
        border_style="cyan"
    ))
    
    # 1. Auth
    console.print("\n[yellow]Authenticating...[/yellow]")
    token = get_token()
    if not token: return
    client = FabricClient(token)
    console.print("[green]✓ Authenticated[/green]")
    
    # 2. Select Workspace
    try:
        ws_list = client.workspaces()
    except Exception as e:
        console.print(f"[red]Failed to list workspaces: {e}[/red]")
        return
        
    console.print("\n[bold]Select Workspace:[/bold]")
    for i, w in enumerate(ws_list[:15], 1):
        console.print(f"  [cyan][{i}][/cyan] {w['name']}")
    
    idx = IntPrompt.ask("\nChoice", default=1) - 1
    client.ws_id = ws_list[idx]["id"]
    client.ws_name = ws_list[idx]["name"]
    
    # 3. Select Mode
    console.print("\n[bold]Select Mode:[/bold]")
    console.print("  [cyan][1][/cyan] Semantic Model (Power BI)")
    console.print("  [cyan][2][/cyan] Data Warehouse (Fabric SQL)")
    
    mode_choice = IntPrompt.ask("\nChoice", choices=["1", "2"], default="1")
    
    if mode_choice == 1:
        client.mode = "semantic"
        console.print(f"\n[green]✓ Semantic Model Mode[/green]")
        console.print(f"  Workspace: {client.ws_name}")
        console.print("\n[dim]AI CLI will list and connect to datasets automatically.[/dim]")
        console.print("[dim]Use commands like: 'list all semantic models' or 'connect to Sales Model'[/dim]")
        # Don't select dataset here - let AI do it via MCP tools
        client.item_id = None
        client.item_name = None
        
    else:
        client.mode = "warehouse"
        console.print(f"\n[cyan]Data Warehouse Mode[/cyan]")
        console.print("Note: Automatic warehouse listing requires Fabric API permissions.")
        console.print("Please enter the [bold]SQL Connection String[/bold] (from Fabric Settings).")
        console.print("Example: xxxxx.datawarehouse.fabric.microsoft.com")
        
        endpoint = Prompt.ask("\nSQL Endpoint")
        client.sql_endpoint = endpoint
        
        console.print("\nPlease enter the [bold]Warehouse Name[/bold] (e.g., 'eCentric Warehouse').")
        console.print("[dim]This must match the exact name in Fabric to ensure correct database connection.[/dim]")
        wh_name = Prompt.ask("Warehouse Name")
        client.item_name = wh_name
    
    # 4. Save Context
    client.save_context()
    console.print(f"\n[green]✓ Context Saved![/green]")
    console.print(f"  Mode: {client.mode}")
    console.print(f"  Target: {client.item_name}")
    
    # 5. Launch CLI
    console.print("\n[bold]Select Interface:[/bold]")
    console.print("  [cyan][1][/cyan] Gemini CLI")
    console.print("  [cyan][2][/cyan] Codex CLI")
    console.print("  [cyan][3][/cyan] Standalone Terminal")
    console.print("  [cyan][0][/cyan] Exit")
    
    cli = IntPrompt.ask("\nChoice", choices=["0", "1", "2", "3"], default="3")
    
    if cli == 0: return
    
    # Update MCP Configs
    project_dir = os.path.dirname(os.path.abspath(__file__))
    mcp_config = {
        "command": f"{project_dir}/venv/bin/python",
        "args": ["-m", "src.mcp_server"],
        "cwd": project_dir,
        "env": {"PYTHONUNBUFFERED": "1"} 
    }
    
    # Update Gemini
    gemini_conf = os.path.expanduser("~/.gemini/settings.json")
    if os.path.exists(os.path.dirname(gemini_conf)):
        try:
            with open(gemini_conf) as f: s = json.load(f)
        except: s = {"mcpServers": {}}
        if "mcpServers" not in s: s["mcpServers"] = {}
        s["mcpServers"]["fabric"] = mcp_config
        with open(gemini_conf, "w") as f: json.dump(s, f, indent=2)
    
    # Update Codex (assuming config.toml mostly static, but ensure enabled)
    # We rely on previous fix for Codex config
    
    if cli == 1:
        console.print("[dim]Launching Gemini...[/dim]")
        os.execvp("gemini", ["gemini"])
    elif cli == 2:
        console.print("[dim]Launching Codex...[/dim]")
        os.execvp("codex", ["codex"])
    elif cli == 3:
        console.print("[dim]Launching Standalone...[/dim]")
        # Pass args to quickstart to auto-load context if we wanted, 
        # but quickstart currently doesn't read context file. 
        # For now just launch it interactively as before but we should update it.
        # Ideally standalone should also read context.json.
        # Launch improved CLI
        os.system(f"{project_dir}/venv/bin/python src/cli.py")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[dim]Bye![/dim]")
