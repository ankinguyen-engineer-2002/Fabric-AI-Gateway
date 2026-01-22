#!/usr/bin/env python3
"""
Standalone CLI for Fabric AI Gateway
Runs without MCP server dependency for direct terminal usage.
"""

import asyncio
import sys
import json
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, Confirm
from rich.table import Table

from auth import get_auth_manager, load_config, FabricAuthManager
from context_manager import get_context, ConnectionMode

console = Console()

def display_banner():
    """Display application banner."""
    console.print(Panel.fit(
        "[bold blue]🚀 Fabric AI Gateway[/bold blue]\n"
        "[dim]Standalone CLI for Microsoft Fabric Cloud[/dim]",
        border_style="blue"
    ))

def check_authentication(auth: FabricAuthManager) -> bool:
    """Check and perform authentication if needed."""
    console.print("\n[yellow]🔐 Checking authentication...[/yellow]")
    
    if auth.is_authenticated():
        user = auth.get_current_user()
        console.print(f"[green]✓ Authenticated as:[/green] {user}")
        return True
    
    console.print("[dim]No cached credentials. Starting authentication...[/dim]")
    token = auth.get_token(service="powerbi")
    
    if token:
        user = auth.get_current_user()
        console.print(f"[green]✓ Authenticated as:[/green] {user}")
        return True
    else:
        console.print("[red]✗ Authentication failed[/red]")
        return False

def select_mode() -> int:
    """Display mode selection menu."""
    console.print("\n[bold]Select Mode:[/bold]")
    
    table = Table(show_header=False, box=None)
    table.add_row("[cyan][1][/cyan]", "Semantic Model (Power BI Cloud)")
    table.add_row("[cyan][2][/cyan]", "Data Warehouse Analytics")
    table.add_row("[cyan][0][/cyan]", "Exit")
    console.print(table)
    
    choice = IntPrompt.ask("\nEnter choice", choices=["0", "1", "2"], default="1")
    return choice

async def semantic_model_mode(auth: FabricAuthManager, workspace_id=None, model_id=None, model_name=None):
    """Semantic Model interactive mode."""
    import semantic_adapter
    SemanticModelAdapter = semantic_adapter.SemanticModelAdapter
    
    console.print("\n[bold cyan]📊 Semantic Model Mode[/bold cyan]")
    
    adapter = SemanticModelAdapter(auth)
    
    selected_workspace = None
    selected_model = None

    if workspace_id and model_id:
         # Simplified flow if IDs are provided
         console.print(f"[yellow]Connecting to saved context: {model_name or model_id}...[/yellow]")
         # We need to minimally init the adapter's state if needed, but the adapter 
         # usually takes IDs in method calls. 
         # We'll just set local variables for display.
         selected_workspace = {"id": workspace_id, "name": "Context Workspace"}
         selected_model = {"id": model_id, "name": model_name or "Context Model"}
         
         # Optionally verify connection/listing?
         # For speed, we assume valid and just connect.
    else:

        # List workspaces
        console.print("\n[yellow]Loading workspaces...[/yellow]")
        try:
            workspaces = await adapter.list_workspaces()
        except Exception as e:
            console.print(f"[red]Error loading workspaces:[/red] {e}")
            return
        
        if not workspaces:
            console.print("[red]No workspaces found[/red]")
            return
        
        # Display workspaces
        console.print("\n[bold]Select Workspace:[/bold]")
        for i, ws in enumerate(workspaces[:20], 1):
            console.print(f"  [cyan][{i}][/cyan] {ws['name']}")
        
        ws_choice = IntPrompt.ask("Enter workspace number", default=1)
        
        if ws_choice < 1 or ws_choice > len(workspaces):
            console.print("[red]Invalid choice[/red]")
            return
        
        selected_workspace = workspaces[ws_choice - 1]
        
        # List models
        console.print(f"\n[yellow]Loading models in '{selected_workspace['name']}'...[/yellow]")
        try:
            models = await adapter.list_models(selected_workspace["id"])
        except Exception as e:
            console.print(f"[red]Error loading models:[/red] {e}")
            return
        
        if not models:
            console.print("[red]No semantic models found in this workspace[/red]")
            return
        
        console.print("\n[bold]Select Semantic Model:[/bold]")
        for i, model in enumerate(models[:20], 1):
            console.print(f"  [cyan][{i}][/cyan] {model['name']}")
        
        model_choice = IntPrompt.ask("Enter model number", default=1)
        
        if model_choice < 1 or model_choice > len(models):
            console.print("[red]Invalid choice[/red]")
            return
        
        selected_model = models[model_choice - 1]
    
    # Connect
    console.print(f"\n[green]✓ Connected to:[/green] {selected_workspace['name']} / {selected_model['name']}")
    
    # Interactive loop
    console.print("\n[bold]Commands:[/bold]")
    console.print("  [cyan]dax[/cyan] <query>  - Execute DAX query")
    console.print("  [cyan]schema[/cyan]       - Show schema")
    console.print("  [cyan]exit[/cyan]         - Exit")
    
    while True:
        cmd = Prompt.ask("\n[bold blue]semantic>[/bold blue]")
        
        if cmd.lower() == "exit":
            break
        elif cmd.lower() == "schema":
            try:
                # We need XMLA for full schema, but we can try simplified if needed
                # For now using adapter methods if available or placeholders
                # Check adapter capabilities
                console.print("[yellow]Fetching schema...[/yellow]")
                # Ideally adapter should have get_schema
                # For now we use the placeholder message logic or call execute_dax
                pass
            except:
                pass
                
            console.print("[yellow]Schema discovery requires XMLA endpoint connection...[/yellow]")
            console.print(f"[dim]Workspace: {selected_workspace['name']}[/dim]")
            console.print(f"[dim]Model: {selected_model['name']}[/dim]")
            
        elif cmd.lower().startswith("dax "):
            query = cmd[4:].strip()
            console.print(f"[yellow]Executing DAX: {query}[/yellow]")
            try:
                # Need to implement execute_dax in adapter if not present
                # Using direct API call via adapter if possible
                result = await adapter.execute_dax(selected_workspace["id"], selected_model["id"], query)
                console.print(result)
            except Exception as e:
                console.print(f"[red]Error:[/red] {e}")
        else:
            console.print("[red]Unknown command. Use: dax, schema, exit[/red]")


async def warehouse_mode(auth: FabricAuthManager, sql_endpoint=None):
    """Data Warehouse interactive mode."""
    import warehouse_adapter
    WarehouseAdapter = warehouse_adapter.WarehouseAdapter
    
    console.print("\n[bold cyan]🗄️ Data Warehouse Mode[/bold cyan]")
    
    if not sql_endpoint:
        # Get SQL endpoint
        sql_endpoint = Prompt.ask(
            "\nEnter SQL endpoint",
            default=""
        )
    
    if not sql_endpoint:
        console.print("[red]SQL endpoint is required[/red]")
        return
    
    adapter = WarehouseAdapter(auth)
    
    console.print(f"\n[yellow]Connecting to {sql_endpoint}...[/yellow]")
    
    try:
        await adapter.connect(sql_endpoint)
        console.print(f"[green]✓ Connected to:[/green] {sql_endpoint}")
    except Exception as e:
        console.print(f"[red]Connection failed:[/red] {e}")
        return
    
    # Interactive loop
    console.print("\n[bold]Commands:[/bold]")
    console.print("  [cyan]overview[/cyan]            - Show warehouse overview")
    console.print("  [cyan]profile[/cyan] <table>     - Profile a table (e.g., dbo.Sales)")
    console.print("  [cyan]sample[/cyan] <table> [n]  - Sample rows from table")
    console.print("  [cyan]sql[/cyan] <query>         - Execute SQL SELECT")
    console.print("  [cyan]exit[/cyan]                - Exit")
    
    while True:
        cmd = Prompt.ask("\n[bold green]warehouse>[/bold green]")
        
        if cmd.lower() == "exit":
            adapter.close()
            break
        elif cmd.lower() == "overview":
            try:
                console.print("[yellow]Scanning warehouse...[/yellow]")
                result = await adapter.get_overview()
                
                console.print(f"\n[bold]Schemas:[/bold] {', '.join(result['schemas'])}")
                console.print(f"[bold]Tables:[/bold] {result['table_count']}")
                
                table = Table(title="Tables Overview")
                table.add_column("Schema", style="cyan")
                table.add_column("Table", style="green")
                table.add_column("Rows", justify="right")
                
                for t in result['tables'][:20]:
                    table.add_row(t['schema'], t['name'], str(t['row_count']))
                
                console.print(table)
            except Exception as e:
                console.print(f"[red]Error:[/red] {e}")
                
        elif cmd.lower().startswith("profile "):
            table_name = cmd[8:].strip()
            try:
                console.print(f"[yellow]Profiling {table_name}...[/yellow]")
                result = await adapter.profile_table(table_name)
                
                table = Table(title=f"Profile: {table_name}")
                table.add_column("Column", style="cyan")
                table.add_column("Type", style="green")
                table.add_column("Nullable")
                table.add_column("Null %", justify="right")
                table.add_column("Distinct", justify="right")
                
                for col in result['columns']:
                    null_ratio = col.get('null_ratio', '-')
                    if isinstance(null_ratio, float):
                        null_ratio = f"{null_ratio*100:.1f}%"
                    table.add_row(
                        col['name'],
                        col['type'],
                        "Yes" if col['nullable'] else "No",
                        str(null_ratio),
                        str(col.get('distinct_count', '-'))
                    )
                
                console.print(table)
            except Exception as e:
                console.print(f"[red]Error:[/red] {e}")
                
        elif cmd.lower().startswith("sample "):
            parts = cmd[7:].strip().split()
            table_name = parts[0]
            n = int(parts[1]) if len(parts) > 1 else 10
            
            try:
                console.print(f"[yellow]Sampling {n} rows from {table_name}...[/yellow]")
                result = await adapter.sample_rows(table_name, n)
                console.print(result)
            except Exception as e:
                console.print(f"[red]Error:[/red] {e}")
                
        elif cmd.lower().startswith("sql "):
            query = cmd[4:].strip()
            try:
                console.print(f"[yellow]Executing SQL...[/yellow]")
                result = await adapter.execute_sql(query)
                console.print(result)
            except Exception as e:
                console.print(f"[red]Error:[/red] {e}")
        else:
            console.print("[red]Unknown command. Use: overview, profile, sample, sql, exit[/red]")


async def main():
    """Main entry point."""
    display_banner()
    
    # Load config and authenticate
    try:
        config = load_config()
    except FileNotFoundError:
        console.print("[red]Error: config.yaml not found[/red]")
        return
    
    auth = get_auth_manager(config)
    
    if not check_authentication(auth):
        return
    
    # Check for persisted context
    context_path = os.path.expanduser("~/.fabric-gateway/context.json")
    auto_mode = None
    
    if os.path.exists(context_path):
        try:
            with open(context_path) as f:
                ctx = json.load(f)
                
            mode_name = ctx.get("mode")
            target_name = ctx.get("item_name", "Unknown")
            
            if mode_name and Confirm.ask(f"[bold]Resume session?[/bold] ({mode_name.title()} - {target_name})", default=True):
                if mode_name == "semantic":
                    await semantic_model_mode(
                        auth, 
                        workspace_id=ctx.get("workspace_id"), 
                        model_id=ctx.get("item_id"),
                        model_name=target_name
                    )
                    # If we exit the mode, we might want to exit the app or go to menu
                    # For now, let's go to menu on exit
                elif mode_name == "warehouse":
                    await warehouse_mode(
                        auth, 
                        sql_endpoint=ctx.get("sql_endpoint")
                    )
            else:
                pass # Fall through to menu
                
        except Exception as e:
            console.print(f"[dim]Failed to load context: {e}[/dim]")

    # Main loop
    while True:
        mode = select_mode()
        
        if mode == 0:
            console.print("\n[dim]Goodbye![/dim]")
            break
        elif mode == 1:
            await semantic_model_mode(auth)
        elif mode == 2:
            await warehouse_mode(auth)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted. Goodbye![/dim]")
