"""
TMSL Generator for Fabric AI Gateway

Generates TMSL (Tabular Model Scripting Language) JSON payloads for:
- Creating/updating measures
- Deleting measures
- Other model modifications (future)
"""

import json
from typing import Optional


def generate_measure_upsert(
    name: str,
    formula: str,
    table: Optional[str] = None,
    description: Optional[str] = None,
    format_string: Optional[str] = None,
    display_folder: Optional[str] = None
) -> str:
    """
    Generate TMSL JSON for creating or replacing a measure.
    
    Uses the 'createOrReplace' command which will:
    - Create the measure if it doesn't exist
    - Replace the measure if it already exists
    
    Args:
        name: Measure name
        formula: DAX expression
        table: Table to add measure to (required if creating new measure)
        description: Optional description
        format_string: Optional format string (e.g., "#,##0.00")
        display_folder: Optional display folder path
        
    Returns:
        TMSL JSON string
    """
    measure_def = {
        "name": name,
        "expression": formula
    }
    
    if description:
        measure_def["description"] = description
    
    if format_string:
        measure_def["formatString"] = format_string
    
    # Smart JSON for Middleware (TOM-based)
    # We use a custom format that FabricClient recognizes and handles via TOM.
    # This bypasses the complexity/incompatibility of raw TMSL for Measures.
    
    payload = {
        "operation": "upsert_measure",
        "database": "<database>", 
        "table": table,
        "measure": measure_def
    }
    
    return json.dumps(payload, indent=2)


def generate_measure_delete(name: str, table: Optional[str] = None) -> str:
    """
    Generate TMSL JSON for deleting a measure.
    
    Args:
        name: Measure name to delete
        table: Table containing the measure (optional, will search if not provided)
        
    Returns:
        TMSL JSON string
    """
    # Smart JSON for Middleware (TOM-based)
    payload = {
        "operation": "delete_measure",
        "database": "<database>",
        "table": table,
        "measure": name
    }
    
    return json.dumps(payload, indent=2)


def generate_measure_create(
    name: str,
    formula: str,
    table: str,
    description: Optional[str] = None,
    format_string: Optional[str] = None
) -> str:
    """
    Generate TMSL JSON for creating a new measure (only if it doesn't exist).
    
    Args:
        name: Measure name
        formula: DAX expression
        table: Table to add measure to (required)
        description: Optional description
        format_string: Optional format string
        
    Returns:
        TMSL JSON string
    """
    measure_def = {
        "name": name,
        "expression": formula
    }
    
    if description:
        measure_def["description"] = description
    
    if format_string:
        measure_def["formatString"] = format_string
    
    tmsl = {
        "create": {
            "parentObject": {
                "database": "<database>",
                "table": table
            },
            "measure": measure_def
        }
    }
    
    return json.dumps(tmsl, indent=2)


def generate_refresh(
    database: str,
    table: Optional[str] = None,
    refresh_type: str = "full"
) -> str:
    """
    Generate TMSL JSON for refreshing data.
    
    Args:
        database: Database/model name
        table: Specific table to refresh (optional, refreshes all if not specified)
        refresh_type: Type of refresh (full, automatic, calculate, dataOnly, etc.)
        
    Returns:
        TMSL JSON string
    """
    if table:
        target = {
            "database": database,
            "table": table
        }
    else:
        target = {
            "database": database
        }
    
    tmsl = {
        "refresh": {
            "type": refresh_type,
            "objects": [target]
        }
    }
    
    return json.dumps(tmsl, indent=2)


def validate_dax_expression(formula: str) -> tuple[bool, str]:
    """
    Basic validation of DAX expression syntax.
    
    This is a simple check - full validation requires XMLA execution.
    
    Args:
        formula: DAX expression to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    formula = formula.strip()
    
    if not formula:
        return False, "Formula cannot be empty"
    
    # Check for balanced parentheses
    open_parens = formula.count("(")
    close_parens = formula.count(")")
    if open_parens != close_parens:
        return False, f"Unbalanced parentheses: {open_parens} opening, {close_parens} closing"
    
    # Check for balanced brackets
    open_brackets = formula.count("[")
    close_brackets = formula.count("]")
    if open_brackets != close_brackets:
        return False, f"Unbalanced brackets: {open_brackets} opening, {close_brackets} closing"
    
    # Check for common typos
    if formula.endswith(","):
        return False, "Formula ends with trailing comma"
    
    if formula.endswith("("):
        return False, "Formula ends with unclosed function call"
    
    return True, "Basic validation passed"
