#!/usr/bin/env python3
"""
Fix type mismatches in the Slurm OpenAPI spec.

The Slurm OpenAPI specification has several fields defined as strings that
actually contain numeric values (Unix timestamps, integers, etc.). This script
fixes these type mismatches to match the actual API behavior.

Usage:
    ./scripts/fix-slurm-openapi-types.py [--input FILE] [--output FILE]
"""

import argparse
import json
import sys
from pathlib import Path


# Fields that are documented as strings but return numbers (Unix timestamps)
TIMESTAMP_FIELDS = [
    "accrue_time",
    "eligible_time",
    "end_time",
    "last_sched_evaluation",
    "pre_sus_time",
    "preempt_time",
    "resize_time",
    "start_time",
    "submit_time",
    "suspend_time",
    "deadline",
    "delay_boot",
    "time_limit",
    "time_minimum",
]

# Fields that are documented as strings but return integers
INTEGER_FIELDS = [
    "job_id",
    "array_job_id",
    "array_task_id",
    "array_max_tasks",
    "association_id",
    "derived_exit_code",
    "exit_code",
    "group_id",
    "het_job_id",
    "het_job_offset",
    "max_cpus",
    "max_nodes",
    "min_cpus",
    "minimum_cpus_per_node",
    "minimum_tmp_disk_per_node",
    "node_count",
    "tasks",
    "tasks_per_node",
    "tasks_per_board",
    "priority",
    "profile",
    "reboot",
    "requeue",
    "restart_cnt",
    "shared",
    "cpus",
    "sockets_per_board",
    "user_id",
]

# Fields that are documented as strings but return floating point numbers
FLOAT_FIELDS = [
    "billable_tres",
]


def fix_property_type(prop_name: str, prop_def: dict) -> bool:
    """
    Fix the type of a property if it's a known mismatch.
    Returns True if a fix was applied.
    """
    if prop_def.get("type") != "string":
        return False
    
    if prop_name in TIMESTAMP_FIELDS:
        prop_def["type"] = "integer"
        prop_def["format"] = "int64"
        if "description" in prop_def:
            prop_def["description"] += " (Unix timestamp)"
        return True
    
    if prop_name in INTEGER_FIELDS:
        prop_def["type"] = "integer"
        return True
    
    if prop_name in FLOAT_FIELDS:
        prop_def["type"] = "number"
        prop_def["format"] = "double"
        return True
    
    return False


# Schemas where certain array fields should be converted to maps (object with additionalProperties)
# because Slurm API returns objects with string keys like {"0": {...}, "1": {...}}
ARRAY_TO_MAP_FIELDS = {
    "v0.0.36_job_resources": ["allocated_nodes"],
    "v0.0.37_job_resources": ["allocated_nodes"],
}

# Fields to remove because they have complex nested type mismatches that are hard to fix
# and we don't use them in our application
FIELDS_TO_REMOVE = {
    "v0.0.36_job_response_properties": ["job_resources"],
    "v0.0.37_job_response_properties": ["job_resources"],
}


def remove_problematic_fields(schema_name: str, schema_def: dict) -> int:
    """
    Remove fields that have complex type mismatches we don't want to fix.
    Returns the number of fields removed.
    """
    if schema_name not in FIELDS_TO_REMOVE:
        return 0
    
    fields = FIELDS_TO_REMOVE[schema_name]
    properties = schema_def.get("properties", {})
    removed = 0
    
    for field in fields:
        if field in properties:
            del properties[field]
            removed += 1
    
    return removed


def fix_array_to_map(schema_name: str, schema_def: dict) -> int:
    """
    Fix fields that are defined as arrays but should be maps (objects with additionalProperties).
    The Slurm API sometimes returns objects with numeric string keys like {"0": {...}} 
    instead of arrays.
    Returns the number of fixes applied.
    """
    fixes = 0
    if schema_name not in ARRAY_TO_MAP_FIELDS:
        return fixes
    
    fields_to_fix = ARRAY_TO_MAP_FIELDS[schema_name.lower()]
    properties = schema_def.get("properties", {})
    
    for field_name in fields_to_fix:
        if field_name not in properties:
            continue
        prop_def = properties[field_name]
        
        # Only convert if it's currently an array
        if prop_def.get("type") != "array":
            continue
        
        # Get the items schema (what the array contains)
        items_schema = prop_def.get("items", {})
        
        # Convert to object with additionalProperties
        prop_def["type"] = "object"
        prop_def["additionalProperties"] = items_schema
        if "items" in prop_def:
            del prop_def["items"]
        
        if "description" in prop_def:
            prop_def["description"] += " (map with node indices as keys)"
        
        fixes += 1
    
    return fixes


def fix_schema_properties(schema: dict, path: str = "") -> int:
    """
    Recursively fix property types in a schema.
    Returns the number of fixes applied.
    """
    fixes = 0
    
    if not isinstance(schema, dict):
        return fixes
    
    # Fix properties in this schema
    properties = schema.get("properties", {})
    for prop_name, prop_def in properties.items():
        if fix_property_type(prop_name, prop_def):
            fixes += 1
    
    # Recurse into nested schemas
    for key in ["items", "additionalProperties"]:
        if key in schema and isinstance(schema[key], dict):
            fixes += fix_schema_properties(schema[key], f"{path}.{key}")
    
    # Recurse into allOf, anyOf, oneOf
    for key in ["allOf", "anyOf", "oneOf"]:
        if key in schema and isinstance(schema[key], list):
            for i, item in enumerate(schema[key]):
                fixes += fix_schema_properties(item, f"{path}.{key}[{i}]")
    
    return fixes


def fix_openapi_spec(spec: dict) -> int:
    """
    Fix type mismatches in an OpenAPI spec.
    Returns the total number of fixes applied.
    """
    total_fixes = 0
    
    # Fix schemas in components
    components = spec.get("components", {})
    schemas = components.get("schemas", {})
    
    for schema_name, schema_def in schemas.items():
        # Remove problematic fields that have complex nested type issues
        remove_fixes = remove_problematic_fields(schema_name, schema_def)
        if remove_fixes > 0:
            print(f"  Removed {remove_fixes} problematic fields from {schema_name}")
            total_fixes += remove_fixes
        
        # Fix primitive type mismatches
        fixes = fix_schema_properties(schema_def, f"#/components/schemas/{schema_name}")
        if fixes > 0:
            print(f"  Fixed {fixes} fields in {schema_name}")
            total_fixes += fixes
        
        # Fix array-to-map type mismatches
        map_fixes = fix_array_to_map(schema_name, schema_def)
        if map_fixes > 0:
            print(f"  Fixed {map_fixes} array-to-map conversions in {schema_name}")
            total_fixes += map_fixes
    
    return total_fixes


def main():
    parser = argparse.ArgumentParser(
        description="Fix type mismatches in Slurm OpenAPI spec"
    )
    parser.add_argument(
        "--input",
        default="config/openapi/slurm/slurm-openapi-v0.0.37.json",
        help="Input OpenAPI spec file",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output file (default: overwrite input)",
    )
    
    args = parser.parse_args()
    
    if args.output is None:
        args.output = args.input
    
    # Read input spec
    try:
        with open(args.input) as f:
            spec = json.load(f)
    except FileNotFoundError:
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {args.input}: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Apply fixes
    print(f"Fixing type mismatches in {args.input}...")
    total_fixes = fix_openapi_spec(spec)
    
    if total_fixes == 0:
        print("No fixes needed")
        return
    
    # Write output spec
    with open(args.output, "w") as f:
        json.dump(spec, f, indent=2)
    
    print(f"Applied {total_fixes} fixes to {args.output}")
    print("Remember to regenerate the client: go generate ./internal/slurmclient/...")


if __name__ == "__main__":
    main()
