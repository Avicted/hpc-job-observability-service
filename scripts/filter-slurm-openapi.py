#!/usr/bin/env python3
"""
Filter Slurm OpenAPI spec to a single API version.

The Slurm OpenAPI spec from slurmrestd contains multiple API versions
(e.g., v0.0.35, v0.0.36, v0.0.37) which causes duplicate operation ID
conflicts when generating code with oapi-codegen.

This script filters the spec to include only paths for a specific version.

Usage:
    # Filter to v0.0.36 (default)
    ./scripts/filter-slurm-openapi.py

    # Filter to a specific version
    ./scripts/filter-slurm-openapi.py --version v0.0.37

    # Custom input/output paths
    ./scripts/filter-slurm-openapi.py --input spec.json --output filtered.json
"""

import argparse
import json
import sys
from pathlib import Path


def filter_openapi_spec(spec: dict, version: str) -> dict:
    """
    Filter OpenAPI spec paths to only include a specific API version.
    
    Args:
        spec: The full OpenAPI specification dict
        version: The API version to keep (e.g., "v0.0.36")
    
    Returns:
        A new spec dict with only matching paths
    """
    version_pattern = f"/{version}/"
    
    filtered_paths = {}
    for path, operations in spec.get("paths", {}).items():
        # Keep paths that match the version or are version-agnostic (like /openapi/v3)
        if version_pattern in path or path == "/openapi/v3":
            filtered_paths[path] = operations
    
    # Create a copy of the spec with filtered paths
    filtered_spec = spec.copy()
    filtered_spec["paths"] = filtered_paths
    
    return filtered_spec


def main():
    parser = argparse.ArgumentParser(
        description="Filter Slurm OpenAPI spec to a single API version"
    )
    parser.add_argument(
        "--input",
        default="config/openapi/slurm/slurm-openapi.json",
        help="Input OpenAPI spec file (default: config/openapi/slurm/slurm-openapi.json)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output filtered spec file (default: config/openapi/slurm/slurm-openapi-{version}.json)",
    )
    parser.add_argument(
        "--version",
        default="v0.0.37",
        help="API version to keep (default: v0.0.37)",
    )
    
    args = parser.parse_args()
    
    # Determine output path
    if args.output is None:
        input_path = Path(args.input)
        args.output = str(input_path.parent / f"slurm-openapi-{args.version}.json")
    
    # Read input spec
    try:
        with open(args.input) as f:
            spec = json.load(f)
    except FileNotFoundError:
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        print("Make sure to fetch the spec first:", file=sys.stderr)
        print("  curl -s http://localhost:6820/openapi/v3 > config/slurm-openapi.json", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {args.input}: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Filter spec
    filtered_spec = filter_openapi_spec(spec, args.version)
    path_count = len(filtered_spec["paths"])
    
    if path_count == 0:
        print(f"Warning: No paths found for version {args.version}", file=sys.stderr)
        print("Available versions in spec:", file=sys.stderr)
        versions = set()
        for path in spec.get("paths", {}).keys():
            # Extract version from path like /slurm/v0.0.36/jobs
            parts = path.split("/")
            for part in parts:
                if part.startswith("v0."):
                    versions.add(part)
        for v in sorted(versions):
            print(f"  {v}", file=sys.stderr)
        sys.exit(1)
    
    # Write output spec
    with open(args.output, "w") as f:
        json.dump(filtered_spec, f, indent=2)
    
    print(f"Filtered {args.input} -> {args.output}")
    print(f"  Version: {args.version}")
    print(f"  Paths: {path_count}")


if __name__ == "__main__":
    main()
