#!/usr/bin/env python3
"""
Test script to analyze consumer output files and identify duplicate payload processing.
This script examines the consumer_output directory to check for:
1. Duplicate payload numbers across consumers
2. Missing payload numbers
3. Total payload counts per consumer
4. Overall statistics
"""

import os
import re
import json
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple
import argparse

def parse_consumer_output_file(file_path: str) -> List[int]:
    """
    Parse a consumer output file and extract all package numbers.
    
    Args:
        file_path: Path to the consumer output file
        
    Returns:
        List of package numbers found in the file
    """
    package_numbers = []
    
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                # Extract package number using regex
                # Expected format: "Consumer <id> processed package <number> (timestamp: <timestamp>)"
                match = re.search(r'processed package (\d+)', line)
                if match:
                    package_num = int(match.group(1))
                    package_numbers.append(package_num)
                else:
                    print(f"Warning: Could not parse line {line_num} in {file_path}: {line}")
                    
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        
    return package_numbers

def analyze_consumer_outputs(output_dir: str = "consumer_output") -> Dict:
    """
    Analyze all consumer output files in the specified directory.
    
    Args:
        output_dir: Directory containing consumer output files
        
    Returns:
        Dictionary containing analysis results
    """
    if not os.path.exists(output_dir):
        print(f"Error: Output directory '{output_dir}' does not exist")
        return {}
    
    # Find all consumer output files
    consumer_files = []
    for filename in os.listdir(output_dir):
        if filename.startswith("consumer_") and filename.endswith(".txt"):
            consumer_files.append(os.path.join(output_dir, filename))
    
    if not consumer_files:
        print(f"No consumer output files found in '{output_dir}'")
        return {}
    
    print(f"Found {len(consumer_files)} consumer output files")
    
    # Parse each file
    consumer_data = {}
    all_packages = []
    
    for file_path in consumer_files:
        consumer_id = os.path.basename(file_path).replace("consumer_", "").replace(".txt", "")
        packages = parse_consumer_output_file(file_path)
        consumer_data[consumer_id] = packages
        all_packages.extend(packages)
        print(f"Consumer {consumer_id}: {len(packages)} packages")
    
    # Analyze for duplicates
    package_counter = Counter(all_packages)
    duplicates = {pkg: count for pkg, count in package_counter.items() if count > 1}
    
    # Find missing packages (assuming sequential numbering)
    if all_packages:
        min_package = min(all_packages)
        max_package = max(all_packages)
        expected_packages = set(range(min_package, max_package + 1))
        actual_packages = set(all_packages)
        missing_packages = expected_packages - actual_packages
    else:
        missing_packages = set()
        min_package = max_package = 0
    
    # Calculate statistics
    total_processed = len(all_packages)
    unique_packages = len(set(all_packages))
    duplicate_count = total_processed - unique_packages
    
    # Group duplicates by consumer
    duplicate_by_consumer = defaultdict(list)
    for package_num, count in duplicates.items():
        for consumer_id, packages in consumer_data.items():
            if package_num in packages:
                duplicate_by_consumer[consumer_id].append((package_num, count))
    
    return {
        "total_processed": total_processed,
        "unique_packages": unique_packages,
        "duplicate_count": duplicate_count,
        "duplicate_packages": duplicates,
        "missing_packages": sorted(missing_packages),
        "package_range": (min_package, max_package),
        "consumer_data": consumer_data,
        "duplicate_by_consumer": dict(duplicate_by_consumer),
        "consumer_counts": {cid: len(packages) for cid, packages in consumer_data.items()}
    }

def print_analysis_results(results: Dict):
    """
    Print formatted analysis results.
    
    Args:
        results: Analysis results dictionary
    """
    if not results:
        print("No results to display")
        return
    
    print("\n" + "="*60)
    print("CONSUMER OUTPUT ANALYSIS RESULTS")
    print("="*60)
    
    # Summary statistics
    print(f"\n📊 SUMMARY STATISTICS:")
    print(f"   Total packages processed: {results['total_processed']}")
    print(f"   Unique packages: {results['unique_packages']}")
    print(f"   Duplicate packages: {results['duplicate_count']}")
    print(f"   Package range: {results['package_range'][0]} - {results['package_range'][1]}")
    
    # Consumer breakdown
    print(f"\n👥 CONSUMER BREAKDOWN:")
    for consumer_id, count in results['consumer_counts'].items():
        print(f"   Consumer {consumer_id}: {count} packages")
    
    # Duplicates analysis
    if results['duplicate_packages']:
        print(f"\n⚠️  DUPLICATE PACKAGES FOUND:")
        print(f"   {len(results['duplicate_packages'])} packages were processed multiple times:")
        
        # Show first 10 duplicates
        duplicate_list = sorted(results['duplicate_packages'].items(), key=lambda x: x[1], reverse=True)
        for package_num, count in duplicate_list[:10]:
            consumers = []
            for cid, packages in results['consumer_data'].items():
                if package_num in packages:
                    consumers.append(cid)
            print(f"     Package {package_num}: processed {count} times by consumers {consumers}")
        
        if len(duplicate_list) > 10:
            print(f"     ... and {len(duplicate_list) - 10} more duplicates")
    else:
        print(f"\n✅ NO DUPLICATES FOUND")
    
    # Missing packages
    if results['missing_packages']:
        print(f"\n❌ MISSING PACKAGES:")
        missing_list = results['missing_packages']
        if len(missing_list) <= 20:
            print(f"   Missing packages: {missing_list}")
        else:
            print(f"   Missing {len(missing_list)} packages: {missing_list[:10]} ... {missing_list[-10:]}")
    else:
        print(f"\n✅ NO MISSING PACKAGES")
    
    # Duplicate by consumer
    if results['duplicate_by_consumer']:
        print(f"\n🔍 DUPLICATES BY CONSUMER:")
        for consumer_id, duplicates in results['duplicate_by_consumer'].items():
            print(f"   Consumer {consumer_id}: {len(duplicates)} duplicate packages")
            for package_num, count in duplicates[:5]:  # Show first 5
                print(f"     Package {package_num}: {count} times")
            if len(duplicates) > 5:
                print(f"     ... and {len(duplicates) - 5} more")
    
    print("\n" + "="*60)

def main():
    """Main function to run the analysis."""
    parser = argparse.ArgumentParser(description="Analyze consumer output files for duplicate processing")
    parser.add_argument("--output-dir", default="consumer_output", 
                       help="Directory containing consumer output files (default: consumer_output)")
    parser.add_argument("--verbose", "-v", action="store_true", 
                       help="Enable verbose output")
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"Analyzing consumer outputs in directory: {args.output_dir}")
    
    # Run analysis
    results = analyze_consumer_outputs(args.output_dir)
    
    if results:
        print_analysis_results(results)
        
        # Return exit code based on findings
        if results['duplicate_count'] > 0:
            print(f"\n🚨 ISSUE DETECTED: {results['duplicate_count']} packages were processed multiple times!")
            return 1
        else:
            print(f"\n✅ NO ISSUES DETECTED: All packages processed exactly once")
            return 0
    else:
        print("❌ ANALYSIS FAILED: Could not analyze consumer outputs")
        return 2

if __name__ == "__main__":
    exit(main()) 