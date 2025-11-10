#!/usr/bin/env python3

import re
import sys
from collections import defaultdict

# --- Configuration ---
PRIORITY_FILE = '../workload/priority.txt'
RESULT_FILE = '../workload/result.txt'
# --- End Configuration ---

def main():
    # Regular expressions to extract executionTime and costCents
    # 1. Match 'executionTime=' followed by digits (with optional decimal), up to ' ms'
    time_pattern = re.compile(r'executionTime=([\d.]+) ms')

    # 2. Match 'costCents=' followed by digits (with optional decimal)
    cost_pattern = re.compile(r'costCents=([\d.]+)')

    # Use defaultdict to automatically initialize nested dictionaries
    # Structure: totals['priority_name']['total_time'] = 0.0
    #            totals['priority_name']['total_cost'] = 0.0
    totals = defaultdict(lambda: {'total_time': 0.0, 'total_cost': 0.0})

    line_num = 0

    try:
        # Open both files simultaneously
        with open(PRIORITY_FILE, 'r') as f_priority, open(RESULT_FILE, 'r') as f_result:

            # Use zip to read both files line by line in parallel
            for priority_line, result_line in zip(f_priority, f_result):
                line_num += 1

                # 1. Get the priority
                # .strip() removes trailing newline characters
                priority = priority_line.strip()

                if not priority:
                    print(f"Warning: {PRIORITY_FILE} is empty on line {line_num}. Skipping...", file=sys.stderr)
                    continue

                # 2. Extract data from the result_line
                time_match = time_pattern.search(result_line)
                cost_match = cost_pattern.search(result_line)

                # 3. Check if both matches were successful
                if time_match and cost_match:
                    try:
                        # Convert extracted strings to floating-point numbers
                        exec_time = float(time_match.group(1))
                        cost_cents = float(cost_match.group(1))

                        # 4. Add to the totals
                        totals[priority]['total_time'] += exec_time
                        totals[priority]['total_cost'] += cost_cents

                    except ValueError:
                        print(f"Warning: Could not parse numerical value on line {line_num}. Skipping...", file=sys.stderr)
                        print(f"  -> Original data: Time='{time_match.group(1)}', Cost='{cost_match.group(1)}'", file=sys.stderr)
                else:
                    print(f"Warning: Could not find matches on line {line_num}. Skipping...", file=sys.stderr)
                    if not time_match:
                        print(f"  -> 'executionTime' not found in: '{result_line.strip()}'", file=sys.stderr)
                    if not cost_match:
                        print(f"  -> 'costCents' not found in: '{result_line.strip()}'", file=sys.stderr)

    except FileNotFoundError as e:
        print(f"Error: File not found: {e.filename}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return 1

    # --- 5. Print the final results ---
    print("--- Query Execution Statistics ---")
    print("\n")

    if not totals:
        print("No data was processed. Please check if files are empty or formatted incorrectly.")
        return

    # Sort priorities for a clean, ordered output
    sorted_priorities = sorted(totals.keys())

    for priority in sorted_priorities:
        data = totals[priority]
        print(f"## Priority: {priority}")
        # :.2f formats the float to 2 decimal places
        print(f"  Total Execution Time (ms): {data['total_time']:.2f}")
        # :.8f formats the float to 8 decimal places (for small cost values)
        print(f"  Total Cost (cents):        {data['total_cost']:.8f}")
        print("-" * 30)

    return 0

if __name__ == "__main__":
    sys.exit(main())