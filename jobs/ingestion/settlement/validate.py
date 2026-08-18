#!/usr/bin/env python3
"""
Validate settlement CSV files before ingestion.

Usage:
    python validate_settlement_csv.py <csv_file_path>

Example:
    python validate_settlement_csv.py settlement_data.csv
"""

import csv
import sys
from pathlib import Path
from typing import List, Tuple
import re


REQUIRED_COLUMNS = {
    "home_pmn", "home_operator", "traffic_direction", "service_type",
    "event_type", "partner_name", "partner_pmn", "group_name", "country",
    "zone_name", "traffic_period", "agreement_reference", "agreement_start_date",
    "agreement_end_date", "actual_forecasted", "currency", "conversion_rate",
    "traffic_volume", "tap_charges_excl_tax", "tap_charges_incl_tax",
    "tap_iot_rate_excl_tax", "tap_iot_rate_incl_tax",
    "post_discounted_charges_excl_tax", "post_discounted_charges_incl_tax",
    "sop_adjustment_excl_tax", "sop_adjustment_incl_tax",
    "before_sop_discounted_charge_excl_tax", "before_sop_discounted_charge_incl_tax",
    "post_discounted_iot_rate_excl_tax", "post_discounted_iot_rate_incl_tax",
    "discount_achieved_excl_tax", "discount_achieved_incl_tax",
    "agreement_status", "negotiator", "iot_rate_source",
    "settled_amount_excl_tax", "settled_amount_incl_tax",
}

DATE_COLUMNS = {"agreement_start_date", "agreement_end_date"}
DECIMAL_COLUMNS = {
    "conversion_rate", "traffic_volume",
    "tap_charges_excl_tax", "tap_charges_incl_tax",
    "tap_iot_rate_excl_tax", "tap_iot_rate_incl_tax",
    "post_discounted_charges_excl_tax", "post_discounted_charges_incl_tax",
    "sop_adjustment_excl_tax", "sop_adjustment_incl_tax",
    "before_sop_discounted_charge_excl_tax", "before_sop_discounted_charge_incl_tax",
    "post_discounted_iot_rate_excl_tax", "post_discounted_iot_rate_incl_tax",
    "discount_achieved_excl_tax", "discount_achieved_incl_tax",
    "settled_amount_excl_tax", "settled_amount_incl_tax",
}


def normalize_column_name(name: str) -> str:
    """Normalize column name to snake_case."""
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip()).strip("_").lower()
    return cleaned or "col"


def detect_delimiter(file_path: str) -> str:
    """Detect CSV delimiter."""
    with open(file_path, "r", encoding="utf-8-sig") as f:
        sample = f.read(4096)

    delimiters = {",": 0, ";": 0, "\t": 0, "|": 0, "~": 0}
    first_line = sample.split("\n")[0]

    for delimiter in delimiters:
        delimiters[delimiter] = first_line.count(delimiter)

    return max(delimiters, key=delimiters.get)


def validate_csv(file_path: str) -> Tuple[bool, List[str]]:
    """Validate settlement CSV file."""
    errors = []
    warnings = []

    path = Path(file_path)
    if not path.exists():
        return False, [f"File not found: {file_path}"]

    if not path.suffix.lower() == ".csv":
        return False, [f"File is not a CSV: {file_path}"]

    # Detect delimiter
    delimiter = detect_delimiter(file_path)
    print(f"✓ Detected delimiter: {repr(delimiter)}")

    try:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=delimiter)

            if not reader.fieldnames:
                return False, ["No header row found in CSV"]

            # Normalize and check column names
            normalized_columns = {normalize_column_name(col): col for col in reader.fieldnames}

            print(f"✓ Found {len(reader.fieldnames)} columns")
            print(f"  Raw columns: {', '.join(reader.fieldnames[:5])}...")
            print(f"  Normalized: {', '.join(list(normalized_columns.keys())[:5])}...")

            # Check for missing columns
            missing = REQUIRED_COLUMNS - set(normalized_columns.keys())
            if missing:
                errors.append(f"Missing required columns: {', '.join(sorted(missing))}")
            else:
                print(f"✓ All {len(REQUIRED_COLUMNS)} required columns present")

            # Check data rows
            row_count = 0
            empty_rows = 0
            date_errors = []
            decimal_errors = []

            for row_num, row in enumerate(reader, start=2):
                row_count += 1

                # Check for empty rows
                if not any(v.strip() for v in row.values()):
                    empty_rows += 1
                    continue

                # Validate dates
                for col, value in row.items():
                    norm_col = normalize_column_name(col)
                    if norm_col in DATE_COLUMNS and value.strip():
                        if not re.match(r"^\d{4}-\d{2}-\d{2}$", value.strip()):
                            if len(date_errors) < 5:
                                date_errors.append(
                                    f"Row {row_num}, {col}: Invalid date '{value}' (expected YYYY-MM-DD)"
                                )

                    # Validate decimals
                    if norm_col in DECIMAL_COLUMNS and value.strip():
                        try:
                            float(value.strip())
                        except ValueError:
                            if len(decimal_errors) < 5:
                                decimal_errors.append(
                                    f"Row {row_num}, {col}: Invalid number '{value}'"
                                )

            print(f"✓ Read {row_count} data rows")
            if empty_rows:
                warnings.append(f"Found {empty_rows} empty rows (will be filtered during ingestion)")

            if date_errors:
                errors.extend(date_errors)
                if len(date_errors) == 5:
                    errors.append("  ... (showing first 5 errors)")
            else:
                print(f"✓ Date columns validated (YYYY-MM-DD format)")

            if decimal_errors:
                errors.extend(decimal_errors)
                if len(decimal_errors) == 5:
                    errors.append("  ... (showing first 5 errors)")
            else:
                print(f"✓ Decimal columns validated (numeric format)")

    except Exception as e:
        return False, [f"Error reading CSV: {str(e)}"]

    if errors:
        return False, errors

    return True, warnings


def main():
    if len(sys.argv) < 2:
        print("Usage: python validate_settlement_csv.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]

    print(f"Validating: {csv_file}\n")
    print("=" * 70)

    is_valid, messages = validate_csv(csv_file)

    print("=" * 70)

    if is_valid:
        print("\n✓ CSV is valid and ready for ingestion!")
        if messages:
            print("\nWarnings:")
            for msg in messages:
                print(f"  ⚠ {msg}")
    else:
        print("\n✗ CSV has errors:\n")
        for msg in messages:
            print(f"  ✗ {msg}")
        sys.exit(1)


if __name__ == "__main__":
    main()
