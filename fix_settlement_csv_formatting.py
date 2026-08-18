#!/usr/bin/env python3
"""
Fix Gold Settlement CSV Formatting Issues

This script fixes common data quality issues in settlement CSV files:
1. Removes thousand separators from numeric columns
2. Validates decimal and date formats
3. Generates a report of issues found

Usage:
    python fix_settlement_csv_formatting.py <input_csv> <output_csv>

Example:
    python fix_settlement_csv_formatting.py EE_FOR_2026_06_23_03_15_26.csv EE_FOR_2026_06_23_03_15_26_FIXED.csv
"""

import sys
import re
import csv
from pathlib import Path
from typing import List, Tuple
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# List of columns that should have numeric/decimal values (nullable)
DECIMAL_COLUMNS = [
    'conversion_rate',
    'traffic_volume',
    'tap_charges_excl_tax',
    'tap_charges_incl_tax',
    'tap_iot_rate_excl_tax',
    'tap_iot_rate_incl_tax',
    'post_discounted_charges_excl_tax',
    'post_discounted_charges_incl_tax',
    'sop_adjustment_excl_tax',
    'sop_adjustment_incl_tax',
    'before_sop_discounted_charge_excl_tax',
    'before_sop_discounted_charge_incl_tax',
    'post_discounted_iot_rate_excl_tax',
    'post_discounted_iot_rate_incl_tax',
    'discount_achieved_excl_tax',
    'discount_achieved_incl_tax',
    'settled_amount_excl_tax',
    'settled_amount_incl_tax',
    # File 2 extra columns
    'dch_tap_charges_excl_tax',
    'dch_tap_charges_incl_tax',
    'dch_traffic_volume_actual',
    'dch_traffic_volume_billed',
    'discount_achieved_netting_excl_tax',
    'discount_achieved_netting_incl_tax',
    'discounted_vs_tap_rate_excl_tax',
    'discounted_vs_tap_rate_incl_tax',
    'document_raised_amount_excl_tax',
    'document_raised_amount_incl_tax',
    'traffic_volume_not_rounded',
]

# List of columns that should have date values (nullable)
DATE_COLUMNS = [
    'traffic_period',
    'agreement_start_date',
    'agreement_end_date',
]

# Columns that allow null values
NULLABLE_COLUMNS = [
    'agreement_reference',
    'agreement_start_date',
    'agreement_end_date',
    'zone_name',
]

# Regex patterns for validation
DECIMAL_REGEX = re.compile(r'^-?\d+(\.\d+)?$')
DATE_REGEX = re.compile(
    r'^\d{4}-\d{2}-\d{2}$|'  # YYYY-MM-DD
    r'^\d{1,2}-[A-Za-z]{3}-\d{2,4}$|'  # DD-MMM-YY
    r'^\d{1,2}/\d{1,2}/\d{2,4}$|'  # DD/MM/YYYY or MM/DD/YYYY
    r'^\d{2,4}/\d{1,2}/\d{1,2}$'  # YYYY/MM/DD or other formats
)


class CSVValidator:
    def __init__(self, input_file: str):
        self.input_file = Path(input_file)
        self.issues = []
        self.fixed_count = 0
        self.total_rows = 0
        self.exponential_conversions = 0
        self.thousand_separator_removals = 0

        if not self.input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

    def convert_exponential_to_decimal(self, value: str) -> Tuple[str, bool]:
        """
        Convert exponential notation (e.g., 5.541656101035829E7) to decimal.

        Returns: (converted_value, was_converted)
        """
        if not value or value.strip() == '':
            return value, False

        value_str = str(value).strip()
        try:
            # Check if value contains exponential notation
            if 'e' in value_str.lower():
                # Convert exponential to float then to string
                float_val = float(value_str)
                # Format as integer if it's a whole number, otherwise with decimals
                if float_val == int(float_val):
                    return str(int(float_val)), True
                else:
                    return str(float_val), True
            return value_str, False
        except (ValueError, AttributeError):
            return value, False

    def remove_thousand_separators(self, value: str, column: str) -> Tuple[str, bool]:
        """
        Remove thousand separators and convert exponential notation to decimal.
        Silently fixes both issues without reporting as errors.

        Returns: (cleaned_value, was_modified)
        """
        if not value or value.strip() == '':
            return value, False

        original = value
        was_modified = False

        # First convert exponential notation if present
        cleaned, exp_converted = self.convert_exponential_to_decimal(original)
        if exp_converted:
            self.exponential_conversions += 1
            was_modified = True

        # Remove thousand separators (commas) but keep decimal point
        if ',' in cleaned:
            cleaned = cleaned.replace(',', '')
            self.thousand_separator_removals += 1
            was_modified = True

        return cleaned, was_modified

    def validate_decimal(self, value: str, column: str) -> Tuple[bool, str]:
        """
        Validate decimal format. Nulls allowed for all columns.

        Returns: (is_valid, reason)
        """
        if not value or value.strip() == '':
            return True, "NULL (allowed)"

        if DECIMAL_REGEX.match(value):
            return True, "Valid"

        return False, f"Invalid format: '{value}' (expected: ^-?\\d+(\\.\\d+)?$)"

    def validate_date(self, value: str, column: str) -> Tuple[bool, str]:
        """
        Validate date format. Nulls allowed for all date columns.

        Returns: (is_valid, reason)
        """
        if not value or value.strip() == '':
            return True, "NULL (allowed)"

        if DATE_REGEX.match(value):
            return True, "Valid"

        return False, f"Invalid format: '{value}' (expected: YYYY-MM-DD, DD-MMM-YY, etc.)"

    def process_file(self, output_file: str):
        """Process CSV file and fix formatting issues."""
        logger.info(f"Processing file: {self.input_file}")

        # Read input CSV
        rows = []
        with open(self.input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                logger.error("CSV file has no headers")
                return False

            columns = reader.fieldnames
            logger.info(f"Found {len(columns)} columns: {', '.join(columns[:5])}...")

            for row_num, row in enumerate(reader, start=2):
                self.total_rows += 1
                fixed_row = row.copy()

                # Fix decimal columns (exponential + thousand separators)
                for col in DECIMAL_COLUMNS:
                    if col in fixed_row:
                        original_value = fixed_row[col]
                        cleaned_value, was_modified = self.remove_thousand_separators(original_value, col)

                        if was_modified:
                            self.fixed_count += 1
                            fixed_row[col] = cleaned_value
                            # Don't report exponential/comma fixes as issues - these are expected conversions
                        else:
                            # Only validate if no modifications were made (comma/exponential handling)
                            is_valid, reason = self.validate_decimal(original_value, col)
                            if not is_valid and original_value.strip():
                                self.issues.append({
                                    'row': row_num,
                                    'column': col,
                                    'original': original_value,
                                    'fixed': original_value,
                                    'status': 'INVALID_VALUE',
                                    'reason': reason
                                })

                # Validate date columns (no fixing needed for dates)
                for col in DATE_COLUMNS:
                    if col in fixed_row:
                        value = fixed_row[col]
                        is_valid, reason = self.validate_date(value, col)
                        if not is_valid and value.strip():
                            self.issues.append({
                                'row': row_num,
                                'column': col,
                                'original': value,
                                'fixed': value,
                                'status': 'INVALID_DATE_FORMAT',
                                'reason': reason
                            })

                rows.append(fixed_row)

        # Write output CSV
        output_path = Path(output_file)
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)

        logger.info(f"Successfully wrote fixed CSV to: {output_path}")
        return True

    def print_report(self):
        """Print validation report."""
        logger.info("\n" + "="*70)
        logger.info("CSV FORMATTING FIX REPORT")
        logger.info("="*70)
        logger.info(f"Input File:       {self.input_file}")
        logger.info(f"Total Rows:       {self.total_rows}")
        logger.info(f"Cells Fixed:      {self.fixed_count}")
        if self.exponential_conversions > 0:
            logger.info(f"  - Exponential notation converted: {self.exponential_conversions} values")
        if self.thousand_separator_removals > 0:
            logger.info(f"  - Comma separators removed: {self.thousand_separator_removals} values")
        logger.info(f"Validation Issues: {len(self.issues)}")

        if self.issues:
            logger.info("\n" + "-"*70)
            logger.info("DATA VALIDATION ISSUES:")
            logger.info("-"*70)

            # Group by status
            by_status = {}
            for issue in self.issues:
                status = issue['status']
                by_status.setdefault(status, []).append(issue)

            for status in sorted(by_status.keys()):
                issues = by_status[status]
                logger.info(f"\n{status}: {len(issues)} issues")

                # Show first 5 examples
                for issue in issues[:5]:
                    logger.info(
                        f"  Row {issue['row']}, Column '{issue['column']}': "
                        f"{issue['original']!r} ({issue['reason']})"
                    )

                if len(issues) > 5:
                    logger.info(f"  ... and {len(issues) - 5} more")
        else:
            logger.info("\n✓ All values successfully converted and validated!")

        logger.info("\n" + "="*70)


def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python fix_settlement_csv_formatting.py <input_csv> [output_csv]")
        logger.error("Example: python fix_settlement_csv_formatting.py input.csv input_FIXED.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else f"{Path(input_file).stem}_FIXED.csv"

    try:
        validator = CSVValidator(input_file)
        if validator.process_file(output_file):
            validator.print_report()

            # Return exit code based on issues found
            sys.exit(1 if validator.issues else 0)
    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
