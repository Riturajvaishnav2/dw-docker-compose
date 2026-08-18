#!/usr/bin/env python3
"""
Validate Settlement CSV Files

This script validates settlement CSV files to ensure:
1. home_pmn field is present in all rows (ONLY validation for invalid rows)
2. No data is modified - validation only, all transformations in Spark

Usage:
    python validate_settlement_csv.py <input_csv>

Example:
    python validate_settlement_csv.py EE_FOR_2026_06_23_03_15_26.csv
"""

import sys
import csv
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class CSVValidator:
    def __init__(self, input_file: str):
        self.input_file = Path(input_file)
        self.total_rows = 0
        self.missing_home_pmn = 0
        self.invalid_rows = []
        self.has_home_pmn_column = False

        if not self.input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

    def validate_file(self):
        """Validate CSV file for home_pmn requirement."""
        logger.info(f"Validating file: {self.input_file}")

        with open(self.input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                logger.error("CSV file has no headers")
                return False

            columns = reader.fieldnames
            logger.info(f"Found {len(columns)} columns")

            # Check if home_pmn column exists
            self.has_home_pmn_column = 'home_pmn' in columns
            if not self.has_home_pmn_column:
                logger.warning("⚠️  Column 'home_pmn' NOT found in CSV headers")
                logger.info("   → home_pmn will be NULL-filled during Spark ingestion")

            for row_num, row in enumerate(reader, start=2):
                self.total_rows += 1

                # Only validate: Check home_pmn value if column exists
                if self.has_home_pmn_column:
                    home_pmn_value = row.get('home_pmn', '').strip()
                    if not home_pmn_value:
                        self.missing_home_pmn += 1
                        self.invalid_rows.append({
                            'row': row_num,
                            'reason': 'home_pmn is missing/empty'
                        })

        return True

    def print_report(self):
        """Print validation report."""
        logger.info("\n" + "="*70)
        logger.info("CSV VALIDATION REPORT")
        logger.info("="*70)
        logger.info(f"Input File:           {self.input_file}")
        logger.info(f"Total Rows:           {self.total_rows}")

        if self.has_home_pmn_column:
            logger.info(f"home_pmn Present:     Yes")
            logger.info(f"Missing home_pmn:     {self.missing_home_pmn}")
            logger.info(f"Valid Rows:           {self.total_rows - self.missing_home_pmn}")
        else:
            logger.info(f"home_pmn Present:     No (will be NULL-filled)")
            logger.info(f"Valid Rows:           {self.total_rows} (auto-fill NULL)")

        if self.invalid_rows:
            logger.info("\n" + "-"*70)
            logger.info("INVALID ROWS (missing home_pmn):")
            logger.info("-"*70)

            # Show first 10 examples
            for issue in self.invalid_rows[:10]:
                logger.info(f"  Row {issue['row']}: {issue['reason']}")

            if len(self.invalid_rows) > 10:
                logger.info(f"  ... and {len(self.invalid_rows) - 10} more")

        logger.info("\n" + "-"*70)
        logger.info("DATA TRANSFORMATION (handled in Spark):")
        logger.info("-"*70)
        logger.info("✓ Exponential notation → Decimal conversion")
        logger.info("✓ Thousand separators (commas) → Removed")
        logger.info("✓ Null values → Preserved")
        logger.info("✓ Missing columns → NULL-filled (File 1)")

        logger.info("\n" + "="*70)
        if not self.invalid_rows and self.has_home_pmn_column:
            logger.info("STATUS: ✓ Ready for Spark ingestion to Iceberg")
        elif not self.invalid_rows and not self.has_home_pmn_column:
            logger.info("STATUS: ✓ Ready for Spark ingestion (home_pmn will be NULL)")
        else:
            logger.info(f"STATUS: ⚠️  {len(self.invalid_rows)} invalid rows - review before ingestion")
        logger.info("="*70 + "\n")

        return len(self.invalid_rows) == 0


def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python validate_settlement_csv.py <input_csv>")
        logger.error("Example: python validate_settlement_csv.py input.csv")
        sys.exit(1)

    input_file = sys.argv[1]

    try:
        validator = CSVValidator(input_file)
        if validator.validate_file():
            is_valid = validator.print_report()
            # Exit with 0 if all rows valid, 1 if any invalid
            sys.exit(0 if is_valid else 1)
    except Exception as e:
        logger.error(f"Error validating file: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
