"""Check consistency of band column definitions."""

import logging
import pprint
import re

from deepdiff.diff import DeepDiff
from typing import Any

from felis.datamodel import Schema, Table

logger = logging.getLogger("band_column_checker")
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

BANDS = set(["u", "g", "r", "i", "z", "y"])


class BandColumnChecker:
    """Check consistency of band column definitions.

    Parameters
    ----------
    files : list[str]
        List of schema files to check.
    table_names : list[str]
        List of table names to check. If empty, all tables are checked.
    """

    def __init__(self, files: list[str], table_names: list[str]) -> None:
        self.files = files
        self.table_names = table_names
        if len(self.table_names) > 0:
            logger.debug(f"Checking tables: {self.table_names}")
        self.schemas: dict[str, Schema] = {}
        for file in files:
            with open(file) as schema_file:
                logger.debug(f"Reading schema file: {file}")
                schema = Schema.from_stream(schema_file)
                if schema.name in self.schemas:
                    raise ValueError(f"Duplicate schema name: {schema.name}")
                self.schemas[schema.name] = schema
        self._band_dict = self._create_band_dict()

    @property
    def band_columns(self) -> dict[str, Any]:
        """Return the band columns by schema and table."""
        return self._band_dict

    def _should_check_table(self, table_or_name: str | Table) -> bool:
        """Check if the table or table name should be included in checks."""
        table_name = table_or_name if isinstance(table_or_name, str) else table_or_name.name
        return len(self.table_names) == 0 or table_name in self.table_names

    def _create_band_dict(self) -> dict[str, Any]:
        """Create a dictionary of band columns by schema and table."""
        band_columns: dict[str, Any] = {}
        for schema_name, schema in self.schemas.items():
            band_columns[schema_name] = {}
            for table in schema.tables:
                if self._should_check_table(table):
                    band_columns[schema.name][table.name] = {}
                    for column in table.columns:
                        for band in BANDS:
                            if column.name.startswith(f"{band}_"):
                                if band not in band_columns[schema.name][table.name]:
                                    band_columns[schema.name][table.name][band] = []
                                band_columns[schema.name][table.name][band].append(
                                    column.name.removeprefix(f"{band}_")
                                )
                    if len(band_columns[schema.name][table.name]) == 0:
                        del band_columns[schema.name][table.name]
                else:
                    logger.debug(f"Skipping table: {table.name}")
        return band_columns

    def _create_table_band_dict(self, table: Table) -> dict[str, Any]:
        """Create a dictionary of band columns for a single table."""
        band_columns: dict[str, Any] = {}
        for column in table.columns:
            for band in BANDS:
                if column.name.startswith(f"{band}_"):
                    if band not in band_columns:
                        band_columns[band] = []
                    raw_data = column.model_dump(exclude_none=True)
                    raw_data["name"] = raw_data["name"].removeprefix(f"{band}_")
                    raw_data["description"] = self._clean_column_description(raw_data["description"], band)
                    del raw_data["id"]  # Remove the id field as they are always different
                    band_columns[band].append(raw_data)
        return band_columns

    def _check_column_count(self) -> None:
        """Check that number of columns in each band is the same."""
        for schema_name, columns in self.band_columns.items():
            for table_name, bands in columns.items():
                column_counts = {band: len(column_names) for band, column_names in bands.items()}
                logger.info(f"'{schema_name}'.'{table_name}' column counts: %s", column_counts)
                if len(set(column_counts.values())) > 1:
                    logger.error(
                        f"Inconsistent number of band columns in "
                        f"'{schema_name}'.'{table_name}': {column_counts}"
                    )

    def _check_column_names(self) -> None:
        """Check that the names of the band columns are the same."""
        for schema_name, tables in self.band_columns.items():
            logger.info("Checking column names for schema: %s", schema_name)
            for table_name, bands in tables.items():
                # Check that each band has the same column names
                reference_band, reference_columns = next(iter(bands.items()))
                reference_set = set(reference_columns)
                for band, column_names in bands.items():
                    column_set = set(column_names)
                    if column_set == reference_set:
                        continue
                    logger.error(
                        f"In '{schema_name}'.'{table_name}', "
                        f"inconsistencies found between '{reference_band}' and '{band}'"
                    )
                    logger.error(
                        f"  In '{reference_band}' but not in '{band}': {sorted(reference_set - column_set)}"
                    )
                    logger.error(
                        f"  In '{band}' but not in '{reference_band}': {sorted(column_set - reference_set)}"
                    )
        logger.info("All band columns have the same names")

    def dump(self) -> None:
        pprint.pprint(self.band_columns)

    def print(self) -> None:
        """Print out band columns by schema and table."""
        for schema_name, columns in self.band_columns.items():
            print(f"Schema: {schema_name}")
            for table_name, bands in columns.items():
                print(f"  Table: {table_name}")
                for band, column_names in bands.items():
                    print(f"    Band: {band}")
                    for column_name in column_names:
                        print(f"      Column: {column_name}")

    def _clean_column_description(self, description: str, band: str) -> str:
        """Replace band names in the column description with a placeholder."""
        description = re.sub(rf"\b{band}-", "[BAND]-", description)
        description = re.sub(rf"\b{band}_", "[BAND]_", description)
        return description

    def _diff(self) -> None:
        for schema_name, schema in self.schemas.items():
            logger.debug(f"Running diff on schema: {schema_name}")
            for table in schema.tables:
                if self._should_check_table(table):
                    logger.debug(f"Running diff on table: {table.name}")
                    band_columns: dict[str, Any] = self._create_table_band_dict(table)
                    if len(band_columns) > 1:
                        self._diff_single_table(schema_name, table.name, band_columns)
                else:  # Skip table
                    logger.debug(f"Skipping table: {table.name}")

    def _diff_single_table(self, schema_name: str, table_name: str, band_columns: dict[str, Any]) -> None:
        """Run diff comparison on the band columns for a single table."""
        reference_band, reference_columns = next(iter(band_columns.items()))
        for compare_band, compare_columns in band_columns.items():
            if compare_band == reference_band:
                continue
            logger.debug(f"Comparing '{reference_band}' and '{compare_band}'")
            diff = DeepDiff(reference_columns, compare_columns, ignore_order=True)
            if len(diff) > 0:
                logger.warning(
                    f"In '{schema_name}'.'{table_name}', differences found between"
                    f"'{reference_band}' and '{compare_band}': "
                    f"{diff}"
                )

    def check(self) -> None:
        """Check consistency of band column definitions."""
        self._check_column_count()
        self._check_column_names()
        self._diff()
