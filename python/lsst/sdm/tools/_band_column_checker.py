"""Tools for checking consistency of band column definitions within a single
table, as well as comparing them across different schemas.
"""

# This file is part of sdm_tools.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from typing import Any

from deepdiff.diff import DeepDiff
from felis.datamodel import Column, Schema, Table

logger = logging.getLogger("lsst.sdm.tools")

BANDS = ("u", "g", "r", "i", "z", "y")

__all__ = [
    "BandColumnChecker",
    "BandComparisonReport",
    "DiffFormatter",
    "ParsedDeepDiffKey",
    "SchemaBandColumnComparator",
]


class ParsedDeepDiffKey:
    """Represents the parsed index and field name from a raw DeepDiff key.

    Parameters
    ----------
    index : int
        The index of the column in the list of columns.
    field_name : str
        The name of the field in the column (or None if there is no field
        name).
    """

    def __init__(self, index: int, field_name: str | None) -> None:
        self.index = index
        self.field_name = field_name

    @classmethod
    def _parse(cls, diff_entry: str) -> ParsedDeepDiffKey:
        """
        Parse a DeepDiff entry key to extract the index and field name.

        Parameters
        ----------
        diff_entry : str
            The DeepDiff entry string, e.g., "root[1]" or
            "root[1]['fieldname']".

        Returns
        -------
        `ParsedDeepDiffKey`
            The extracted index and field name (or None if there is no field
            name).
        """
        index_match = re.search(r"root\[(\d+)\]", diff_entry)
        field_name_match = re.search(r"root\[\d+\]\['(.+)'\]", diff_entry)

        if index_match:
            index = int(index_match.group(1))
        else:
            raise ValueError(f"Invalid diff entry format: {diff_entry}")

        field_name = field_name_match.group(1) if field_name_match else None

        return ParsedDeepDiffKey(index, field_name)


def _remap_keys(diff: dict[str, Any], key_map: dict[str, str]) -> None:
    """
    Remap keys in the diff dictionary according to the key_map.

    Parameters
    ----------
    diff : dict
        The dictionary containing the diff.
    key_map : dict
        A dictionary mapping old keys to new keys.
    """
    for old_key, new_key in key_map.items():
        if old_key in diff:
            diff[new_key] = diff.pop(old_key)


class DiffFormatter:
    """Format a DeepDiff dictionary to a more user-friendly format.

    Parameters
    ----------
    diff : dict[str, Any]
        The DeepDiff dictionary.
    reference_columns : list[dict[str, Any]]
        The reference columns.
    comparison_columns : list[dict[str, Any]]
        The columns to compare against the reference columns.
    """

    def __init__(
        self,
        diff: dict[str, Any],
        reference_columns: list[dict[str, Any]],
        comparison_columns: list[dict[str, Any]],
    ) -> None:
        self.diff = diff
        self.reference_columns = reference_columns
        self.comparison_columns = comparison_columns

    def _handle_values_changed(self, diff: dict[str, Any]) -> None:
        values_changed = diff["values_changed"]
        keys = list(values_changed.keys())
        for key in keys:
            parsed_key = ParsedDeepDiffKey._parse(key)
            column_name = self.reference_columns[parsed_key.index]["name"]

            new_key = f"columns['{column_name}']"
            if parsed_key.field_name:
                new_key += f"['{parsed_key.field_name}']"

            # Map values_changed keys to user-friendly keys
            values_changed[key]["reference"] = values_changed[key].pop("old_value")
            values_changed[key]["comparison"] = values_changed[key].pop("new_value")

            # Map to new key
            values_changed[new_key] = values_changed.pop(key)

    def _handle_dictionary_item_added(self, diff: dict[str, Any]) -> None:
        dictionary_item_added = diff["dictionary_item_added"]
        new_keys: list[str] = []
        for key in dictionary_item_added:
            parsed_key = ParsedDeepDiffKey._parse(key)
            column_name = self.reference_columns[parsed_key.index]["name"]
            new_keys.append(f"columns['{column_name}']['{parsed_key.field_name}']")
        diff["dictionary_item_added"] = new_keys

    def _handle_iterable_item_added(self, diff: dict[str, Any]) -> None:
        iterable_item_added = diff["iterable_item_added"]
        keys = list(iterable_item_added.keys())
        for key in keys:
            parsed_key = ParsedDeepDiffKey._parse(key)
            iterable_item_added[f"columns[{parsed_key.index}]"] = iterable_item_added.pop(key)

    def _handle_dictionary_item_removed(self, diff: dict[str, Any]) -> None:
        dictionary_item_removed = diff["dictionary_item_removed"]
        removed_keys: list[str] = []
        for key in dictionary_item_removed:
            parsed_key = ParsedDeepDiffKey._parse(key)
            column_name = self.reference_columns[parsed_key.index]["name"]
            removed_keys.append(f"columns['{column_name}']['{parsed_key.field_name}']")
        diff["dictionary_item_removed"] = removed_keys

    def _handle_iterable_item_removed(self, diff: dict[str, Any]) -> None:
        iterable_item_removed = diff["iterable_item_removed"]
        keys = list(iterable_item_removed.keys())
        parsed_keys = [ParsedDeepDiffKey._parse(key) for key in keys]
        removed_column_names = []
        for key, parsed_key in zip(keys, parsed_keys):
            column_name = self.reference_columns[parsed_key.index]["name"]
            removed_column_names.append(column_name)
        diff["iterable_item_removed"] = removed_column_names

    def format(self) -> dict[str, Any]:
        """Return the reformatted diff as a dictionary.

        Returns
        -------
        dict[str, Any]
            The reformatted diff.
        """
        diff = self.diff.copy()

        function_map: dict[str, Callable[[dict[str, Any]], None]] = {
            "values_changed": self._handle_values_changed,
            "dictionary_item_added": self._handle_dictionary_item_added,
            "iterable_item_added": self._handle_iterable_item_added,
            "dictionary_item_removed": self._handle_dictionary_item_removed,
            "iterable_item_removed": self._handle_iterable_item_removed,
        }

        for key in diff.keys():
            if key in function_map:
                function_map[key](diff)

        # Remap keys to more user-friendly names
        key_map = {
            "values_changed": "field_changed",
            "dictionary_item_added": "field_added",
            "dictionary_item_removed": "field_removed",
            "iterable_item_added": "column_added",
            "iterable_item_removed": "column_removed",
        }

        _remap_keys(diff, key_map)

        return diff


class BandComparisonReport:
    """Represents a report of band column differences."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    def add_diff(self, schema_name: str, table_name: str, band: str, diff: dict[str, Any]) -> None:
        """Add a band column difference to the report.

        Parameters
        ----------
        schema_name : str
            The name of the schema.
        table_name : str
            The name of the table.
        band : str
            The band name.
        diff : dict[str, Any]
            The diff dictionary.
        """
        self.data.setdefault(schema_name, {}).setdefault(table_name, {}).setdefault(band, []).append(diff)

    def to_json_file(self, output_path: str) -> None:
        """Write the band column differences to a JSON file.

        Parameters
        ----------
        output_path : str
            The path to write the output JSON file.
        """
        with open(output_path, "w") as stream:
            json.dump(self.data, stream, indent=2)
            logger.info(f"Band column differences written to: {output_path}")


class BandColumnChecker:
    """Check consistency of band column definitions.

    Parameters
    ----------
    files : list[str]
        List of schema files to check.
    table_names : list[str]
        List of table names to check. If empty, all tables are checked.
    reference_band : str
        The band column to use as the reference for comparison.
    output_path : str | None
        The path to write the output JSON file. If None, the output is printed
        to stdout.
    error_on_differences : bool
        Raise an error if differences are found.
    ignore_description : bool
        Ignore differences in column descriptions.
    """

    def __init__(
        self,
        files: list[str],
        table_names: list[str],
        reference_band: str = "u",
        output_path: str | None = None,
        error_on_differences: bool = False,
        ignore_description: bool = False,
    ) -> None:
        self.files = files
        self.table_names = table_names
        self.reference_band = reference_band
        self.output_path = output_path
        self.error_on_differences = error_on_differences
        self.ignore_description = ignore_description
        if len(self.table_names) > 0:
            logger.debug(f"Checking tables: {self.table_names}")
        self.schemas: dict[str, Schema] = self._load_schemas(files)
        self._band_dict = self._create_band_columns()

    def _diff(
        self, reference_columns: list[dict[str, Any]], comparison_columns: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Diff the columns in the compare schema against the reference
        schema.
        """
        return DeepDiff(reference_columns, comparison_columns, ignore_order=True)

    def _diff_column(
        self,
        reference_column: Column,
        comparison_column: Column,
    ) -> dict[str, Any]:
        """Diff two columns."""
        return DeepDiff(reference_column, comparison_column, ignore_order=True, exclude_paths="root['id']")

    def _load_schemas(self, files: list[str]) -> dict[str, Schema]:
        """Load schemas from a list of files."""
        schemas: dict[str, Schema] = {}
        for file in files:
            with open(file) as schema_file:
                schema = Schema.from_stream(schema_file)
                if schema.name in schemas:
                    raise ValueError(f"Duplicate schema name: {schema.name}")
                schemas[schema.name] = schema
        return schemas

    @property
    def band_columns(self) -> dict[str, Any]:
        """Return the band columns by schema and table.

        Returns
        -------
        dict[str, Any]
            The band columns by schema and table.
        """
        return self._band_dict

    def _create_band_columns(self) -> dict[str, Any]:
        """Create a dictionary of band columns by schema and table."""
        band_columns: dict[str, Any] = {}
        for schema_name, schema in self.schemas.items():
            band_columns[schema_name] = {}
            for table in schema.tables:
                if self._should_check_table(table):
                    band_columns[schema.name][table.name] = {}
                    logger.debug(f"Processing table: {table.name}")
                    for band in BANDS:
                        for column in table.columns:
                            if column.name.startswith(f"{band}_"):
                                band_columns[schema.name][table.name].setdefault(band, [])
                                column_data = self._dump_column(column, band)
                                band_columns[schema.name][table.name][band].append(column_data)

                    if not band_columns[schema_name][table.name]:
                        del band_columns[schema_name][table.name]
                else:
                    logger.debug(f"Skipping table: {table.name}")

        return band_columns

    def _dump_column(self, column: Column, band: str) -> dict[str, Any]:
        """Dump a table to a dictionary."""
        raw_data: dict[str, Any]
        raw_data = column.model_dump(exclude_none=True, exclude_defaults=True)
        raw_data["name"] = column.name.removeprefix(f"{band}_")

        # Special case for column names in APDB and DP0.3
        if raw_data["name"] == f"H_{band}_G12_Cov":
            raw_data["name"] = "H_[BAND]_G12_Cov"
        elif raw_data["name"] == f"H_{band}G12_Cov":
            raw_data["name"] = "H_[BAND]G12_Cov"

        if not self.ignore_description:
            raw_data["description"] = self._clean_column_description(raw_data["description"], band)
        else:  # Remove description
            raw_data.pop("description")
        del raw_data["id"]  # This is always different
        return raw_data

    def _should_check_table(self, table_or_name: str | Table) -> bool:
        """Check if the table or table name should be included in checks."""
        table_name = table_or_name if isinstance(table_or_name, str) else table_or_name.name
        return len(self.table_names) == 0 or table_name in self.table_names

    def _check_column_count(self) -> None:
        """Check that number of columns in each band is the same."""
        for schema_name, columns in self.band_columns.items():
            for table_name, bands in columns.items():
                column_counts = {band: len(column_names) for band, column_names in bands.items()}
                logger.info(f"'{schema_name}'.'{table_name}' column counts: %s", column_counts)
                if len(set(column_counts.values())) > 1:
                    logger.warning(
                        f"Inconsistent number of band columns in "
                        f"'{schema_name}'.'{table_name}': {column_counts}"
                    )

    def _check_column_names(self) -> None:
        """Check that the names of the band columns are the same."""
        for schema_name, tables in self.band_columns.items():
            logger.debug("Checking column names for schema: %s", schema_name)

            for table_name, bands in tables.items():
                logger.debug("Checking column names for table: %s", table_name)
                logger.debug("Bands: %s", bands.keys())
                if self.reference_band not in bands:
                    logger.error(
                        f"Reference band '{self.reference_band}' not found in '{schema_name}'.'{table_name}'"
                    )
                    continue
                reference_column_names = {column["name"] for column in bands[self.reference_band]}
                logger.debug("Reference band: %s", self.reference_band)
                logger.debug(f"Reference column names: {reference_column_names}")
                for band, columns in bands.items():
                    if band == self.reference_band:
                        logger.debug(f"Skipping reference band: {band}")
                        continue
                    logger.debug(f"Checking names for Table: {table_name}, Band: {band}")
                    column_names = {column["name"] for column in columns}
                    in_ref_and_not_in_band = sorted(reference_column_names - column_names)
                    in_band_and_not_in_ref = sorted(column_names - reference_column_names)
                    if in_ref_and_not_in_band or in_band_and_not_in_ref:
                        logger.warning(
                            f"In '{schema_name}'.'{table_name}', "
                            f"inconsistencies found between '{self.reference_band}' and '{band}'"
                        )
                        logger.warning(
                            f"  In '{self.reference_band}' but not in '{band}': {in_ref_and_not_in_band}"
                        )
                        logger.warning(
                            f"  In '{band}' but not in '{self.reference_band}': {in_band_and_not_in_ref}"
                        )
                    else:
                        logger.debug(f"  Band '{band}' column names match reference band")

    @classmethod
    def _clean_column_description(cls, description: str, band: str) -> str:
        """Replace band names in the column description with a placeholder."""
        description = re.sub(rf"\b{band}_", "[BAND]_", description)
        description = re.sub(rf"\b{band}-band", "[BAND]-band", description)
        description = re.sub(rf"\b{band} band\b", "[BAND] band", description)
        description = re.sub(rf"\b{band} filter\b", "[BAND] filter", description)
        return description

    def _create_band_report(self) -> BandComparisonReport:
        """Create a list of band column differences."""
        band_report = BandComparisonReport()
        for schema_name in self.band_columns:
            for table_name in self.band_columns[schema_name]:
                if self._should_check_table(table_name):
                    table_band_columns = self.band_columns[schema_name][table_name]
                    reference_columns = table_band_columns.get(self.reference_band, [])
                    if not reference_columns:
                        logger.warning(
                            f"Reference band '{self.reference_band}' not found in "
                            f"'{schema_name}'.'{table_name}' - skipping table"
                        )
                        continue
                    for band_name, band_columns in table_band_columns.items():
                        if band_name == self.reference_band:
                            continue
                        diff = self._diff(reference_columns, band_columns)
                        if diff:
                            formatted_diff = DiffFormatter(diff, reference_columns, band_columns).format()
                            band_report.add_diff(schema_name, table_name, band_name, formatted_diff)
                else:  # Skip table
                    logger.debug(f"Skipping table: {table_name}")
        return band_report

    def run(self) -> None:
        """Check consistency of band column definitions."""
        # Check that the number of columns in each band is the same
        self._check_column_count()

        # Check that the names of the band columns are the same
        self._check_column_names()

        # Create the band report with full diff output
        band_report = self._create_band_report()

        # Write the band report to a file or print to stdout
        if self.output_path:
            band_report.to_json_file(self.output_path)
        else:
            diff_dump = json.dumps(band_report.data, indent=2)
            if diff_dump:
                logger.info(f"Changes: {diff_dump}")
            else:
                logger.info("No band column changes were found")

        # Raise an error if differences are found
        if self.error_on_differences and len(band_report.data):
            raise ValueError("Band column differences found")


class SchemaBandColumnComparator(BandColumnChecker):
    """Compare band columns between two schemas.

    Parameters
    ----------
    *args : Any
        Positional arguments.
    **kwargs : Any
        Keyword arguments.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "bands" in kwargs:
            self.bands = kwargs.pop("bands")
        else:
            self.bands = BANDS
        super().__init__(*args, **kwargs)

    def _compare_schemas(self) -> dict[str, Any]:
        """Compare band columns between two schemas."""
        schemas = list(self.schemas.values())
        reference_schema = schemas[0]
        comparison_schema = schemas[1]
        results: dict[str, Any] = {}
        logger.debug(f"Comparing schemas: {reference_schema.name} and {comparison_schema.name}")
        reference_tables = self.band_columns[reference_schema.name]
        comparison_tables = self.band_columns[comparison_schema.name]
        results = {}
        for table_name in reference_tables.keys():
            if table_name in comparison_tables and self._should_check_table(table_name):
                logger.debug(f"Comparing table: {table_name}")
                for band in self.bands:
                    reference_columns = reference_tables[table_name][band]
                    comparison_columns = comparison_tables[table_name][band]
                    logger.debug("  Band: %s", band)
                    diff = self._diff(reference_columns, comparison_columns)
                    if diff:
                        results.setdefault(table_name, {}).setdefault(band, []).append(
                            DiffFormatter(
                                diff,
                                reference_columns,
                                comparison_columns,
                            ).format()
                        )
        return results

    def run(self) -> None:
        """Run the schema band column comparison."""
        results = self._compare_schemas()

        if self.output_path:
            with open(self.output_path, "w") as stream:
                json.dump(results, stream, indent=2)
                logger.info(f"Band column differences written to: {self.output_path}")
        else:
            if results:
                logger.info(f"Differences: {json.dumps(results, indent=2)}")
            else:
                logger.info("No band column differences found")

        if self.error_on_differences and results:
            raise ValueError("Band column differences found")
