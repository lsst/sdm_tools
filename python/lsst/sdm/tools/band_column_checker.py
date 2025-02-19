"""Check consistency of band column definitions."""

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

from dataclasses import dataclass
from deepdiff.diff import DeepDiff

from typing import Any, Callable

from felis.datamodel import Schema, Table

logger = logging.getLogger(__name__)

BANDS = set(["u", "g", "r", "i", "z", "y"])

__all__ = ["BandColumnChecker", "BandDiff", "BandReport", "ParsedDeepDiffKey"]


class ParsedDeepDiffKey:
    """Simple class to represent the parsed index and field name from a
    DeepDiff key.
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
            The DeepDiff entry string, e.g., "root[1]" or "root[1]['fieldname']".

        Returns
        -------
        `ParsedDeepDiffKey`
            The extracted index and field name (or None if there is no field name).
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


@dataclass
class BandDiff:
    """Class to represent the differences between two sets of band columns."""

    diff: dict[str, Any]
    reference_columns: list[dict[str, Any]]
    compare_columns: list[dict[str, Any]]
    schema_name: str
    table_name: str
    reference_band: str
    compare_band: str

    def __post_init__(self) -> None:
        if self.reference_band not in BANDS:
            raise ValueError(f"Invalid ref band name: '{self.reference_band}'. Must be one of {BANDS}")
        if self.compare_band not in BANDS:
            raise ValueError(f"Invalid compare band name: '{self.compare_band}'. Must be one of {BANDS}")

    def _handle_values_changed(self, diff: dict[str, Any]) -> None:
        values_changed = diff["values_changed"]
        keys = list(values_changed.keys())
        for key in keys:
            parsed_key = ParsedDeepDiffKey._parse(key)
            column_name = self.reference_columns[parsed_key.index]["name"]
            values_changed[f"root['{column_name}']['{parsed_key.field_name}']"] = values_changed.pop(key)

    def _handle_dictionary_item_added(self, diff: dict[str, Any]) -> None:
        dictionary_item_added = diff["dictionary_item_added"]
        new_keys: list[str] = []
        for key in dictionary_item_added:
            parsed_key = ParsedDeepDiffKey._parse(key)
            column_name = self.reference_columns[parsed_key.index]["name"]
            new_keys.append(f"root['{column_name}']['{parsed_key.field_name}']")
        diff["dictionary_item_added"] = new_keys

    def _handle_iterable_item_added(self, diff: dict[str, Any]) -> None:
        iterable_item_added = diff["iterable_item_added"]
        keys = list(iterable_item_added.keys())
        for key in keys:
            parsed_key = ParsedDeepDiffKey._parse(key)
            column_name = self.reference_columns[parsed_key.index]["name"]
            iterable_item_added[f"root['{column_name}']"] = iterable_item_added.pop(key)

    def _handle_dictionary_item_removed(self, diff: dict[str, Any]) -> None:
        dictionary_item_removed = diff["dictionary_item_removed"]
        removed_keys: list[str] = []
        for key in dictionary_item_removed:
            parsed_key = ParsedDeepDiffKey._parse(key)
            column_name = self.reference_columns[parsed_key.index]["name"]
            removed_keys.append(f"root['{column_name}']['{parsed_key.field_name}']")
        diff["dictionary_item_removed"] = removed_keys

    def _handle_iterable_item_removed(self, diff: dict[str, Any]) -> None:
        iterable_item_removed = diff["iterable_item_removed"]
        keys = list(iterable_item_removed.keys())
        for key in keys:
            parsed_key = ParsedDeepDiffKey._parse(key)
            column_name = self.reference_columns[parsed_key.index]["name"]
            iterable_item_removed[f"root['{column_name}']"] = iterable_item_removed.pop(key)
        diff["iterable_item_removed"] = iterable_item_removed

    def to_dict(self) -> dict[str, Any]:
        output_dict: dict[str, Any] = {
            "schema": self.schema_name,
            "table": self.table_name,
            "reference_band": self.reference_band,
            "compare_band": self.compare_band,
        }
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

        output_dict["changes"] = diff
        return output_dict


class BandReport:

    def __init__(self, band_diffs: list[BandDiff]) -> None:
        self.band_diffs = band_diffs

    def to_json(self) -> list[dict[str, Any]]:
        json_output = []
        for band_diff in self.band_diffs:
            json_output.append(band_diff.to_dict())
        return json_output

    def to_json_file(self, output_path: str) -> None:
        json_output = self.to_json()
        with open(output_path, "w") as stream:
            json.dump(json_output, stream, indent=2)
            logger.info(f"Band column differences written to: {output_path}")


class BandColumnChecker:
    """Check consistency of band column definitions.

    Parameters
    ----------
    files : list[str]
        list of schema files to check.
    table_names : list[str]
        list of table names to check. If empty, all tables are checked.
    """

    def __init__(
        self,
        files: list[str],
        table_names: list[str],
        reference_column_name: str = "u",
        output_path: str | None = None,
        error_on_differences: bool = False,
    ) -> None:
        self.files = files
        self.table_names = table_names
        self.reference_column_name = reference_column_name
        self.output_path = output_path
        self.error_on_differences = error_on_differences
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
        self._band_dict = self._create_band_dict_by_schema()

    @property
    def band_columns(self) -> dict[str, Any]:
        """Return the band columns by schema and table."""
        return self._band_dict

    def _should_check_table(self, table_or_name: str | Table) -> bool:
        """Check if the table or table name should be included in checks."""
        table_name = table_or_name if isinstance(table_or_name, str) else table_or_name.name
        return len(self.table_names) == 0 or table_name in self.table_names

    def _create_band_dict_by_schema(self) -> dict[str, Any]:
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

    def _create_single_table_band_dict(self, table: Table) -> dict[str, Any]:
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
                reference_band = self.reference_column_name
                reference_columns = bands[reference_band]
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

    def _clean_column_description(self, description: str, band: str) -> str:
        """Replace band names in the column description with a placeholder."""
        description = re.sub(rf"\b{band}-band", "[BAND]-band", description)
        description = re.sub(rf"\b{band}_", "[BAND]_", description)
        return description

    def _create_band_report(self) -> BandReport:
        """Create a list of band column differences."""
        band_diffs: list[BandDiff] = []
        for schema_name, schema in self.schemas.items():
            for table in schema.tables:
                if self._should_check_table(table):
                    band_columns: dict[str, Any] = self._create_single_table_band_dict(table)
                    if len(band_columns) > 1:
                        table_diff = self._diff_single_table(schema_name, table.name, band_columns)
                        band_diffs.extend(table_diff)
                else:  # Skip table
                    logger.debug(f"Skipping table: {table.name}")
        return BandReport(band_diffs)

    def _diff_single_table(
        self, schema_name: str, table_name: str, band_columns: dict[str, Any]
    ) -> list[BandDiff]:
        """Run diff comparison on the band columns for a single table."""
        band_diffs: list[BandDiff] = []
        reference_band = self.reference_column_name
        reference_columns = band_columns[reference_band]
        for compare_band, compare_columns in band_columns.items():
            if compare_band == reference_band:
                continue
            diff = DeepDiff(reference_columns, compare_columns, ignore_order=True)
            band_diff = BandDiff(
                diff,
                reference_columns,
                compare_columns,
                schema_name,
                table_name,
                reference_band,
                compare_band,
            )
            if len(diff) > 0:
                band_diffs.append(band_diff)
        return band_diffs

    def to_json_file(self, band_diffs: list[BandDiff], output_path: str) -> None:
        """Write the band column differences to a JSON file."""
        diffs = []
        for band_diff in band_diffs:
            diffs.append(band_diff.to_dict())

        with open(output_path, "w") as stream:
            json.dump(diffs, stream, indent=2)
            logger.info(f"Band column differences written to: {output_path}")

    def run(self) -> None:
        """Check consistency of band column definitions."""
        self._check_column_count()
        self._check_column_names()

        band_report = self._create_band_report()

        if self.output_path:
            band_report.to_json_file(self.output_path)
        else:
            print(json.dumps(band_report.to_json(), indent=2))

        if self.error_on_differences and len(band_report.band_diffs) > 0:
            raise ValueError("Band column differences found")
