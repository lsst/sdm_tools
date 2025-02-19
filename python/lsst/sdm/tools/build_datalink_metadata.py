"""From the Felis source files, build YAML metadata used by DataLink.

Currently, this only determines principal column names.  In the future, once
a new key has been added to Felis, it will include other column lists, and
possibly additional metadata.
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

import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml


def filter_columns(table: dict[str, Any], filter_key: str) -> list[str]:
    """Find the columns for a table with a given key.

    This respects the TAP v1.1 convention for ordering of columns.  All
    columns without ``tap:column_index`` set will be sorted after all those
    with it set, in the order in which they appeared in the Felis file.

    Parameters
    ----------
    table : Dict[`str`, Any]
        Felis definition of a table.
    filter_key : `str`
        Felis key to use to find columns of interest.  For example, use
        ``tap:principal`` to find principal columns.

    Returns
    -------
    columns : List[`str`]
        List of filtered columns in sorted order.
    """
    principal = []
    unknown_column_index = 100000000
    for column in table["columns"]:
        if column.get(filter_key):
            column_index = column.get("tap:column_index", unknown_column_index)
            unknown_column_index += 1
            principal.append((column["name"], column_index))
    return [c[0] for c in sorted(principal, key=lambda c: c[1])]


def build_columns(felis: dict[str, Any], column_properties: list[str]) -> dict[str, dict[str, list[str]]]:
    """Find the list of tables with a particular Felis property.

    Parameters
    ----------
    felis : Dict[`str`, Any]
        The parsed Felis YAML.
    column_properties : `str`
        The column properties to search for.
    """
    schema = felis["name"]
    output: dict[str, dict[str, list[str]]] = defaultdict(dict)
    for table in felis["tables"]:
        name = table["name"]
        full_name = f"{schema}.{name}"
        for column_property in column_properties:
            columns = filter_columns(table, column_property)
            output[full_name][column_property] = columns
    return output


def process_files(files: list[Path], output_path: Path | None = None) -> None:
    """Process a set of Felis input files and print output to standard out.

    Parameters
    ----------
    files : List[`pathlib.Path`]
        List of input files.

    Output
    ------
    The YAML version of the output format will look like this:

    .. code-block:: yaml

       tables:
         dp02_dc2_catalogs.ForcedSourceOnDiaObject:
           tap:principal:
             - band
             - ccdVisitId
    """
    tables = {}
    for input_file in files:
        with input_file.open("r") as fh:
            felis = yaml.safe_load(fh)
        tables.update(build_columns(felis, ["tap:principal"]))

    # Dump the result to the output stream.
    if output_path is None:
        print(yaml.dump({"tables": tables}), file=sys.stdout)
    else:

        with output_path.open("w") as output:
            print(yaml.dump({"tables": tables}), file=output)


def main() -> None:
    """Script entry point."""
    process_files([Path(f) for f in sys.argv[1:]])


if __name__ == "__main__":
    main()
