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

import zipfile
from pathlib import Path

import click
import logging

from . import __version__
from . import build_datalink_metadata as _build_datalink_metadata
from .band_column_checker import BandColumnChecker

__all__ = ["cli"]

logger = logging.getLogger("sdm_tools")

loglevel_choices = list(logging._nameToLevel.keys())


@click.group()
@click.version_option(__version__)
@click.option(
    "--log-level",
    type=click.Choice(loglevel_choices),
    envvar="SDM_TOOLS_LOGLEVEL",
    help="SDM Tools log level",
    default=logging.getLevelName(logging.INFO),
)
@click.option(
    "--log-file",
    type=click.Path(),
    envvar="SDM_TOOLS_LOGFILE",
    help="SDM Tools log file path",
)
@click.pass_context
def cli(ctx: click.Context, log_level: str, log_file: str | None) -> None:
    """SDM Tools Command Line Interface"""
    ctx.ensure_object(dict)
    if log_file:
        logging.basicConfig(filename=log_file, level=log_level)
    else:
        logging.basicConfig(level=log_level)


@cli.command("build-datalink-metadata", help="Build Datalink metadata from Felis YAML files")
@click.argument("files", type=click.Path(exists=True), nargs=-1, required=True)
@click.option(
    "--resource-dir",
    type=click.Path(exists=True, file_okay=False),
    default=".",
    help="Directory to search for and write resources (DEFAULT: current directory)",
)
@click.option(
    "--zip-dir",
    type=click.Path(exists=True, file_okay=False),
    default=".",
    help="Directory to write zip files (DEFAULT: current directory)",
)
@click.pass_context
def build_datalink_metadata(ctx: click.Context, files: list[str], resource_dir: str, zip_dir: str) -> None:
    """Build Datalink Metadata

    Build a collection of configuration files for datalinker that specify the
    principal and minimal columns for tables. This temporarily only does
    tap:principal and we hand-maintain a columns-minimal.yaml file until we can
    include a new key in the Felis input files.
    """
    resource_path = Path(resource_dir)

    paths = [Path(file) for file in files]
    _build_datalink_metadata.process_files(paths, Path(resource_path / "columns-principal.yaml"))

    zip_path = Path(zip_dir)
    with zipfile.ZipFile(zip_path / "datalink-columns.zip", "w") as columns_zip:
        for yaml_file in resource_path.glob("columns-*.yaml"):
            columns_zip.write(yaml_file, yaml_file.name)
    with zipfile.ZipFile(zip_path / "datalink-snippets.zip", "w") as snippets_zip:
        for snippet_file in resource_path.glob("*.json"):
            snippets_zip.write(snippet_file, snippet_file.name)
        for snippet_file in resource_path.glob("*.xml"):
            snippets_zip.write(snippet_file, snippet_file.name)


def _parse_comma_separated(ctx, param, value):
    if value:
        return value.split(",")
    return []


@cli.command("check-band-columns", help="Check consistency of band column definitions")
@click.argument("files", type=click.Path(exists=True), nargs=-1, required=True)
@click.option("--print", "-p", "print_columns", is_flag=True, help="Print out the band columns")
@click.option("--dump", "-d", "dump", is_flag=True, help="Dump the raw band column data")
@click.option(
    "--tables",
    "-t",
    "table_names",
    callback=_parse_comma_separated,
    help="Names of tables to check (comma-separated)",
)
@click.pass_context
def check_band_columns(
    ctx: click.Context,
    files: list[str],
    print_columns: bool = False,
    dump: bool = False,
    table_names: list[str] = [],
) -> None:
    """Build Datalink Metadata

    Build a collection of configuration files for datalinker that specify the
    principal and minimal columns for tables. This temporarily only does
    tap:principal and we hand-maintain a columns-minimal.yaml file until we can
    include a new key in the Felis input files.
    """
    checker = BandColumnChecker(files, table_names)
    if dump:
        checker.dump()
    if print_columns:
        checker.print()
    checker.check()


if __name__ == "__main__":
    cli()
