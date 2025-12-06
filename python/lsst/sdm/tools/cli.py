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

import logging
import zipfile
from pathlib import Path

import click
from felis import Schema

from . import __version__, _build_datalink_metadata
from ._band_column_checker import BANDS, BandColumnChecker, SchemaBandColumnComparator

__all__ = ["cli"]

logger = logging.getLogger("lsst.sdm.tools")

loglevel_choices = list(logging._nameToLevel.keys())


def _setup_logger(log_level: str, log_file: str | None) -> None:
    """Set up the logger with the specified log level and log file."""
    # Create a console handler or file handler based on log_file
    handler: logging.Handler
    if log_file:
        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
    handler.setFormatter(formatter)

    # Set the formatter for the root logger but do NOT change its level
    root_logger = logging.getLogger()
    root_logger.handlers = []  # Clear existing handlers
    root_logger.addHandler(handler)

    # Configure the specific logger for "lsst.sdm.tools"
    _logger = logging.getLogger("lsst.sdm.tools")
    _logger.setLevel(log_level)
    _logger.propagate = False  # Prevent propagation to root
    _logger.handlers = []  # Clear existing handlers
    _logger.addHandler(handler)


@click.group()
@click.version_option(__version__)
@click.option(
    "--log-level",
    type=click.Choice(loglevel_choices),
    envvar="SDM_TOOLS_LOGLEVEL",
    help="SDM Tools log level (DEFAULT: INFO)",
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

    _setup_logger(log_level, log_file)


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
    try:
        data_path = Path(resource_dir)

        paths = [Path(file) for file in files]

        schemas: list[Schema] = []
        for path in paths:
            schema = Schema.from_uri(path, context={"id_generation": True})
            schemas.append(schema)

        _build_datalink_metadata.process_schemas(schemas, Path(data_path / "columns-principal.yaml"))

        zip_path = Path(zip_dir)
        with zipfile.ZipFile(zip_path / "datalink-columns.zip", "w") as columns_zip:
            for yaml_file in data_path.glob("columns-*.yaml"):
                columns_zip.write(yaml_file, yaml_file.name)
        with zipfile.ZipFile(zip_path / "datalink-snippets.zip", "w") as snippets_zip:
            for snippet_file in data_path.glob("*.json"):
                snippets_zip.write(snippet_file, snippet_file.name)
            for snippet_file in data_path.glob("*.xml"):
                snippets_zip.write(snippet_file, snippet_file.name)
    except Exception as e:
        logger.error(e, exc_info=True)
        raise click.ClickException(str(e))


def _parse_comma_separated(ctx: click.Context, param: click.Parameter, value: str) -> list[str]:
    """Parse a comma-separated string into a list of values"""
    if value:
        return value.split(",")
    return []


@cli.command(
    "check-band-columns",
    help="""
    Check self-consistency of band column definitions within schema tables

    This command checks that the band columns in a schema, which start with the
    band name followed by an underscore, are consistent across all bands. This
    includes checking that the column names, types, and descriptions are the same.
    Differences that are found will be printed to the console or written to an
    output file.

    Example:

      sdm-tools check-band-columns schema1.yaml schema2.yaml -t table1 -o diff_report.json -e
    """,
)
@click.argument("files", type=click.Path(exists=True), nargs=-1, required=True)
@click.option(
    "--tables",
    "-t",
    "table_names",
    callback=_parse_comma_separated,
    help="Names of tables to check, comma-separated (DEFAULT: all tables)",
)
@click.option("--output-file", "-o", type=click.Path(), help="Output file for the diff report")
@click.option(
    "--reference-band",
    "-r",
    type=str,
    help="Reference band for comparison (will be compared against all others)",
    default="i",
)
@click.option(
    "--error-on-differences",
    "-e",
    is_flag=True,
    help="Return an error if differences are found",
)
@click.option(
    "--ignore-description",
    "-i",
    is_flag=True,
    help="Ignore differences in column descriptions",
)
@click.pass_context
def check_band_columns(
    ctx: click.Context,
    files: list[str],
    table_names: list[str] = [],
    output_file: str | None = None,
    reference_band: str = "u",
    error_on_differences: bool = False,
    ignore_description: bool = False,
) -> None:
    """Check Band Columns"""
    if reference_band not in BANDS:
        raise click.BadParameter(f"Reference band must be one of {BANDS}")
    try:
        logger.info("Reference band: %s", reference_band)
        checker = BandColumnChecker(
            files,
            table_names,
            reference_band=reference_band,
            output_path=output_file,
            error_on_differences=error_on_differences,
            ignore_description=ignore_description,
        )
        checker.run()
    except Exception as e:
        logger.error(e, exc_info=True)
        raise click.ClickException(str(e))


@cli.command(
    "compare-band-columns",
    help="""
    Compare band column definitions between schemas

    Differences will be printed as the set of transformations required to turn
    the first schema (reference) into the second one (comparison). For
    instance, if a column was added to the comparison schema, this would be
    reported under 'column_added' in the output. When values change, the old or
    reference value is reported under 'reference' and the new value under
    'comparison'.

    Example:

      sdm-tools compare-band-columns reference_schema.yaml comparison_schema.yaml
    """,
)
@click.argument("files", type=click.Path(exists=True), nargs=2, required=True)
@click.option(
    "--tables",
    "-t",
    "table_names",
    callback=_parse_comma_separated,
    help="Names of tables to check, comma-separated (DEFAULT: all tables)",
)
@click.option("--output-file", "-o", type=click.Path(), help="Output file for the diff report")
@click.option(
    "--bands",
    "-b",
    help=f"List of bands to compare, comma-separated (DEFAULT: {','.join(BANDS)})",
    callback=_parse_comma_separated,
    default=",".join(BANDS),
)
@click.option(
    "--error-on-differences",
    "-e",
    is_flag=True,
    help="Return an error if differences are found",
)
@click.option(
    "--ignore-description",
    "-i",
    is_flag=True,
    help="Ignore all differences in column descriptions",
)
@click.pass_context
def compare_band_columns(
    ctx: click.Context,
    files: list[str],
    table_names: list[str] = [],
    output_file: str | None = None,
    bands: list[str] = list(BANDS),
    error_on_differences: bool = False,
    ignore_description: bool = False,
) -> None:
    """Check Band Columns"""
    if len(bands) == 0:
        raise click.BadParameter("At least one band must be specified")
    try:
        checker = SchemaBandColumnComparator(
            files,
            table_names,
            bands=bands,
            output_path=output_file,
            error_on_differences=error_on_differences,
            ignore_description=ignore_description,
        )
        checker.run()
    except Exception as e:
        logger.error(e, exc_info=True)
        raise click.ClickException(str(e))


if __name__ == "__main__":
    cli()
