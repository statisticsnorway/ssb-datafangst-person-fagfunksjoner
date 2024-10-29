"""Command-line interface."""

import click


@click.command()
@click.version_option()
def main() -> None:
    """SSB Datafangst Person Fagfunksjoner."""


if __name__ == "__main__":
    main(prog_name="ssb-datafangst-person-fagfunksjoner")  # pragma: no cover
