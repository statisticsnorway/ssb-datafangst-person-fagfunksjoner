# SSB Datafangst Person Fagfunksjoner

[![PyPI](https://img.shields.io/pypi/v/ssb-datafangst-person-fagfunksjoner.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/ssb-datafangst-person-fagfunksjoner.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/ssb-datafangst-person-fagfunksjoner)][pypi status]
[![License](https://img.shields.io/pypi/l/ssb-datafangst-person-fagfunksjoner)][license]

[![Documentation](https://github.com/statisticsnorway/ssb-datafangst-person-fagfunksjoner/actions/workflows/docs.yml/badge.svg)][documentation]
[![Tests](https://github.com/statisticsnorway/ssb-datafangst-person-fagfunksjoner/actions/workflows/tests.yml/badge.svg)][tests]
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-datafangst-person-fagfunksjoner&metric=coverage)][sonarcov]
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-datafangst-person-fagfunksjoner&metric=alert_status)][sonarquality]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)][poetry]

[pypi status]: https://pypi.org/project/ssb-datafangst-person-fagfunksjoner/
[documentation]: https://statisticsnorway.github.io/ssb-datafangst-person-fagfunksjoner
[tests]: https://github.com/statisticsnorway/ssb-datafangst-person-fagfunksjoner/actions?workflow=Tests

[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-datafangst-person-fagfunksjoner
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-datafangst-person-fagfunksjoner
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

## Features

Functions:

**hent_status_pd**
Retrieves status data for a specific InstrumentId and date range from datafangst-person GCS bucket, returning it as a Pandas DataFrame.

```
hent_status_pd(
    instrument_id: str,
    start_dato: Optional[date] = None,
    slutt_dato: Optional[date] = None,
) -> pd.DataFrame

```

**hent_status_pl**
Retrieves status data for a specific InstrumentId and date range from datafangst-person GCS bucket, returning it as a Polars DataFrame.

```
hent_status_pl(
    instrument_id: str,
    start_dato: Optional[date] = None,
    slutt_dato: Optional[date] = None,
) -> pl.DataFrame
```

**hent_utvalg_pd**
Retrieves utvalg data for a specific InstrumentId from datafangst-person GCS bucket, returning it as a Pandas DataFrame.

```
hent_utvalg_pd(
    instrument_id: str,
) -> pd.DataFrame
```

**hent_utvalg_pl**
Retrieves utvalg data for a specific InstrumentId from datafangst-person GCS bucket, returning it as a Polars DataFrame.

```
hent_utvalg_pl(
    instrument_id: str,
) -> pl.DataFrame
```

**question_sorting**
Processes a Paradata DataFrame returning a list of FieldNames in the order they were asked in the survey.

```
question_sorting(
    x: pd.DataFrame,
) -> list[str]
```

**make_bolk**
Extracts and returns a nested section (bolk) name from a string, such as FieldName.

```
make_bolk(
    row: str,
) -> str
```

**fill_all_para_pl**
Prepares a Polars DataFrame with paradata for analysis by filling missing values, creating new columns, and transforming the data for analysis.

```
fill_all_para_pl(
    table_df: pl.DataFrame,
) -> pl.DataFrame
```

**fill_para_pl**
Prepares a Polars DataFrame with paradata for analysis, transforming data as necessary if the data has a min TimeStamp after we started doing fill_para automatically in out iac repo.

```
fill_para_pl(
    table_df: pl.DataFrame,
) -> pl.DataFrame
```

**fill_para_pd**
Prepares a Pandas DataFrame with paradata for analysis, transforming data as necessary if the data has a min TimeStamp after we started doing fill_para automatically in out iac repo.

```
fill_para_pd(
    table_df: pd.DataFrame,
) -> pd.DataFrame
```

## Requirements

- TODO

## Installation

You can install _SSB Datafangst Person Fagfunksjoner_ via [pip] from [PyPI]:

```console
pip install ssb-datafangst-person-fagfunksjoner
```

## Usage

Please see the [Reference Guide] for details.

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_SSB Datafangst Person Fagfunksjoner_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [Statistics Norway]'s [SSB PyPI Template].

[statistics norway]: https://www.ssb.no/en
[pypi]: https://pypi.org/
[ssb pypi template]: https://github.com/statisticsnorway/ssb-pypitemplate
[file an issue]: https://github.com/statisticsnorway/ssb-datafangst-person-fagfunksjoner/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/statisticsnorway/ssb-datafangst-person-fagfunksjoner/blob/main/LICENSE
[contributor guide]: https://github.com/statisticsnorway/ssb-datafangst-person-fagfunksjoner/blob/main/CONTRIBUTING.md
[reference guide]: https://statisticsnorway.github.io/ssb-datafangst-person-fagfunksjoner/reference.html
