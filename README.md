# SSB Datafangst Person Fagfunksjoner

Felles funksjonsbibliotek for S851

This package is maintained as a shared internal Python package and is installed
directly from GitHub. It is no longer published to PyPI.


## Requirements

- Python >=3.11,<4.0
- Access to relevant buckets in Dapla


## Installation

Add the package directly from GitHub with Poetry:
```console
poetry add "git+https://github.com/statisticsnorway/ssb-datafangst-person-fagfunksjoner.git"
```

To install a specific branch:
```console
poetry add "git+https://github.com/statisticsnorway/ssb-datafangst-person-fagfunksjoner.git@branch-name"
```

Import it as
```python
import ssb_datafangst_person_fagfunksjoner as ff
```

## Package structure

The package currently contains three modules:

- `functions.py` – general shared functions for working with Datafangst Person data  
  (all polars functions and some other functions have been commented out for now as part of the recent cleanup.
  They can be reviewed and restored later if needed.)
- `velg_skjema.py` – functions and widgets for selecting a survey and date range
- `klass_utils.py` – helper functions for mapping classifications using KLASS


## Functions

### Velg skjema.py
**velg_skjema** displays widgets for selecting a survey and optional start and end dates. It returns the current selection as a dictionary containing the survey name, InstrumentId, and selected dates.
```markdown
```python
valg = ff.velg_skjema()
```
```returns
valg["InstrumentId"]
valg["skjemanavn"]
valg["start_date"]
valg["end_date"]
```

### klass_utils.py

**add_klass_mapping** helper functions for looking up KLASS classifications and adding mappings between classifications to Pandas DataFrames.
```python
df = ff.add_klass_mapping(
    df,
    column,
    source,
    target,
    date,
    output="code",
    new_column="klass"
)
```
Eksempel på bruk - hente fylke fra kommunenummer
df = ff.add_klass_mapping(
    df,
    column="municipalityNumber",
    source="Standard for kommuneinndeling",
    target="Standard for fylkesinndeling",
    date="2026-09-03",
    output="code",
    new_column="fylke"
)


### functions.py
**hent_status_pd** Retrieves status data for a specific InstrumentId and date range from datafangst-person GCS bucket, returning it as a Pandas DataFrame.
```python
hent_status_pd(
    instrument_id: str,
    start_dato: date | None = None,
    slutt_dato: date | None = None,
) -> pd.DataFrame
```

**hent_utvalg_pd** Retrieves utvalg data for a specific InstrumentId from datafangst-person GCS bucket, returning it as a Pandas DataFrame.
```python
hent_utvalg_pd(instrument_id: str) -> pd.DataFrame
```

**hent_ringedata** Retrieves utvalg data for a specific InstrumentId from datafangst-person GCS bucket, returning it as a Pandas DataFrame.
```python
hent_ringedata(
    instrumentId: str,
    start_dato: date | None = None,
    slutt_dato: date | None = None,
) -> pd.DataFrame
```

**question_sorting** Processes a Paradata DataFrame returning a list of FieldNames in the order they were asked in the survey.
```python
question_sorting(x: pd.DataFrame) -> list[str]
```

**make_bolk** Extracts and returns a nested section (bolk) name from a string, such as FieldName.
```python
make_bolk(row: str) -> str
```

**fill_para_pd** repares a Pandas DataFrame with paradata for analysis, transforming data as necessary if the data has a min TimeStamp after we started doing fill_para automatically in out iac repo.
```python
fill_para_pd(table_df: pd.DataFrame) -> pd.DataFrame
```


# BØH!