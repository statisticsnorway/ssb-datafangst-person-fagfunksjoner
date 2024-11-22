def example_function(number1: int, number2: int) -> str:
    """Compare two integers.
    This is merely an example function can be deleted. It is used to show and test generating
    documentation from code, type hinting, testing, and testing examples
    in the code.
    Args:
        number1: The first number.
        number2: The second number, which will be compared to number1.
    Returns:
        A string describing which number is the greatest.
    Examples:
        Examples should be written in doctest format, and should illustrate how
        to use the function.
        >>> example_function(1, 2)
        1 is less than 2
    """
    if number1 < number2:
        return f"{number1} is less than {number2}"
    return f"{number1} is greater than or equal to {number2}"


import pandas as pd
from dapla import FileClient
import pyarrow.parquet as pq
import polars as pl
from datetime import date, datetime
from typing import Optional


def hent_status_pd(
    instrument_id: str,
    start_dato: Optional[date] = None,
    slutt_dato: Optional[date] = None,
) -> pd.DataFrame:
    """
    Retrieves status data from GCS for a specified instrument within a given date range.

    Parameters
    ----------
    instrument_id : str
        The ID of the instrument to retrieve data for.
    start_dato : datetime.date. Example: datetime.date(2024, 10, 29)
        The start date of the range for filtering data.
    slutt_dato : datetime.date. Example: datetime.date(2024, 10, 29)
        The end date of the range for filtering data.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame containing the status information for the specified
        instrument within the defined date range

    """

    fs = FileClient.get_gcs_file_system()

    filepath = fs.glob(
        f"gs://ssb-datafangst-person-data-produkt-prod/{instrument_id}/status/*.parquet"
    )

    filters = []
    if start_dato:
        filters.append(("TimeStamp", ">=", pd.Timestamp(start_dato)))

    if slutt_dato:
        filters.append(("TimeStamp", "<=", pd.Timestamp(slutt_dato)))

    df = (
        (
            pq.ParquetDataset(
                filepath, filesystem=fs, filters=filters if filters else None
            )
        )
        .read()
        .to_pandas()
    )

    return pd.DataFrame(df)


def hent_status_pl(
    instrument_id: str,
    start_dato: Optional[date] = None,
    slutt_dato: Optional[date] = None,
) -> pl.DataFrame:
    """
    Retrieves status data from GCS for a specified instrument within a given date range.

    Parameters
    ----------
    instrument_id : str
        The ID of the instrument to retrieve data for.
    start_dato : datetime.date
        The start date of the range for filtering data. Example: datetime.date(2024, 10, 29)
    slutt_dato : datetime.date
        The end date of the range for filtering data. Example: datetime.date(2024, 10, 29)

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame containing the status information for the specified
        instrument within the defined date range.
    """

    fs = FileClient.get_gcs_file_system()

    filepath = fs.glob(
        f"gs://ssb-datafangst-person-data-produkt-prod/{instrument_id}/status/*.parquet"
    )

    filters = []
    if start_dato:
        filters.append(("TimeStamp", ">=", pd.Timestamp(start_dato)))

    if slutt_dato:
        filters.append(("TimeStamp", "<=", pd.Timestamp(slutt_dato)))

    df = (
        pq.ParquetDataset(filepath, filesystem=fs, filters=filters if filters else None)
    ).read()

    df = pl.from_arrow(df)

    if "__index_level_0__" in df.columns:
        df = df.drop("__index_level_0__")

    return pl.DataFrame(df)


def hent_utvalg_pd(instrument_id: str) -> pd.DataFrame:
    """
    Retrieves utvalg data from GCS for a specified instrument.

    Parameters
    ----------
    instrument_id : str
        The ID of the instrument to retrieve data for.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame containing the utvalg information for the specified
        instrument.
    """

    fs = FileClient.get_gcs_file_system()

    filepath = fs.glob(
        f"gs://ssb-datafangst-person-data-produkt-prod/{instrument_id}/utvalg/*.parquet"
    )

    df = (
        (
            pq.ParquetDataset(
                filepath,
                filesystem=fs,
            )
        )
        .read()
        .to_pandas()
    )

    return pd.DataFrame(df)


def hent_utvalg_pl(instrument_id: str) -> pl.DataFrame:
    """
    Retrieves utvalg data from GCS for a specified instrument.

    Parameters
    ----------
    instrument_id : str
        The ID of the instrument to retrieve data for.

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame containing the utvalg information for the specified
        instrument.
    """

    fs = FileClient.get_gcs_file_system()

    filepath = fs.glob(
        f"gs://ssb-datafangst-person-data-produkt-prod/{instrument_id}/utvalg/*.parquet"
    )

    df = (
        pq.ParquetDataset(
            filepath,
            filesystem=fs,
        )
    ).read()

    df = pl.from_arrow(df)

    if "__index_level_0__" in df.columns:
        df = df.drop("__index_level_0__")

    return pl.DataFrame(df)


def question_sorting(x: pd.DataFrame) -> list[str]:
    """
    Retrieves utvalg data from GCS for a specified instrument.

    Parameters
    ----------
    instrument_id : str
        The ID of the instrument to retrieve data for.

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame containing the utvalg information for the specified
        instrument.
    """

    layout_set_names = [
        "CASI-SSB_Small_Touch",
        "SSB_Small_Touch",
        "CASI-SSB_Large",
        "SSB_Small_Large",
    ]

    if "LayoutSetName" in x.columns.tolist():
        if any(x["LayoutSetName"].isin(layout_set_names)):
            filtered_beer = x[x["LayoutSetName"].isin(layout_set_names)]
    else:
        if any(x["LayoutSetName"].isin(layout_set_names)):
            filtered_beer = x[x["LayoutSetName"].isin(layout_set_names)]
        else:
            filtered_beer = x

    one_pint = (
        filtered_beer.assign(PageIndex=lambda x: x["PageIndex"].astype("Int64"))
        .dropna(subset=["FieldName", "PageIndex"])  # fjerner missing verdier
        .drop_duplicates(subset=["FieldName"])  # fjerner duplikater
        .sort_values("PageIndex")  # sorterer etter kolonnen PageIndex
        .filter(items=["FieldName"])["FieldName"]  # Velger ut bare en kolonne
        .tolist()
    )

    # tar bort cati bolker og innlogging-bolker på casi
    ikkebolker = [
        "io_idnr",
        "passord",
        "intro",
        "_nonresponsenrpersfrafallsgrunn",
        "innled",
        "_nonresponsenrpersoverforingsgrunn",
        "samtykke",
        "_nonresponsenrintmelding",
        "_nonresponsenrintslutt",
        "_nonresponsenrpersavgangsgrunn",
        "_nonresponse.nrinnled",
        "_nonresponse.nrpersavgangsgrunn",
        "_nonresponse.nrspraakoppf",
        "_nonresponse.nrpersfrafallsgrunn",
        "_nonresponse.nrpersoverforingsgrunn",
        "_nonresponse.nrintslutt",
        "_nonresponse.nrintmelding",
        "casibesvart",
        "_nonresponse.nrpersoverforingsgrunn",
        "_nonresponse.nrinnled",
        "_nonresponse.nrpersfrafallsgrunn",
        "_nonresponse.nrintmelding",
        "_nonresponse.nrspraakoppf",
        "_nonresponse.nrintslutt",
    ]

    field_names = [x for x in one_pint if x not in ikkebolker]

    return field_names


import pandas as pd
from typing import Optional


def make_bolk(row: str) -> str:
    """
    En funksjon som kan brukes med map eller apply som tar en string, FieldName, og returnerer bolk navn.
    Denne funksjonen tar med om IO har bolk inni bolk som repeteres.
    Eksempel:  skjema.bolk2[1].field og skjema.bolk2[2].field blir til bolk1.bolk2.
    Eksempel: skjema.bolk2.bolk3.bolk4 blir bolk2.bolk3
    """
    import re

    if not row or row is None:
        return ""
    else:
        row = str(row)
        if row == "":
            return ""
        elif (match := re.search(r"\.(.*?)\[", row)) is not None:
            return match.group(1)
        elif (match := re.search(r"skjema\.([^\.]+(?:\.[^\.]+))\.", row)) is not None:
            return match.group(1)
        elif (match := re.search("skjema\.\s*(\w+)", row)) is not None:
            return match.group(1)
        else:
            return row


def fill_all_para_pl(table_df: pl.DataFrame) -> pl.DataFrame:
    """
    param: polars dataframe
    output: prepared polars dataframe for analysis
    The function prepares table_df for data analysis:
    - fills PageIndex downward
    - fills FieldName downward
    - creates a new variable with VariableName
    - creates variable diff_time which represents time spent on each observation/action for an IO.
    - Fills LayoutSetName by session id
    - creates a new column with bolk name using the function make_bolk()
    """

    # Sorting rows by timestamp and SessionId. IO will appear in the order based on timestamp when they responded, and rows for an IO will be sorted by timestamp.
    table_df = table_df.sort(["SessionId", "TimeStamp"])

    # fill PageIndex downward
    table_df = table_df.with_columns(
        pl.col("PageIndex").forward_fill().over("SessionId")
    )

    # Grouping by PageIndex and filling FieldName downward and upward. Thus, FieldName will stay on the same side until a different FieldName appears on the side.
    table_df = table_df.with_columns(
        pl.col("FieldName").forward_fill().over("PageIndex")
    )
    table_df = table_df.with_columns(
        pl.col("FieldName").backward_fill().over("PageIndex")
    )

    # Creating a new variable from FieldName where the last word is the variable name
    table_df = table_df.with_columns(
        pl.col("FieldName").str.split(by=".").list.last().alias("VariableName")
    )

    # Creating diff_time which is time per question
    table_df = table_df.with_columns(
        (
            pl.col("TimeStamp").diff().over("SessionId").dt.total_milliseconds() * 0.001
        ).alias("diff_time")
    )

    # If cati, grouping by sessionid and filling InterviewerId downward and upward as well
    if "InterviewerId" in table_df.columns:
        table_df = table_df.with_columns(
            pl.col("InterviewerId").forward_fill().backward_fill().over("SessionId")
        )
        table_df = table_df.with_columns(
            pl.when(pl.col("InterviewerId").is_not_null())
            .then(pl.lit("CATI"))
            .otherwise(pl.lit("CASI"))
            .alias("Mode")
        )

    else:
        table_df = table_df.with_columns(pl.lit("CASI").alias("Mode"))

    # Grouping by sessionid and filling LayoutSetName downward and upward
    table_df = table_df.with_columns(
        pl.col("LayoutSetName").forward_fill().over("SessionId")
    )
    table_df = table_df.with_columns(
        pl.col("LayoutSetName").backward_fill().over("SessionId")
    )

    # Creating bolk variable
    table_df = table_df.with_columns(
        pl.col("FieldName")
        .map_elements(make_bolk, return_dtype=pl.String)
        .alias("Bolk")
    )

    return pl.DataFrame(table_df)


from polars.datatypes import Datetime


def fill_para_pl(table_df: pl.DataFrame) -> pl.DataFrame:
    """
    param: pandas dataframe
    output: prepared pandas dataframe for analysis
    The function prepares table_df for data analysis:
    - fills PageIndex downward
    - fills FieldName downward
    - creates a new variable with VariableName
    - creates variable diff_time which represents time spent on each observation/action for an IO.
    - Fills LayoutSetName by session id
    - creates a new column with bolk name using the function make_bolk()
    """

    # Dato vi la til fillpara i synk
    if (
        table_df.schema["TimeStamp"] == Datetime
        and table_df["TimeStamp"].is_not_null().all()
    ):
        # Ensure the column is a datetime type and handle nulls
        if str(table_df["TimeStamp"].min()) < str(datetime(2024, 8, 16)):
            table_df = fill_all_para_pl(table_df)
            return pl.DataFrame(table_df)
        else:
            return pl.DataFrame(table_df)
    else:
        raise ValueError("TimeStamp is not a datetime column")


from pandas.api.types import is_datetime64_any_dtype


def fill_para_pd(table_df: pd.DataFrame) -> pd.DataFrame:
    """
    param: pandas dataframe
    output: prepared pandas dataframe for analysis
    The function prepares table_df for data analysis:
    - fills PageIndex downward
    - fills FieldName downward
    - creates a new variable with VariableName
    - creates variable diff_time which represents time spent on each observation/action for an IO.
    - Fills LayoutSetName by session id
    - creates a new column with bolk name using the function make_bolk()
    """

    # Check if "TimeStamp" is a datetime type
    # Dato vi la til fillpara i synk
    if is_datetime64_any_dtype(table_df["TimeStamp"]):
        if str(table_df["TimeStamp"].min()) < str(datetime(2024, 8, 16)):

            polars_table_df = pl.from_pandas(table_df)

            polars_table_df = fill_all_para_pl(polars_table_df)

            pandas_df = polars_table_df.to_pandas()

            return pd.DataFrame(pandas_df)
        else:
            return pd.DataFrame(table_df)
    else:
        raise ValueError("TimeStamp is not datetime column")
