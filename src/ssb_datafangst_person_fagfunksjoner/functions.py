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
from datetime import date


def hent_status_pd(
    InstrumentId: str, start_dato: date, slutt_dato: date
) -> pd.DataFrame:
    """
    Fetches the status for a given instrument and date range.

    Parameters
    ----------
    instrument_id : str
        The identifier for the instrument.
    start_date : datetime.date
        The start date for filtering the status data.
    end_date : datetime.date
        The end date for filtering the status data.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing status information for the given instrument and date range.
    """

    fs = FileClient.get_gcs_file_system()

    filepath = fs.glob(
        f"gs://ssb-datafangst-person-data-produkt-prod/{InstrumentId}/status/*.parquet"
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


# def hent_status_pl(
#     InstrumentId: str, start_dato:date, slutt_dato:date
# ) -> pl.DataFrame:
#     """
#     Retrieves status data from GCS for a specified instrument within a given date range.

#     Parameters:
#     ----------
#     InstrumentId : str
#         The ID of the instrument to retrieve data for.
#     start_dato :date. Example:date(2024, 10, 29)
#         The start date of the range for filtering data.
#     slutt_dato :date. Example:date(2024, 10, 29)
#         The end date of the range for filtering data.

#     Returns:
#     -------
#     pl.DataFrame
#         A Polars DataFrame containing the status information for the specified
#         instrument within the defined date range

#     """

#     fs = FileClient.get_gcs_file_system()

#     filepath = fs.glob(
#         f"gs://ssb-datafangst-person-data-produkt-prod/{InstrumentId}/status/*.parquet"
#     )

#     filters = []
#     if start_dato:
#         filters.append(("TimeStamp", ">=", pd.Timestamp(start_dato)))

#     if slutt_dato:
#         filters.append(("TimeStamp", "<=", pd.Timestamp(slutt_dato)))

#     df = (
#         pq.ParquetDataset(filepath, filesystem=fs, filters=filters if filters else None)
#     ).read()

#     df = pl.from_arrow(df)

#     if "__index_level_0__" in df.columns:
#         df = df.drop("__index_level_0__")

#     return df


# def hent_utvalg_pd(InstrumentId: str) -> pd.DataFrame:
#     """
#     Retrieves utvalg data from GCS for a specified instrument.

#     Parameters:
#     ----------
#     InstrumentId : str
#         The ID of the instrument to retrieve data for.

#     Returns:
#     -------
#     pd.DataFrame
#         A Pandas DataFrame containing the utvalg information for the specified
#         instrument.

#     """

#     fs = FileClient.get_gcs_file_system()

#     filepath = fs.glob(
#         f"gs://ssb-datafangst-person-data-produkt-prod/{InstrumentId}/utvalg/*.parquet"
#     )

#     df = (
#         (
#             pq.ParquetDataset(
#                 filepath,
#                 filesystem=fs,
#             )
#         )
#         .read()
#         .to_pandas()
#     )

#     return df


# def hent_utvalg_pl(InstrumentId: str) -> pl.DataFrame:
#     """
#     Retrieves utvalg data from GCS for a specified instrument.

#     Parameters:
#     ----------
#     InstrumentId : str
#         The ID of the instrument to retrieve data for.

#     Returns:
#     -------
#     pl.DataFrame
#         A Polars DataFrame containing the utvalg information for the specified
#         instrument.

#     """

#     fs = FileClient.get_gcs_file_system()

#     filepath = fs.glob(
#         f"gs://ssb-datafangst-person-data-produkt-prod/{InstrumentId}/utvalg/*.parquet"
#     )

#     df = (
#         pq.ParquetDataset(
#             filepath,
#             filesystem=fs,
#         )
#     ).read()

#     df = pl.from_arrow(df)

#     if "__index_level_0__" in df.columns:
#         df = df.drop("__index_level_0__")

#     return df
