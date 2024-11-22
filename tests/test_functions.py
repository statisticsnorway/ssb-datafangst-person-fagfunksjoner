from ssb_datafangst_person_fagfunksjoner.functions import example_function


def test_example_function() -> None:
    assert example_function(1, 2) == "1 is less than 2"
    assert example_function(1, 0) == "1 is greater than or equal to 0"


from pathlib import Path

import pandas as pd
from pandas import testing as tm

from ssb_datafangst_person_fagfunksjoner.functions import question_sorting


def test_question_sorting() -> None:
    file = Path(__file__).parent / "testdata" / "testdata_result.parquet"
    df_test = pd.read_parquet(file)

    expected_result = ["question_1", "question_2"]

    result = question_sorting(df_test)

    # Assertion
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


from ssb_datafangst_person_fagfunksjoner.functions import fill_para_pd


def test_fill_para_pd() -> None:
    file = Path(__file__).parent / "testdata" / "testdata.parquet"
    df_test = pd.read_parquet(file)

    file_expected = Path(__file__).parent / "testdata" / "testdata_result.parquet"
    df_expected = pd.read_parquet(file_expected)

    result = fill_para_pd(df_test)

    # Assertion
    tm.assert_frame_equal(result, df_expected)


from ssb_datafangst_person_fagfunksjoner.functions import fill_para_pl
import polars as pl
from polars.testing import assert_frame_equal


def test_fill_para_pl() -> None:
    file = Path(__file__).parent / "testdata" / "testdata.parquet"
    df_test = pl.read_parquet(file)

    file_expected = Path(__file__).parent / "testdata" / "testdata_result.parquet"
    df_expected = pl.read_parquet(file_expected)

    result = fill_para_pl(df_test)

    # Assertion
    assert_frame_equal(result, df_expected)


from ssb_datafangst_person_fagfunksjoner.functions import fill_all_para_pl


def test_fill_all_para_pl() -> None:
    file = Path(__file__).parent / "testdata" / "testdata.parquet"
    df_test = pl.read_parquet(file)

    file_expected = Path(__file__).parent / "testdata" / "testdata_result.parquet"
    df_expected = pl.read_parquet(file_expected)

    result = fill_all_para_pl(df_test)

    # Assertion
    assert_frame_equal(result, df_expected)
