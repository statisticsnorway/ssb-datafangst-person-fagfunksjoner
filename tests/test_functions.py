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

    
def test_fill_para_pl_invalid_timestamp() -> None:
    # Create test data with an invalid TimeStamp column (e.g., not datetime or has nulls)
    df_test = pl.DataFrame(
        {
            "TimeStamp": [None, "2024-08-15 10:00:00", "2024-08-15 12:00:00"],  # Invalid: None and str
            "FieldName": ["field1", "field2", "field3"],
            "PageIndex": [1, 2, 3],
        }
    )

    # Attempt to call the function and catch the expected exception
    with pytest.raises(ValueError, match="TimeStamp is not a datetime column"):
        fill_para_pl(df_test)

        
def test_fill_para_pd_invalid_timestamp() -> None:
    # Create test data with an invalid TimeStamp column (e.g., not datetime or has nulls)
    df_test = pd.DataFrame(
        {
            "TimeStamp": [None, "2024-08-15 10:00:00", "2024-08-15 12:00:00"],  # Invalid: None and str
            "FieldName": ["field1", "field2", "field3"],
            "PageIndex": [1, 2, 3],
        }
    )

    # Attempt to call the function and catch the expected exception
    with pytest.raises(ValueError, match="TimeStamp is not datetime column"):
        fill_para_pd(df_test)


import pytest    
from ssb_datafangst_person_fagfunksjoner.functions import make_bolk  

@pytest.mark.parametrize(
    "input_row, expected_output",
    [
        ("skjema.bolk2[1].field", "bolk2"),  # Extract bolk name inside square brackets
        ("skjema.bolk2[2].field", "bolk2"),  # Repeated bolk
        ("skjema.bolk2.bolk3.bolk4", "bolk2.bolk3"),  # Nested bolks
        ("skjema.bolk1.field", "bolk1"),  # Single bolk
        ("random.string.without.match", "random.string.without.match"),  # No match
        ("skjema.bolk1 ", "bolk1"),  # Match with extra spaces
        ("", ""),  # Empty input
        (None, ""),  # None input
    ],
)
def test_make_bolk(input_row, expected_output):
    assert make_bolk(input_row) == expected_output