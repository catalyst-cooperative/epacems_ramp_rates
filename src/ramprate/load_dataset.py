# -*- coding: utf-8 -*-
# from pathlib import Path
from typing import Sequence, Optional
import itertools
from os import getenv

import pandas as pd

# from makefile:install
EPA_CEMS_DATA_PATH = getenv("EPA_CEMS_DATA_PATH")

EPA_CROSSWALK_RELEASE = "https://github.com/USEPA/camd-eia-crosswalk/releases/download/v0.2.1/"

ALL_STATES = (  # includes territories and DC
    "AK",
    "AL",
    "AR",
    "AS",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "GU",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MP",
    "MS",
    "MT",
    "NA",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "PR",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VI",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
)

ALL_CEMS_YEARS = range(1995, 2020)


def year_state_filter(years=(), states=()):
    """
    Create filters to read given years and states from partitioned parquet dataset.

    This function was the only dependency on the pudl repo, so I copied it from
    pudl.outputs.epacems.py:year_state_filter

    A subset of an Apache Parquet dataset can be read in more efficiently if files
    which don't need to be queried are avoideed. Some datasets are partitioned based
    on the values of columns to make this easier. The EPA CEMS dataset which we
    publish is partitioned by state and report year.

    This function takes a set of years, and a set of states, and returns a list of lists
    of tuples, appropriate for use with the read_parquet() methods of pandas and dask
    dataframes.

    Args:
        years (iterable): 4-digit integers indicating the years of data you would like
            to read. By default it includes all years.
        states (iterable): 2-letter state abbreviations indicating what states you would
            like to include. By default it includes all states.

    Returns:
        list: A list of lists of tuples, suitable for use as a filter in the
        read_parquet method of pandas and dask dataframes.

    """
    year_filters = [("year", "=", year) for year in years]
    state_filters = [("state", "=", state.upper()) for state in states]

    if states and not years:
        filters = [
            [
                tuple(x),
            ]
            for x in state_filters
        ]
    elif years and not states:
        filters = [
            [
                tuple(x),
            ]
            for x in year_filters
        ]
    elif years and states:
        filters = [list(x) for x in itertools.product(year_filters, state_filters)]
    else:
        filters = None

    return filters


def load_epacems(
    states: Optional[Sequence[str]] = ("CO",),
    years: Optional[Sequence[int]] = (2019,),
    columns: Optional[Sequence[str]] = (
        "plant_id_eia",
        "unitid",
        "operating_datetime_utc",
        # "operating_time_hours",
        "gross_load_mw",
        # "steam_load_1000_lbs",
        # "so2_mass_lbs",
        # "so2_mass_measurement_code",
        # "nox_rate_lbs_mmbtu",
        # "nox_rate_measurement_code",
        # "nox_mass_lbs",
        # "nox_mass_measurement_code",
        # "co2_mass_tons",
        # "co2_mass_measurement_code",
        # "heat_content_mmbtu",
        # "facility_id",
        "unit_id_epa",
        # "year",
        # "state",
    ),
    engine: Optional[str] = "pandas",
) -> pd.DataFrame:
    """load EPA CEMS data from PUDL with optional subsetting

    Args:
        states (Optional[Sequence[str]], optional): subset by state abbreviation. Pass None to get all states. Defaults to ("CO",).
        years (Optional[Sequence[int]], optional): subset by year. Pass None to get all years. Defaults to (2019,).
        columns (Optional[Sequence[str]], optional): subset by column. Pass None to get all columns. Defaults to ( "plant_id_eia", "unitid", "operating_datetime_utc", "operating_time_hours", "gross_load_mw", "state", ).
        engine (Optional[str], optional): choose 'pandas' or 'dask'. Defaults to 'pandas'

    Returns:
        pd.DataFrame: epacems data
    """
    if states is None:
        states = list(ALL_STATES)
        # states = pudl.constants.us_states.keys()  # all states
    else:
        states = list(states)
    if years is None:
        years = list(ALL_CEMS_YEARS)
        # years = pudl.constants.data_years["epacems"]  # all years
    else:
        years = list(years)
    if columns is not None:
        # columns=None is handled by pd.read_parquet, gives all columns
        columns = list(columns)
    if engine != "pandas":
        raise NotImplementedError("dask engine not yet implemented. Only pandas")

    # pudl_settings = pudl.workspace.setup.get_defaults()
    # cems_path = Path(pudl_settings["parquet_dir"]) / "epacems"
    cems = pd.read_parquet(
        # cems_path,
        EPA_CEMS_DATA_PATH,
        use_nullable_dtypes=True,
        columns=columns,
        # filters=pudl.output.epacems.year_state_filter(
        filters=year_state_filter(
            states=states,
            years=years,
        ),
    )
    return cems


def load_epa_crosswalk():
    return pd.read_csv(EPA_CROSSWALK_RELEASE + "epa_eia_crosswalk.csv")
