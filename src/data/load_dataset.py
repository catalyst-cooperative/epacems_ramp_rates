# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Sequence, Optional

import pandas as pd
import pudl


EPA_CROSSWALK_RELEASE = "https://github.com/USEPA/camd-eia-crosswalk/releases/download/v0.2.1/"


def load_epacems(
    states: Optional[Sequence[str]] = ("CO",),
    years: Optional[Sequence[int]] = (2019,),
    columns: Optional[Sequence[str]] = (
        "unit_id_epa",
        "plant_id_eia",
        # "unitid",
        "operating_datetime_utc",
        "operating_time_hours",
        "gross_load_mw",
        "steam_load_1000_lbs",
        "state",
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
        states = pudl.constants.us_states.keys()  # all states
    else:
        states = list(states)
    if years is None:
        years = pudl.constants.data_years["epacems"]  # all years
    else:
        years = list(years)
    if columns is not None:
        # columns=None is handled by pd.read_parquet, gives all columns
        columns = list(columns)
    if engine != "pandas":
        raise NotImplementedError("dask engine not yet implemented. Only pandas")

    pudl_settings = pudl.workspace.setup.get_defaults()
    cems_path = Path(pudl_settings["parquet_dir"]) / "epacems"
    cems = pd.read_parquet(
        cems_path,
        use_nullable_dtypes=True,
        columns=columns,
        filters=pudl.output.epacems.year_state_filter(
            states=states,
            years=years,
        ),
    )
    return cems


def load_epa_crosswalk():
    return pd.read_csv(EPA_CROSSWALK_RELEASE + "epa_eia_crosswalk.csv")
