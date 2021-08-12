# -*- coding: utf-8 -*-
from typing import Dict, Optional, Sequence, Union, Tuple, Any, Union

import pandas as pd
import dask.dataframe as dd
import numpy as np
import networkx as nx


idx = pd.IndexSlice

CAMD_FUEL_MAP = {
    "Pipeline Natural Gas": "gas",
    "Coal": "coal",
    "Diesel Oil": "oil",
    "Natural Gas": "gas",
    "Process Gas": "gas",
    "Residual Oil": "oil",
    "Other Gas": "gas",
    "Wood": "other",
    "Other Oil": "oil",
    "Coal Refuse": "coal",
    "Petroleum Coke": "oil",
    "Tire Derived Fuel": "other",
    "Other Solid Fuel": "other",
}
EIA_FUEL_MAP = {
    "AB": "other",
    "ANT": "coal",
    "BFG": "gas",
    "BIT": "coal",
    "BLQ": "other",
    "CBL": "Coal",
    "DFO": "oil",
    "JF": "oil",
    "KER": "oil",
    "LFG": "gas",
    "LIG": "coal",
    "MSB": "other",
    "MSN": "other",
    "MSW": "other",
    "MWH": "other",
    "NG": "gas",
    "OBG": "gas",
    "OBL": "other",
    "OBS": "other",
    "OG": "gas",
    "OTH": "other",
    "PC": "oil",
    "PG": "gas",
    "PUR": "other",
    "RC": "coal",
    "RFO": "oil",
    "SC": "coal",
    "SGC": "gas",
    "SGP": "gas",
    "SLW": "other",
    "SUB": "coal",
    "SUN": "gas",  # mis-categorized gas plants with 'solar' in the name
    "TDF": "other",
    "WC": "coal",
    "WDL": "other",
    "WDS": "other",
    "WH": "other",
    "WO": "oil",
}
TECH_TYPE_MAP = {
    frozenset({"ST"}): "steam_turbine",
    frozenset({"GT"}): "gas_turbine",
    frozenset({"CT"}): "combined_cycle",  # in 2019 about half of solo CTs might be GTs
    # Could classify by operational characteristics but there were only 20 total so didn't bother
    frozenset({"CA"}): "combined_cycle",
    frozenset({"CS"}): "combined_cycle",
    frozenset({"IC"}): "internal_combustion",
    frozenset({"CT", "CA"}): "combined_cycle",
    frozenset({"ST", "GT"}): "combined_cycle",  # I think industrial cogen or mistaken
    frozenset({"CA", "GT"}): "combined_cycle",  # most look mistaken
    frozenset({"CT", "CA", "ST"}): "combined_cycle",  # most look mistaken
}


# duration of exclusion zone around startup/shutdown
# values are based on plots in cell 78 of notebook 5.0
# https://github.com/catalyst-cooperative/epacems_ramp_rates/blob/main/notebooks/5.0-tb-one_to_one_ramp_rates_by_plant_type.ipynb
EXCLUSION_SIZE_HOURS = {
    "steam_turbine": 5,
    "combined_cycle": 7,
    "gas_turbine": -1,  # no exclusions
    "internal_combustion": -1,  # no exclusions
}


def _find_uptime(
    ser: pd.Series, multiindex_key: Optional[Union[str, int]] = None, downtime: bool = False
) -> pd.DataFrame:
    """summarize contiguous subsequences of non-zero values in a generation time series

    Args:
        ser (pd.Series): pandas series with datetime index
        multiindex_key (Optional[Union[str, int]], optional): if not None, assign new multiindex level to output. Used in manual groupby loops. Defaults to None.
        downtime (bool, optional): rearrange output events to refer to downtime instead of uptime. Defaults to False.

    Raises:
        NotImplementedError: whens ser has multiindex

    Returns:
        pd.DataFrame: table of events, with shutdown and startup timestamps
    """
    # TODO: all this multiindex stuff could be a separate wrapper function
    if isinstance(ser.index, pd.MultiIndex):
        if multiindex_key is None:
            raise NotImplementedError(
                "groupby functionality not yet implemented. Pass multiindex_key manually per group"
            )
        else:
            names = ser.index.names
            ser = ser.copy()
            ser.index = ser.index.droplevel(0)  # for groupby
    # else single plant

    # binarize and find edges
    diff = ser.gt(0).astype(np.int8).diff()

    # last zero value of a block
    # shift(-1) to select last zero instead of first non-zero
    startups = ser.index[diff.shift(-1) == 1]

    # first zero value of a block
    # no shift needed for this side
    shutdowns = ser.index[diff == -1]

    generator_starts_with_zero = ser.iat[0] == 0
    generator_ends_with_zero = ser.iat[-1] == 0

    events = {}
    nan = pd.Series([pd.NaT]).dt.tz_localize("UTC")

    # Events (uptime or downtime) are defined as having a start and end.
    # If the start (or end) of an event occurs outside the data
    # period, it is marked with pd.NaT

    # NOTE: 'startup' refers to generators transitioning from
    # zero power to positive power.
    # For downtime=True, this can be confusing because
    # 'startup' then indicates the END of a downtime event
    # Vice versa for 'shutdown', which indicates
    # the START of a downtime block.

    # The difference between downtime=False and downtime=True
    # is the shutdown timestamps are shifted 1 row,
    # plus some NaT accounting on the ends,
    # and the shutdown/startup column order is switched to
    # reflect the opposite begin/end convention for the events

    if downtime:  # events table refers to downtime periods (blocks of zeros)
        if (
            generator_starts_with_zero
        ):  # first downtime period has unknown shutdown time, known startup
            events["shutdown"] = nan.append(pd.Series(shutdowns), ignore_index=True)
        else:  # first downtime period is fully defined
            events["shutdown"] = shutdowns

        if generator_ends_with_zero:  # last downtime period has known shutdown but unknown startup
            events["startup"] = pd.Series(startups).append(nan, ignore_index=True)
        else:  # last downtime period is fully defined
            events["startup"] = startups

    else:  # events table refers to uptime periods (blocks of non-zeros)
        if generator_starts_with_zero:  # first uptime period is fully defined
            events["startup"] = startups
        else:  # first uptime period has unknown startup time, known shutdown
            events["startup"] = nan.append(pd.Series(startups), ignore_index=True)

        if generator_ends_with_zero:  # last uptime period is fully defined
            events["shutdown"] = shutdowns
        else:  # last uptime period has known startup but unknown shutdown
            events["shutdown"] = pd.Series(shutdowns).append(nan, ignore_index=True)

    if multiindex_key is None:
        return pd.DataFrame(events)
    else:
        events = pd.DataFrame(events)
        events.index = pd.MultiIndex.from_arrays(
            [np.full(len(events), multiindex_key), np.arange(len(events))],
            names=[names[0], "event"],
        )
        return events


def _binarize(ser: pd.Series):
    """modularize this in case I want to do more smoothing later"""
    return ser.gt(0).astype(np.int8)


def _drop_cols_no_copy(dask_df: dd.DataFrame, cols: Sequence[str]) -> dd.DataFrame:
    """.drop with no copy. No inplace option in dask"""
    keep = [col for col in dask_df.columns if col not in set(cols)]
    return dask_df.loc[:, keep]


def _find_edges(cems: pd.DataFrame, drop_intermediates=True) -> None:
    """find timestamps of startups and shutdowns based on transition from zero to non-zero generation"""
    cems["binarized"] = _binarize(cems["gross_load_mw"])
    # for each unit, find change points from zero to non-zero production
    # this could be done with groupby but it is much slower
    # cems.groupby(level='unit_id_epa')['binarized_col'].transform(lambda x: x.diff())
    cems["binary_diffs"] = (
        cems["binarized"].diff().where(cems["unit_id_epa"].diff().eq(0))
    )  # dont take diffs across units
    cems["shutdowns"] = cems["operating_datetime_utc"].where(cems["binary_diffs"] == -1, pd.NaT)
    cems["startups"] = cems["operating_datetime_utc"].where(cems["binary_diffs"] == 1, pd.NaT)
    if drop_intermediates:
        cems = _drop_cols_no_copy(cems, {"binarized", "binary_diffs"})
    return None


def _distance_from_downtime(
    cems: pd.DataFrame, drop_intermediates=True, boundary_offset_hours: int = 24
) -> None:
    """calculate two columns: the number of hours to the next shutdown; and from the last startup"""
    # fill startups forward and shutdowns backward
    # Note that this leaves NaT values for any uptime periods at the very start/end of the timeseries
    # The second fillna handles this by assuming the real boundary is the edge of the dataset + an offset
    offset = pd.Timedelta(boundary_offset_hours, unit="h")
    cems["startups"] = (
        cems["startups"]
        .groupby(level="unit_id_epa")
        .transform(lambda x: x.ffill().fillna(x.index[0][1] - offset))
    )
    cems["shutdowns"] = (
        cems["shutdowns"]
        .groupby(level="unit_id_epa")
        .transform(lambda x: x.bfill().fillna(x.index[-1][1] + offset))
    )

    cems["hours_from_startup"] = (
        cems["operating_datetime_utc"]
        .sub(cems["startups"])
        .dt.total_seconds()
        .div(3600)
        .astype(np.float32)
    )
    # invert sign so distances are all positive
    cems["hours_to_shutdown"] = (
        cems["shutdowns"]
        .sub(cems["operating_datetime_utc"])
        .dt.total_seconds()
        .div(3600)
        .astype(np.float32)
    )
    if drop_intermediates:
        cems = _drop_cols_no_copy(cems, {"startup", "shutdown"})
    return None


def calc_distance_from_downtime(
    cems: pd.DataFrame, classify_startup=False, drop_intermediates=True
) -> None:
    """calculate two columns: the number of hours to the next shutdown; and from the last startup"""
    # in place
    _find_edges(cems, drop_intermediates)
    _distance_from_downtime(cems, drop_intermediates)
    cems["hours_distance"] = cems[["hours_from_startup", "hours_to_shutdown"]].min(axis=1)
    if classify_startup:
        cems["nearest_to_startup"] = cems["hours_from_startup"] < cems["hours_to_shutdown"]
        # randomly allocate midpoints
        rng = np.random.default_rng(seed=42)
        rand_midpoints = (cems["hours_from_startup"] == cems["hours_to_shutdown"]) & rng.choice(
            np.array([True, False]), size=len(cems)
        )
        cems.loc[rand_midpoints, "nearest_to_startup"] = True
    return None


def uptime_events(cems: pd.DataFrame, infer_boundaries=True) -> pd.DataFrame:
    """convert timeseries of generation to a table of uptime events"""
    units = cems.groupby(level="unit_id_epa")
    event_dfs = []
    for grp, df in units["gross_load_mw"]:
        event_dfs.append(_find_uptime(df, multiindex_key=grp))
    events = pd.concat(event_dfs)

    if infer_boundaries:
        # if a timeseries starts (or ends) with uptime, the first (last) boundary is outside our data range.
        # This method uses the first (last) timestamp as the boundary: a lower bound on duration.
        for col, boundary in {"startup": "first", "shutdown": "last"}.items():
            # __getattr__ doesn't work here
            boundary_timestamps = units.__getattribute__(boundary)()[["operating_datetime_utc"]]
            joined_timestamps = events.join(boundary_timestamps, on="unit_id_epa", how="left")[
                "operating_datetime_utc"
            ]
            events[col].fillna(joined_timestamps, inplace=True)

    events["duration_hours"] = (
        events["shutdown"].sub(events["startup"]).dt.total_seconds().div(3600)
    )
    return events


def _filter_retirements(crosswalk: pd.DataFrame, year_range: Tuple[int, int]) -> pd.DataFrame:
    """remove retired or not-yet-existing units that have zero overlap with year_range"""
    # index 24 hours inside the min/max to avoid timezone shenanigans
    # min_year = cems.index.levels[1][24].year
    # max_year = cems.index.levels[1][-24].year
    min_year = year_range[0]
    max_year = year_range[1]

    not_retired_before_start = crosswalk["CAMD_RETIRE_YEAR"].replace(0, 3000) >= min_year
    not_built_after_end = (
        pd.to_datetime(crosswalk["CAMD_STATUS_DATE"]).dt.year <= max_year
    ) & crosswalk["CAMD_STATUS"].ne("RET")
    return crosswalk.loc[not_retired_before_start & not_built_after_end]


def _remove_irrelevant(crosswalk: pd.DataFrame):
    """remove unmatched or excluded (non-exporting) units"""
    bad = crosswalk["MATCH_TYPE_GEN"].isin({"CAMD Unmatched", "Manual CAMD Excluded"})
    return crosswalk.loc[~bad]


def _make_surrogate_ids(xwalk: pd.DataFrame) -> pd.DataFrame:
    """Prep crosswalk for networkx by making surrogate IDs. Networkx can't handle composite keys."""
    filtered = xwalk.copy()
    filtered["combustor_id"] = filtered.groupby(by=["CAMD_PLANT_ID", "CAMD_UNIT_ID"]).ngroup()
    # node IDs can't overlap so add (max + 1)
    filtered["generator_id"] = (
        filtered.groupby(by=["CAMD_PLANT_ID", "EIA_GENERATOR_ID"]).ngroup()
        + filtered["combustor_id"].max()
        + 1
    )
    return filtered


def _subcomponent_ids_from_prepped_crosswalk(prepped: pd.DataFrame) -> pd.DataFrame:
    graph = nx.from_pandas_edgelist(
        prepped,
        source="combustor_id",
        target="generator_id",
        edge_attr=True,
    )
    for i, node_set in enumerate(nx.connected_components(graph)):
        subgraph = graph.subgraph(node_set)
        assert nx.algorithms.bipartite.is_bipartite(
            subgraph
        ), f"non-bipartite: i={i}, node_set={node_set}"
        nx.set_edge_attributes(subgraph, name="component_id", values=i)
    return nx.to_pandas_edgelist(graph)


def make_subcomponent_ids(
    xwalk: pd.DataFrame, cems: Union[pd.DataFrame, dd.DataFrame]
) -> pd.DataFrame:
    if isinstance(cems, dd.DataFrame):
        gb_args = dict(by="unit_id_epa", group_keys=False)
    else:
        gb_args = dict(level="unit_id_epa")
    key_map = cems.groupby(**gb_args)[["plant_id_eia", "unitid", "unit_id_epa"]].first()
    key_map = key_map.merge(
        xwalk,
        left_on=["plant_id_eia", "unitid"],
        right_on=["CAMD_PLANT_ID", "CAMD_UNIT_ID"],
        how="inner",
    )
    if isinstance(key_map, dd.DataFrame):
        key_map = key_map.compute()
    column_order = list(key_map.columns)
    filtered = _make_surrogate_ids(key_map)
    filtered = _subcomponent_ids_from_prepped_crosswalk(filtered)
    column_order = ["component_id"] + column_order
    return filtered[column_order]


def aggregate_subcomponents(
    xwalk: pd.DataFrame,
    camd_fuel_map: Optional[Dict[str, str]] = None,
    eia_fuel_map: Optional[Dict[str, str]] = None,
    tech_type_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    if camd_fuel_map is None:
        camd_fuel_map = CAMD_FUEL_MAP
    if eia_fuel_map is None:
        eia_fuel_map = EIA_FUEL_MAP
    if tech_type_map is None:
        tech_type_map = TECH_TYPE_MAP

    aggs = (
        xwalk.groupby("component_id")["EIA_UNIT_TYPE"]
        .agg(lambda x: frozenset(x.values.reshape(-1)))
        .to_frame()
        .astype("category")
    )

    for unit in ["CAMD_UNIT_ID", "EIA_GENERATOR_ID"]:
        agency = unit.split("_", maxsplit=1)[0]
        capacity = f"{agency}_NAMEPLATE_CAPACITY"
        aggs[f"capacity_{agency}"] = (
            xwalk.groupby(by=["component_id", unit], as_index=False)
            .first()  # avoid double counting
            .groupby("component_id")[capacity]
            .sum()
            .replace(0, np.nan)
        )
    for col, mapping in {
        "CAMD_FUEL_TYPE": camd_fuel_map,
        "EIA_FUEL_TYPE": eia_fuel_map,
    }.items():
        nan_count = xwalk[col].isna().sum()
        simple = f"simple_{col}"
        xwalk[simple] = xwalk[col].map(mapping)
        if xwalk[simple].isna().sum() > nan_count:
            raise ValueError(f"there is a category in {col} not present in mapping {mapping}")
        aggs[simple + "_via_capacity"] = _assign_by_capacity(xwalk, simple)

    aggs["simple_EIA_UNIT_TYPE"] = aggs["EIA_UNIT_TYPE"].map(tech_type_map)
    aggs = aggs.astype(
        {
            "simple_EIA_UNIT_TYPE": "category",
            "simple_EIA_FUEL_TYPE_via_capacity": "category",
            "simple_CAMD_FUEL_TYPE_via_capacity": "category",
        }
    )
    return aggs


def _assign_by_capacity(xwalk: pd.DataFrame, col: str) -> pd.Series:
    if "CAMD" in col:
        unit = "CAMD_UNIT_ID"
        capacity = "CAMD_NAMEPLATE_CAPACITY"
    elif "EIA" in col:
        unit = "EIA_GENERATOR_ID"
        capacity = "EIA_NAMEPLATE_CAPACITY"
    else:
        raise ValueError(f"{col} must contain 'CAMD' or 'EIA'")

    # assign by category with highest capacity
    grouped = (
        xwalk.groupby(by=["component_id", unit], as_index=False)
        .first()  # avoid double counting units
        .groupby(by=["component_id", col], as_index=False)[capacity]
        .sum()
        .replace({capacity: 0}, np.nan)
    )
    grouped["max"] = grouped.groupby("component_id")[capacity].transform(np.max)
    out = grouped.loc[grouped["max"] == grouped[capacity], ["component_id", col]].set_index(
        "component_id"
    )
    # break ties by taking first category (alphabetical due to groupby)
    # this is not very principled but it is rare enough to probably not matter
    out = out[~out.index.duplicated(keep="first")]
    return out


def _create_exclusion_flags(df: pd.DataFrame) -> None:
    grp = df.groupby(level="unit_id_epa")
    df["zero_load"] = df["gross_load_mw"].eq(0)
    df["exclude_ramp"] = False
    for tech_type, radius in EXCLUSION_SIZE_HOURS.items():
        if radius < 0:
            continue
        exclude = (
            grp.rolling(f"{radius * 2 + 1}h", centered=True, on="operating_datetime_utc")[
                "zero_load"
            ]
            .sum()
            .gt(0)
        )
        # set 'exclude_ramp' based on tech_type-specific radius
        df.loc[:, "exclude_ramp"].where(
            df["simple_EIA_UNIT_TYPE"].ne(tech_type), other=exclude, inplace=True
        )

    df.drop(columns=["zero_load"], inplace=True)
    return


def _add_subcomponent_ids_and_meta(cems: dd.DataFrame, crosswalk: pd.DataFrame) -> Dict[str, Any]:
    key_map = make_subcomponent_ids(crosswalk, cems).astype({"unit_id_epa": np.int32})
    meta = aggregate_subcomponents(key_map)
    # join in component_id
    # NOTE: how='inner' drops unmatched units
    cems = cems.join(
        key_map.groupby("unit_id_epa")[["component_id"]].first(),
        how="inner",
    )

    cems = cems.merge(meta[["simple_EIA_UNIT_TYPE"]], left_on="component_id", right_index=True)
    return {"key_map": key_map, "meta": meta, "cems": cems}


def process_partition(cems_partition: pd.DataFrame, return_intermediates=False):
    """Top level API to analyze a dataset for component-wise max ramp rates

    Args:
        cems ([pd.DataFrame]): EPA CEMS data from ramprate.load_dataset.load_epacems
        crosswalk ([pd.DataFrame]): EPA crosswalk from ramprate.load_dataset.load_epa_crosswalk
        component_id_offset (int, optional): used when processing data in chunks to ensure unique IDs. Defaults to 0.

    Returns:
        [Dict[str, pd.DataFrame]]: key:value pairs are as follows:
        "component_aggs": component-level aggregates like max ramp rates
        "key_map": inner join of crosswalk and CEMS id columns
        "component_timeseries": CEMS timeseries aggregated to component level
        "cems": CEMS data
    }
    """
    cems = cems_partition.copy()
    cems.set_index(
        ["unit_id_epa", "operating_datetime_utc"],
        drop=False,
        inplace=True,
    )
    cems.sort_index(inplace=True)

    _create_exclusion_flags(cems)

    # combine units' timeseries into a single timeseries per component
    component_timeseries = (
        cems.drop(columns=["operating_datetime_utc"])  # resolve name collision with index
        .groupby(["component_id", "operating_datetime_utc"])[["gross_load_mw", "exclude_ramp"]]
        .sum()
    )
    component_timeseries["exclude_ramp"] = (
        component_timeseries["exclude_ramp"] > 0
    )  # sum() > 0 is like logical 'or'
    component_aggs = (
        cems.drop(columns=["unit_id_epa"])  # resolve name collision with index
        .groupby(["component_id", "unit_id_epa"])[["gross_load_mw"]]
        .max()
        .groupby("component_id")
        .sum()
        .add_prefix("sum_of_max_")
    )
    component_aggs = component_aggs.join(
        component_timeseries[["gross_load_mw"]]
        .groupby("component_id")
        .max()
        .add_prefix("max_of_sum_"),
        how="outer",  # shouldn't matter
    )
    # calculate ramp rates
    component_timeseries[["ramp"]] = component_timeseries.groupby("component_id")[
        ["gross_load_mw"]
    ].diff()
    ramps = (
        component_timeseries.loc[~component_timeseries["exclude_ramp"], ["ramp"]]
        .groupby("component_id")
        .agg(["max", "min", lambda x: x.idxmax()[1], lambda x: x.idxmin()[1]])
    ).rename(columns={"<lambda_0>": "idxmax", "<lambda_1>": "idxmin"}, level=1)
    for header in ramps.columns.levels[0]:
        # calculate max of absolute value of ramp rates
        ramps.loc[:, (header, "max_abs")] = (
            ramps.loc[:, idx[header, ["max", "min"]]].abs().max(axis=1)
        )
        # associate correct timestamp - note that ties go to idxmax, nans go to idxmin
        condition = ramps.loc[:, (header, "max")] >= ramps.loc[:, (header, "min")].abs()
        ramps.loc[:, (header, "idxmax_abs")] = ramps.loc[:, idx[header, "idxmax"]]
        ramps.loc[:, (header, "idxmax_abs")].where(
            condition, ramps.loc[:, (header, "idxmin")], inplace=True
        )
    # remove multiindex
    ramps.columns = ["_".join(reversed(col)) for col in ramps.columns]

    # join all the aggs
    component_aggs = component_aggs.join([ramps])  # , meta])

    # normalize ramp rates in various ways
    """ normed = component_aggs[["max_abs_ramp"] * 4].div(
        component_aggs[
            [
                "capacity_CAMD",
                "capacity_EIA",
                "sum_of_max_gross_load_mw",
                "max_of_sum_gross_load_mw",
            ]
        ].to_numpy()
    )
    normed.columns = ["ramp_factor_" + suf for suf in ["CAMD", "EIA", "sum_max", "max_sum"]]
    component_aggs = component_aggs.join(normed)
    """

    if return_intermediates:
        return {
            "component_aggs": component_aggs,
            "component_timeseries": component_timeseries,
            "cems": cems,
        }
    else:
        return {"component_aggs": component_aggs}
