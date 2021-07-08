# -*- coding: utf-8 -*-
from typing import Sequence, Optional, Union, Tuple

import pandas as pd
import numpy as np
from pandas.core.series import Series


class FindStartupShutdown(object):
    """find and characterize startup and shutdown events from hourly EPA CEMS data"""

    def __init__(self) -> None:
        pass

    def fit(self, cems: pd.DataFrame) -> None:
        # assign stuff
        raise NotImplementedError

    def transform(self, cems: pd.DataFrame) -> pd.DataFrame:
        # create something
        raise NotImplementedError

    def fit_transform(self, cems: pd.DataFrame) -> pd.DataFrame:
        self.fit(cems)
        return self.transform(cems)


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


def _find_edges(cems: pd.DataFrame, drop_intermediates=True) -> pd.DataFrame:
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
        cems.drop(columns=["binarized", "binary_diffs"], inplace=True)
    return cems


def _distance_from_downtime(
    cems: pd.DataFrame, drop_intermediates=True, boundary_offset_hours: int = 24
) -> pd.DataFrame:
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
        cems.drop(columns=["startups", "shutdowns"], inplace=True)
    return cems


def calc_distance_from_downtime(cems: pd.DataFrame, drop_intermediates=True) -> pd.DataFrame:
    """calculate two columns: the number of hours to the next shutdown; and from the last startup"""
    cems = _find_edges(cems, drop_intermediates)
    return _distance_from_downtime(cems, drop_intermediates)


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
