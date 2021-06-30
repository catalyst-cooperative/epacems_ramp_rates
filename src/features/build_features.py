# -*- coding: utf-8 -*-
from typing import Sequence, Optional, Union

import pandas as pd
import numpy as np


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
