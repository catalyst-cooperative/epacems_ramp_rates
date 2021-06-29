# -*- coding: utf-8 -*-
from typing import Sequence, Optional

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


def _find_blocks_of_zeros(ser: pd.Series) -> pd.DataFrame:
    """summarize contiguous subsequences of zero values in a time series

    Args:
        ser (pd.Series): pandas series with datetime index

    Raises:
        NotImplementedError: whens ser has multiindex
        ValueError: when first value of ser is np.NaN
        ValueError: when last value of ser is np.NaN

    Returns:
        pd.DataFrame: table of events, with shutdown and startup timestamps
    """
    if isinstance(ser.index, pd.MultiIndex):
        # needs groupby
        raise NotImplementedError
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
    if generator_starts_with_zero is np.nan:
        raise ValueError(f"input series starts with NaN. Please impute before using this method.")
    if generator_ends_with_zero is np.nan:
        raise ValueError(f"input series ends with NaN. Please impute before using this method.")

    events = {}
    nan = pd.Series([pd.NaT]).dt.tz_localize("UTC")

    # Zero blocks are defined as having a start and end.
    # If the start (or end) of a block occurs outside the data
    # period, it is marked with nan.

    # NOTE: 'startup' refers to generators transitioning from
    # zero power to positive power, but confusingly indicates
    # the END of a block of zero power values. Vice versa for
    # 'shutdown', which indicates the START of a zero block.

    if generator_starts_with_zero:  # first zero block has unknown shutdown time, known startup
        events["shutdown"] = nan.append(pd.Series(shutdowns), ignore_index=True)
    else:  # first zero block is fully defined
        events["shutdown"] = shutdowns

    if generator_ends_with_zero:  # last zero block has known shutdown but unknown startup
        events["startup"] = pd.Series(startups).append(nan, ignore_index=True)
    else:  # last zero block is fully defined
        events["startup"] = startups

    return pd.DataFrame(events)
