import pytest
import pandas as pd
import numpy as np

from src.features.build_features import _find_uptime


def test__find_uptime_start_and_end_nonzero():
    dt_idx = pd.date_range(start="2020-01-01 00:00", periods=6, freq="h", tz="UTC")
    data = [2, 2, 0, 0, 0, 2]

    # downtime=True
    # first zero after non-zero
    shutdown = pd.to_datetime(["2020-01-01 02:00"], utc=True)
    # last zero before non-zero
    startup = pd.to_datetime(["2020-01-01 04:00"], utc=True)
    expected = pd.DataFrame({"shutdown": shutdown, "startup": startup})
    actual = _find_uptime(pd.Series(data, index=dt_idx), downtime=True)
    pd.testing.assert_frame_equal(actual, expected)
    # end points ('startup') are after start points ('shutdown')
    assert actual.diff(axis=1)["startup"].dt.total_seconds().fillna(1).ge(0).all()

    # downtime=False
    # last zero before non-zero
    startup = pd.to_datetime([pd.NaT, "2020-01-01 04:00"], utc=True)
    # first zero after non-zero
    shutdown = pd.to_datetime(["2020-01-01 02:00", pd.NaT], utc=True)
    expected = pd.DataFrame({"startup": startup, "shutdown": shutdown})
    actual = _find_uptime(pd.Series(data, index=dt_idx))
    pd.testing.assert_frame_equal(actual, expected)
    # end points ('shutdown') are after start points ('startup')
    assert actual.diff(axis=1)["shutdown"].dt.total_seconds().fillna(1).ge(0).all()


def test__find_uptime_all_zeros():
    dt_idx = pd.date_range(start="2020-01-01 00:00", periods=6, freq="h", tz="UTC")
    data = [0, 0, 0, 0, 0, 0]

    # downtime=True
    # first zero after non-zero
    shutdown = pd.to_datetime([pd.NaT], utc=True)
    # last zero before non-zero
    startup = pd.to_datetime([pd.NaT], utc=True)
    expected = pd.DataFrame({"shutdown": shutdown, "startup": startup})
    actual = _find_uptime(pd.Series(data, index=dt_idx), downtime=True)
    pd.testing.assert_frame_equal(actual, expected)

    # downtime=False
    # first zero after non-zero
    shutdown = pd.to_datetime([], utc=True)
    # last zero before non-zero
    startup = pd.to_datetime([], utc=True)
    expected = pd.DataFrame({"startup": startup, "shutdown": shutdown})
    actual = _find_uptime(pd.Series(data, index=dt_idx))
    pd.testing.assert_frame_equal(actual, expected)


def test__find_uptime_no_zeros():
    dt_idx = pd.date_range(start="2020-01-01 00:00", periods=6, freq="h", tz="UTC")
    data = [5, 5, 5, 5, 5, 5]

    # downtime=True
    # first zero after non-zero
    shutdown = pd.to_datetime([], utc=True)
    # last zero before non-zero
    startup = pd.to_datetime([], utc=True)
    expected = pd.DataFrame({"shutdown": shutdown, "startup": startup})
    actual = _find_uptime(pd.Series(data, index=dt_idx), downtime=True)
    pd.testing.assert_frame_equal(actual, expected)

    # downtime=False
    # first zero after non-zero
    shutdown = pd.to_datetime([pd.NaT], utc=True)
    # last zero before non-zero
    startup = pd.to_datetime([pd.NaT], utc=True)
    expected = pd.DataFrame({"startup": startup, "shutdown": shutdown})
    actual = _find_uptime(pd.Series(data, index=dt_idx))
    pd.testing.assert_frame_equal(actual, expected)


def test__find_uptime_start_zero_end_zero():
    dt_idx = pd.date_range(start="2020-01-01 00:00", periods=6, freq="h", tz="UTC")
    data = [0, 2, 2, 0, 2, 0]

    # downtime=True
    # first zero after non-zero
    shutdown = pd.to_datetime([pd.NaT, "2020-01-01 03:00", "2020-01-01 05:00"], utc=True)
    # last zero before non-zero
    startup = pd.to_datetime(["2020-01-01 00:00", "2020-01-01 03:00", pd.NaT], utc=True)
    expected = pd.DataFrame({"shutdown": shutdown, "startup": startup})
    actual = _find_uptime(pd.Series(data, index=dt_idx), downtime=True)
    pd.testing.assert_frame_equal(actual, expected)
    # end points ('startup') are after start points ('shutdown')
    assert actual.diff(axis=1)["startup"].dt.total_seconds().fillna(1).ge(0).all()

    # downtime=False
    # last zero before non-zero
    startup = pd.to_datetime(["2020-01-01 00:00", "2020-01-01 03:00"], utc=True)
    # first zero after non-zero
    shutdown = pd.to_datetime(["2020-01-01 03:00", "2020-01-01 05:00"], utc=True)
    expected = pd.DataFrame({"startup": startup, "shutdown": shutdown})
    actual = _find_uptime(pd.Series(data, index=dt_idx))
    pd.testing.assert_frame_equal(actual, expected)
    # end points ('shutdown') are after start points ('startup')
    assert actual.diff(axis=1)["shutdown"].dt.total_seconds().fillna(1).ge(0).all()
