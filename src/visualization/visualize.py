from typing import Any, Optional, Dict
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.core import series


idx = pd.IndexSlice


def plot_component_max_ramp(
    component_id: int,
    component_timeseries: pd.DataFrame,
    cems: pd.DataFrame,
    component_aggs: pd.DataFrame,
    key_map: pd.DataFrame,
    window_hours: int = 30 * 24,
    cdf=False,
    subplot_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    has_steam = component_aggs.at[component_id, "max_of_sum_steam_load_1000_lbs"] > 0
    if has_steam:
        raise NotImplementedError("component has steam unit")
    window_offset = pd.Timedelta(window_hours / 2, "h")
    unit_ids = key_map.query(f"component_id == {component_id}")["unit_id_epa"].unique()
    n_units = len(unit_ids)
    n_rows = n_units if n_units == 1 else n_units + 1

    subplot_defaults = dict(
        gridspec_kw={"width_ratios": [3, 1], "height_ratios": [1] + [0.6] * (n_rows - 1)},
        figsize=(15, 3 * n_rows + 1),
    )
    if subplot_kwargs is not None:
        subplot_defaults.update(subplot_kwargs)
    fig, axes = plt.subplots(ncols=2, nrows=n_rows, **subplot_defaults)

    # plot main component
    if n_units == 1:
        series_ax = axes[0]
        dist_ax = axes[1]
    else:
        series_ax = axes[0, 0]
        dist_ax = axes[0, 1]
    col = "gross_load_mw"
    whole_series = component_timeseries.loc[idx[component_id, :], col]
    ramp_ts = component_aggs.at[component_id, "idxmax_abs_ramp"]
    slice_ = idx[component_id, ramp_ts - window_offset : ramp_ts + window_offset]
    subset = whole_series.loc[slice_]
    _plot_unit_series_and_ramp_dist(
        subset,
        whole_series,
        series_ax,
        dist_ax,
        cdf=cdf,
        line_kwargs=dict(c="k", yaxis_label="Generation [MW]"),
        dist_kwargs=dict(color="k"),
    )
    stats = {
        "component": component_id,
        "capacity": component_aggs.at[component_id, "sum_of_max_gross_load_mw"],
        "ramp": component_aggs.at[component_id, "max_abs_ramp"],
        "ramp_factor": component_aggs.at[component_id, "ramp_factor_sum_max"],
        "tech_type": component_aggs.at[component_id, "simple_EIA_UNIT_TYPE"],
        "fuel": component_aggs.at[component_id, "simple_EIA_FUEL_TYPE_via_capacity"],
    }
    start_gen = component_timeseries.at[idx[component_id, ramp_ts - pd.Timedelta(1, "h")], col]
    end_gen = component_timeseries.at[idx[component_id, ramp_ts], col]
    series_ax.vlines(
        ramp_ts,
        ymin=start_gen,
        ymax=end_gen,
        colors="r",
        lw=5,
    )
    """ series_ax.annotate(
        f"{int(stats['ramp'])} MW/hr\n{stats['ramp_factor']:.2f} %max/hr",
        (ramp_ts, min(start_gen, end_gen) * 0.7),
        ha="center",
        bbox=dict(boxstyle="round", alpha=0.7, fc="lightgray"),
        size=12,
    ) """
    title = f"Component {stats['component']}: {round(stats['capacity'])} MW {stats['fuel']}-fueled {stats['tech_type']}      Max Ramp: {int(stats['ramp'])} MW / hr,   {round(stats['ramp_factor']*100)}% max / hr"
    series_ax.set_title(title)
    dist_ax.axvline(stats["ramp"], ls="--", lw=2, c="r")

    # plot the units
    if n_units > 1:
        for i, unit in enumerate(unit_ids, start=1):
            whole_series = cems.loc[idx[unit, :], col]
            slice_ = idx[unit, ramp_ts - window_offset : ramp_ts + window_offset]
            subset = whole_series.loc[slice_]
            _plot_unit_series_and_ramp_dist(
                subset,
                whole_series,
                axes[i, 0],
                axes[i, 1],
                cdf=cdf,
                line_kwargs=dict(yaxis_label="Generation [MW]"),
            )
            axes[i, 0].set_title(f"EPA Unit {unit}")

    plt.tight_layout()
    plt.show()


def _plot_unit_series_and_ramp_dist(
    series: pd.Series,
    whole_series: pd.Series,
    series_ax: plt.Axes,
    dist_ax: plt.Axes,
    cdf=False,
    line_kwargs=None,
    dist_kwargs=None,
    **kwargs,
):
    if line_kwargs is None:
        line_kwargs = dict()
    if dist_kwargs is None:
        dist_kwargs = dict()
    x = series.index.get_level_values(1)
    _plot_base_series(x, series, series_ax, **line_kwargs, **kwargs)
    window_pdf_defaults = dict(
        histtype="step",
        label="window",
        color=None,
        alpha=1,
        cdf=cdf,
    )
    window_pdf_defaults.update(dist_kwargs)
    _plot_base_pdf(
        series.diff().replace(0, np.nan),
        dist_ax,
        **window_pdf_defaults,
        **kwargs,
    )
    _plot_base_pdf(
        whole_series.diff().replace(0, np.nan),
        dist_ax,
        label="whole unit",
        color="gray",
        cdf=cdf,
        **kwargs,
    )
    title = f"Ramp Rate {'C' if cdf else 'P'}DF (zeros excluded)"
    dist_ax.set_title(title)
    dist_ax.legend(loc="upper left")


def _plot_base_series(x, y, ax: plt.Axes, yaxis_label="Generation [MW]", **kwargs) -> None:
    if len(x) <= 60 * 24:
        line_defaults = dict(c=None, marker=".", markersize=4, lw=1)
        line_defaults.update(kwargs)
        ax.plot(x, y, **line_defaults)
    else:
        scatter_defaults = dict(c=None, marker=".", s=4)
        scatter_defaults.update(kwargs)
        ax.scatter(x, y, **scatter_defaults)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b-%d"))
    ax.set_ylim(ymin=0)
    ax.set_ylabel(yaxis_label)


def _plot_base_pdf(series, ax: plt.Axes, cdf=False, **kwargs) -> None:
    defaults = dict(
        bins=int(np.sqrt(len(series))),
        cumulative=cdf,
        histtype="stepfilled",
        alpha=0.5,
        density=True,
    )
    defaults.update(kwargs)
    try:
        ax.hist(series, **defaults)
    except ValueError as e:
        if "[nan, nan]" in e.args[0]:  # data is all zeros
            pass
        else:
            raise e
