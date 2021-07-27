"""This script calculates max ramp rates for each plant component in the EPA CEMS dataset.
Outputs:
    <user_arg>.csv:
        component-level aggregates.
        See ./data_dictionary_for_ramp_rate_script.txt for field descriptions.

    <user_arg>_crosswalk_with_IDs.csv:
        inner join of EPA crosswalk and component_id (the index of the aggregates).
        This can be used to inspect the EPA/EIA units that make up a component.
        You can also compare to the original crosswalk or original CEMS to see which units
        failed to join and were thus excluded from this analysis."""
import argparse
from pathlib import Path
import sys

import pandas as pd
import numpy as np
from tqdm import tqdm

from ramprate.load_dataset import load_epacems, load_epa_crosswalk
import ramprate.build_features as feat
import pudl

idx = pd.IndexSlice

# size of exclusion zone around startup/shutdown
# values are based on plots in cell 78 of notebook 5.0
# https://github.com/catalyst-cooperative/epacems_ramp_rates/blob/main/notebooks/5.0-tb-one_to_one_ramp_rates_by_plant_type.ipynb
EXCLUSION_SIZE_HOURS = {
    "steam_turbine": 5,
    "combined_cycle": 7,
    "gas_turbine": -1,  # no exclusions
    "internal_combustion": -1,  # no exclusions
}
YEARS_TO_PROCESS = list(range(2015, 2020))


def process_subset(cems, crosswalk, component_id_offset=0):
    if "unit_id_epa" not in cems.index.names:
        cems = cems.set_index(
            ["unit_id_epa", "operating_datetime_utc"],
            drop=False,
        ).sort_index(inplace=True)

    feat.calc_distance_from_downtime(cems)  # in place
    key_map = cems.groupby(level="unit_id_epa")[["plant_id_eia", "unitid", "unit_id_epa"]].first()
    key_map = key_map.merge(
        crosswalk,
        left_on=["plant_id_eia", "unitid"],
        right_on=["CAMD_PLANT_ID", "CAMD_UNIT_ID"],
        how="inner",
    )
    key_map = feat.make_subcomponent_ids(key_map, cems)
    if component_id_offset:
        key_map["component_id"] = key_map["component_id"] + component_id_offset

    # NOTE: how='inner' drops unmatched units
    cems = cems.join(key_map.groupby("unit_id_epa")["component_id"].first(), how="inner")

    # Aggregate to components
    # aggregate metadata
    meta = feat.aggregate_subcomponents(key_map)
    # aggregate operational data
    cems = cems.merge(
        meta["simple_EIA_UNIT_TYPE"], left_on="component_id", right_index=True, copy=False
    )
    cems.sort_index(inplace=True)
    cems["exclude_ramp"] = cems["hours_distance"] <= cems["simple_EIA_UNIT_TYPE"].map(
        EXCLUSION_SIZE_HOURS
    ).astype(np.float32)
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
    component_aggs = component_aggs.join([ramps, meta])

    # normalize ramp rates in various ways
    normed = component_aggs[["max_abs_ramp"] * 4].div(
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
    return {
        "component_aggs": component_aggs,
        "key_map": key_map,
        "component_timeseries": component_timeseries,
        "cems": cems,
    }


def main(out_path: str, chunk_size: int):
    """calculate max ramp rates and other metrics per connected subcomponent in EPA CEMS"""
    out_path = Path(out_path)
    if not out_path.parent.exists():
        raise ValueError(f"Parent directory does not exist: {out_path.parent.absolute()}")

    territories = {"MP", "PR", "AS", "GU", "NA", "VI"}
    # territories are not in EPA CEMS. District of Columbia is.
    states = [state for state in pudl.constants.us_states.keys() if state not in territories]
    cems_cols = [
        "plant_id_eia",
        "unitid",
        "operating_datetime_utc",
        "gross_load_mw",
        "unit_id_epa",
    ]
    years = YEARS_TO_PROCESS
    crosswalk = load_epa_crosswalk()
    crosswalk = feat._remove_irrelevant(crosswalk)  # remove unmatched or non-exporting

    # process in chunks due to memory constraints. If you use an instance with 100+ GB memory this won't be necessary.
    aggregates = []
    modified_crosswalk = []
    offset = 0
    chunks = [states[i : i + chunk_size] for i in range(0, len(states), chunk_size)]
    for subset_states in tqdm(chunks):
        cems = load_epacems(states=subset_states, years=years, columns=cems_cols, engine="pandas")
        cems.set_index(
            ["unit_id_epa", "operating_datetime_utc"],
            drop=False,
            inplace=True,
        )
        cems.sort_index(inplace=True)

        outputs = process_subset(cems, crosswalk, component_id_offset=offset)
        agg = outputs["component_aggs"]

        # convert iterable types to something more amenable to csv
        agg["EIA_UNIT_TYPE"] = agg["EIA_UNIT_TYPE"].transform(lambda x: str(tuple(x)))
        aggregates.append(agg)
        modified_crosswalk.append(outputs["key_map"])
        offset += agg.index.max() + 1  # prevent ID overlap when using chunking

    aggregates = pd.concat(aggregates, axis=0)
    aggregates.to_csv(out_path)
    modified_crosswalk = pd.concat(modified_crosswalk, axis=0)
    modified_crosswalk.to_csv(out_path.parent / f"{out_path.stem}_crosswalk_with_IDs.csv")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out_path", type=str, help="""Output path of csv file""")
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=5,
        help="""processing is chunked by US states. This arg selects how many states per chunk. Default is 5. If your instance has 100+GB memory, chunking isn't necessary so set to 55""",
    )
    args = parser.parse_args(sys.argv[1:])
    sys.exit(main(args.out_path, args.chunk_size))
