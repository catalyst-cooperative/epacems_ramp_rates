"""This module calculates max ramp rates for each plant component in the EPA CEMS dataset.
Outputs:
    <user_arg>.csv:
        component-level aggregates.
        See results/README.md for field descriptions.

    <user_arg>_crosswalk_with_IDs.csv:
        inner join of EPA crosswalk and CEMS id columns.
        This can be used to inspect the EPA/EIA units that make up a component.
        You can also compare to the original crosswalk or original CEMS to see which units
        failed to join and were thus excluded from this analysis."""

import argparse
from pathlib import Path
import sys
from typing import Optional, Sequence

import pandas as pd
from tqdm import tqdm

from pudl.constants import us_states
from ramprate.load_dataset import load_epacems, load_epa_crosswalk
from ramprate.build_features import process_subset, _remove_irrelevant


# territories are not in EPA CEMS. District of Columbia is.
TERRITORIES = {"MP", "PR", "AS", "GU", "NA", "VI"}


def process(
    out_path: str,
    chunk_size: int,
    start_year: int,
    end_year: int,
    state_subset: Optional[Sequence[str]] = None,
) -> None:
    """calculate max ramp rates and other metrics per connected subcomponent in EPA CEMS"""
    out_path = Path(out_path)
    if not out_path.parent.exists():
        raise ValueError(f"Parent directory does not exist: {out_path.parent.absolute()}")

    if state_subset is None:
        state_subset = us_states.keys()  # all states
    # exlude territories, which are not in EPA CEMS
    states = [state for state in state_subset if state not in TERRITORIES]

    # minimum subset of columns to load
    cems_cols = [
        "plant_id_eia",
        "unitid",
        "operating_datetime_utc",
        "gross_load_mw",
        "unit_id_epa",
    ]
    years = list(range(start_year, end_year + 1))

    crosswalk = load_epa_crosswalk()
    crosswalk = _remove_irrelevant(crosswalk)  # remove unmatched or non-exporting

    # process in chunks due to memory constraints.
    # If you use an instance with 10+ GB memory per year of data analyzed, this won't be necessary.
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out_path", type=str, help="""Output path of csv file""")
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=5,
        help="""processing is chunked by US states. This arg selects the number of states per chunk. Default is 5. If your instance has 10+ GB memory per year of data analyzed, chunking is unnecessary so set to 55""",
    )
    parser.add_argument(
        "--start_year",
        type=int,
        default=2015,
        help="""first year of CEMS data to include in analysis. Inclusive. Default is 2015.""",
    )
    parser.add_argument(
        "--end_year",
        type=int,
        default=2019,
        help="""last year of CEMS data to include in analysis. Inclusive. Default is 2019.""",
    )
    parser.add_argument(
        "--state_subset",
        type=str,
        default=None,
        nargs="*",
        help="""optional list of state abbreviations to include in the analysis. Default is all states""",
    )
    args = parser.parse_args(sys.argv[1:])
    sys.exit(
        process(
            args.out_path,
            chunk_size=args.chunk_size,
            start_year=args.start_year,
            end_year=args.end_year,
            state_subset=args.state_subset,
        )
    )
