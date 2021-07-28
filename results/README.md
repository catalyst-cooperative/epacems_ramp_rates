## Contents:
* output_2015-2019.csv: results of analysis of CEMS data from 2015-2019, inclusive.
* output_2015-2019_crosswalk_with_IDs.csv: EPA crosswalk with `component_id` column added. Use this to inspect which combustors/generators are grouped together into the same component. Note that component_id may change with different inputs (even different ordering) so we advise keeping these output files together.

These outputs were generated by running:

`calc_ramps ./results/output_2015-2019.csv --first-year 2015 --last-year 2019 --states 'all'`

## Notes
* Ramps are defined between timestamps; the idxmax/idxmin timestamps mark the *end* of the ramp event.
* Nameplate capacity can vary substantially between EIA and EPA. The preferred capacity is the empirical estimate max_of_sum_gross_load_mw
* Some CEMS units report steam production instead of electical generation. This is not yet accounted for in the analysis - their ramp rates and empirical capacity estimates are 0.0 in the output table.


## Output Data Dictionary
For a data dictionary of the EPA crosswalk, see the field descriptions in the [EPA github repo](https://github.com/USEPA/camd-eia-crosswalk).


|               Field                | Description                                                                                                                                                                                                                               |
| :--------------------------------: | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|            component_id            | The ID corresponding to the connected subcomponents (EPA units and EIA generators) that have been aggregated together. Note that these IDs are regenerated each time, so are subject to change between runs.                              |
|      sum_of_max_gross_load_mw      | Empirical estimate of capacity by summing the max load of each individual EPA timeseries. This method may overestimate capacity.                                                                                                          |
|      max_of_sum_gross_load_mw      | Empirical estimate of capacity by taking the max after summing the individual EPA timeseries into a component-level timeseries. This is the preferred capacity estimate.                                                                  |
|              max_ramp              | The highest positive ramp rate of a component-level timeseries after filtering out cold starts                                                                                                                                            |
|              min_ramp              | The lowest negative ramp rate of a component-level timeseries after filtering out cold starts                                                                                                                                             |
|            idxmax_ramp             | The timestamp of max_ramp                                                                                                                                                                                                                 |
|            idxmin_ramp             | The timestamp of min_ramp                                                                                                                                                                                                                 |
|            max_abs_ramp            | The largest magnitude between max_ramp and min_ramp                                                                                                                                                                                       |
|          idxmax_abs_ramp           | The timestamp of max_abs_ramp                                                                                                                                                                                                             |
|           EIA_UNIT_TYPE            | The set of EIA technology types found within this component                                                                                                                                                                               |
|           capacity_CAMD            | Nameplate capacity as reported by CAMD, aggregated to the component level                                                                                                                                                                 |
|            capacity_EIA            | Nameplate capacity as reported by EIA, aggregated to the component level                                                                                                                                                                  |
| simple_CAMD_FUEL_TYPE_via_capacity | Simplified fuel type of component according the CAMD category. See ramprate.build_features.CAMD_FUEL_MAP for details. When there are multiple (rare, <1%), the category with the highest corresponding CAMD nameplate capacity is chosen. |
| simple_EIA_FUEL_TYPE_via_capacity  | Fuel type of component according the EIA category. See ramprate.build_features.EIA_FUEL_MAP for details. When there are multiple (rare, <1%), the category with the highest corresponding EIA nameplate capacity is chosen.               |
|        simple_EIA_UNIT_TYPE        | EIA_UNIT_TYPE mapped to simple categories. See ramprate.build_features.TECH_TYPE_MAP for details                                                                                                                                          |
|          ramp_factor_CAMD          | max_abs_ramp divided by capacity_CAMD                                                                                                                                                                                                     |
|          ramp_factor_EIA           | max_abs_ramp divided by capacity_EIA                                                                                                                                                                                                      |
|        ramp_factor_sum_max         | max_abs_ramp divided by sum_of_max_gross_load_mw                                                                                                                                                                                          |
|        ramp_factor_max_sum         | max_abs_ramp divided by max_of_sum_gross_load_mw. This is the preferred estimate.                                                                                                                                                         |