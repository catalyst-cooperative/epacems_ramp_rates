# epacems_ramp_rates

Characterization of power plant ramp rates using hourly EPA CEMS data

Outline of work and current progress:

- [X] Plan analysis path and do initial exploration of ramp rates
- [X] Identify and select 'normal' plant operations
  - [X] Identify startup and shutdown events
  - [X] Analyze ramp rates vs proximity to startups/shutdowns
- [ ] Check if hourly resolution is sufficient to calculate ramp rates
  - [X] For peakers, check for single-point spikes (unresolveable peaks).
  - [ ] For 'baseload', check that ramp rates are smaller than operating band
- [X] Idenfify maximum per-unit hourly load change
- [X] Join EPA emissions units with EIA generation units
- [ ] Anomaly detection and imputation
- [ ] Ramp rate sensitivity analysis
- [X] Draft module or script to calculate ramp rates

## Installation
In the coming weeks there will be a simple requirements.txt with a line `pudl==0.4`, but it is not released quite yet. So for now, [install the dev branch of catalyst-cooperative/pudl](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/dev_setup.html).  This repo has no additional dependencies.

Then `pip install .` to install the local package.

## Usage
You must have EPA CEMS data as defined by the [catalyst-cooperative/pudl](https://github.com/catalyst-cooperative/pudl) repo. See that repo for how to download/generate the appropriate data.

To create .csv files with the results of this analysis, use the CLI:

`$ python ramp_rate_script.py OUTPUT_PATH`

See `--help` for details.

For interactive use in a jupyter notebook, see the example in notebooks/8.0-tb-example_of_interactive_use.ipynb

## Output Data Dictionary

|               field                | Description                                                                                                                                                                                                                          |
| :--------------------------------: | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|            component_id            | The ID corresponding to the connected subcomponents (EPA units and EIA generators) that have been aggregated together. Note that these IDs are regenerated each time, so are subject to change between runs.                         |
|      sum_of_max_gross_load_mw      | Empirical estimate of capacity by summing the max load of each individual EPA timeseries. This method may overestimate capacity.                                                                                                     |
|      max_of_sum_gross_load_mw      | Empirical estimate of capacity by taking the max after summing the individual EPA timeseries into a component-level timeseries. This is the preferred capacity estimate.                                                             |
|              max_ramp              | The highest positive ramp rate of a component-level timeseries after filtering out cold starts                                                                                                                                       |
|              min_ramp              | The lowest negative ramp rate of a component-level timeseries after filtering out cold starts                                                                                                                                        |
|            idxmax_ramp             | The timestamp of max_ramp                                                                                                                                                                                                            |
|            idxmin_ramp             | The timestamp of min_ramp                                                                                                                                                                                                            |
|            max_abs_ramp            | The largest magnitude between max_ramp and min_ramp                                                                                                                                                                                  |
|          idxmax_abs_ramp           | The timestamp of max_abs_ramp                                                                                                                                                                                                        |
|           EIA_UNIT_TYPE            | The set of EIA technology types found within this component                                                                                                                                                                          |
|           capacity_CAMD            | Nameplate capacity as reported by CAMD, aggregated to the component level                                                                                                                                                            |
|            capacity_EIA            | Nameplate capacity as reported by EIA, aggregated to the component level                                                                                                                                                             |
| simple_CAMD_FUEL_TYPE_via_capacity | Simplified fuel type of component according the CAMD category. See src.build_features.CAMD_FUEL_MAP for details. When there are multiple (rare, <1%), the category with the highest corresponding CAMD nameplate capacity is chosen. |
| simple_EIA_FUEL_TYPE_via_capacity  | Fuel type of component according the EIA category. See src.build_features.EIA_FUEL_MAP for details. When there are multiple (rare, <1%), the category with the highest corresponding EIA nameplate capacity is chosen.               |
|        simple_EIA_UNIT_TYPE        | EIA_UNIT_TYPE mapped to simple categories. See src.build_features.TECH_TYPE_MAP for details                                                                                                                                          |
|          ramp_factor_CAMD          | max_abs_ramp divided by capacity_CAMD                                                                                                                                                                                                |
|          ramp_factor_EIA           | max_abs_ramp divided by capacity_EIA                                                                                                                                                                                                 |
|        ramp_factor_sum_max         | max_abs_ramp divided by sum_of_max_gross_load_mw                                                                                                                                                                                     |
|        ramp_factor_max_sum         | max_abs_ramp divided by max_of_sum_gross_load_mw. This is the preferred estimate.                                                                                                                                                    |

Notes:
* Ramps are defined between timestamps; the idxmax/idxmin timestamps mark the *end* of the ramp event.
* Nameplate capacity can vary substantially between EIA and EPA. The preferred capacity is the empirical estimate max_of_sum_gross_load_mw
* Some CEMS units report steam production instead of electical generation. This is not yet accounted for in the analysis - their ramp rates and empirical capacity estimates are 0.0 in the output table.

## Project Organization


    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
