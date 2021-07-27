# epacems_ramp_rates

Characterization of power plant ramp rates using hourly EPA CEMS data

## Installation
In the coming weeks there will be a simple requirements.txt with a line `pudl==0.4`, but it is not released quite yet. So for now, [install the dev branch of catalyst-cooperative/pudl](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/dev_setup.html).  This repo has no additional dependencies.

Then `pip install .` to install the local package.

## Usage
You must have EPA CEMS data as defined by the [catalyst-cooperative/pudl](https://github.com/catalyst-cooperative/pudl) repo. See that repo for how to download/generate the appropriate data.

To create .csv files with the results of this analysis, use the CLI:

`$ python ramp_rate_script.py OUTPUT_PATH`

See `--help` for details.

For interactive use in a jupyter notebook, see the example in notebooks/8.0-tb-example_of_interactive_use.ipynb

## Project Organization


    ├── LICENSE
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `_` delimited description, e.g.
    │                         `1.0-jqp-initial_data_exploration`.
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    |
    └── src/ramprate       <- Source code for use in this project.
        ├── __init__.py    <- Makes ramprate a Python module
        │
        ├── make_dataset.py <- module to download or read data
        |
        ├── cli.py         <- module to analyze CEMS data and write outputs to csv
        │ 
        ├── build_features.py <- module to turn raw data into features for modeling
        │
        └── visualize.py   <- module to create exploratory and results oriented visualizations

--------
