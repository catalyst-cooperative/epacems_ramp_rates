from setuptools import find_packages, setup

setup(
    name="ramprate",
    packages=find_packages("src"),
    package_dir={"": "src"},
    version="0.1.0",
    description="Characterization of power plant ramp rates using hourly EPA CEMS data",
    author="Catalyst Cooperative",
    author_email="pudl@catalyst.coop",
    license="MIT",
    entry_points={
        "console_scripts": [
            "calc_ramps = ramprate.cli:main",
        ]
    },
)
