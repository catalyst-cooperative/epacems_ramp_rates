download:
	wget https://sandbox.zenodo.org/record/764417/files/databeta-2021-03-30.tgz

extract: download
# the tar archive has gigabytes of docker image and EIA/FERC data not needed here
# Extract only CEMS parquet files
	tar -xvzf ./databeta-2021-03-30.tgz databeta-2021-03-30/pudl_data/parquet/epacems -C ./data_in

install: extract
# this assumes the makefile is run from its parent dir: the repo root
	export EPA_CEMS_DATA_PATH=$(shell pwd)/data_in
	pip install .
