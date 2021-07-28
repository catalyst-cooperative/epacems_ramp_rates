#!/bin/bash
# this script assumes it is run from the repo root (its parent dir)
FILE=./databeta-2021-03-30.tgz
if [ -f $FILE ]
then
	echo "$FILE already downloaded."
else
	wget https://sandbox.zenodo.org/record/764417/files/databeta-2021-03-30.tgz
fi

# the tar archive has gigabytes of docker image and EIA/FERC data not needed here
# Extract only CEMS parquet files
tar -xvzf ./databeta-2021-03-30.tgz databeta-2021-03-30/pudl_data/parquet/epacems
mv ./databeta-2021-03-30/pudl_data/parquet/epacems/ ./data_in
rm -rf ./databeta-2021-03-30

echo "EPA_CEMS_DATA_PATH=$(pwd)/data_in/epacems" > .env
pip install -r requirements.txt
