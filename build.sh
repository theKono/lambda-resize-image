#!/bin/bash

set -e
set -o pipefail

#
# ARGV
#
if [[ $# -ne 1 ]]; then
    echo "usage: $0 <config.json>"
    exit 1
fi

output_dir="dist"
output_zip="dist.zip"
config_json=$1

#
# Create output folder
#
mkdir -p $output_dir
cp -f runtime/* $output_dir
cp main.py $output_dir
pip install -r requirements.txt -t $output_dir

#
# Generate config.py
#
py_script="import json; j = json.load(open('$config_json')); print 'class Config(object): setting = %s' % j"
python -c "$py_script" > $output_dir/config.py

#
# Zip
#
cd $output_dir
zip -r ../$output_zip *
cd ..
rm -rf $output_dir
