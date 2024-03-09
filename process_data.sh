#!/bin/bash

which_python=/home/users/ncasit/miniconda3/envs/netcdf_311/bin/python
input_file=/gws/pw/j07/ncas_obs_vol1/wao/processing/uea-aws-2/uea-aws-2_growing.csv
output_loc=/gws/pw/j07/ncas_obs_vol1/wao/processing/uea-aws-2/20180501_longterm
product_version=2.0
year=2023

${which_python} process_data.py ${input_file} ${output_loc} ${product_version} ${year}
