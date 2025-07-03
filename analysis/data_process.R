####
## This script does the following:
# 1. Import/extract feather dataset from OpenSAFELY
# 2. ...
####


# Import libraries and user functions -------------------------------------
library('arrow')
library('tidyverse')
library('here')


# Create directories for output -------------------------------------------
fs::dir_create(here::here("output", "data"))
fs::dir_create(here::here("output", "data_description"))


# Define redaction threshold ----------------------------------------------
threshold <- 6


# Import the dataset ------------------------------------------------------
print('Import the dataset')
df <- arrow::read_feather(here::here("output", "dataset.arrow"))


