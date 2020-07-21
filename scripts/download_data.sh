#!/usr/bin/env bash

# Scripts to unpack UKB data
# The encrypted data was downloaded separately directly from the AMS portal
# For instructions, see https://biobank.ndph.ox.ac.uk/~bbdatan/Accessing_UKB_data_v2.1.pdf

# The key and MD5sum was emailed directly by the UKB AMS k15422r42306.key, the following 2 lines were the content
# 15422
# a490a9bff5744c45b263f703e5dc0b550281f9a7a61c6720c4ebbaa0bc133bbd

# Make sure that ukb utility tools have been downloaded and added to the $PATH
# https://biobank.ndph.ox.ac.uk/showcase/download.cgi?ams=6484c40676f07664e51b0298b54c7ad4&tn=1591631913&ui=49854&ai=15422&bi=0&ed=1

DATA_DIR="/lustre/scratch/scratch/rmhiaah/Projects/ukb-phenotype/data"
cd $DATA_DIR

FILE_ID="ukb42306"

# STEP 1: check MD5sum ---------
MD5_from_email="4cec31fdc88c218b54029ee4b623075e"
MD5_downloaded=$(ukbmd5 ${FILE_ID}.enc | tail -n 1 MD5_downloaded.txt | sed 's/.*MD5=//g')

if [[ $MD5_from_email == $MD5_downloaded ]]; then
  echo "MD5 hash for the downloaded file is correct"
else
  echo "incorrect MD5 hash. Please check the downloaded file."
fi

# STEP 2: unpack encrypted file -----------
ukbunpack ${FILE_ID}.enc k15422r42306.key

# STEP 3: Conversion ---------
# downloads encoding data (required by ukbconv)
wget  -nd  biobank.ctsu.ox.ac.uk/crystal/util/encoding.ukb

# create data dictionary
ukbconv ${FILE_ID}.enc_ukb docs

# convert to tsv
ukbconv ${FILE_ID}.enc_ukb txt

# convert to csv
ukbconv ${FILE_ID}.enc_ukb csv

# rename and create a timestamp
DATE=$(grep 'REX: ' ${FILE_ID}.log | awk -v FS='REX: |T' '{print $2}')
APP_ID=$(grep 'Application ' ${FILE_ID}.log | awk -v FS='[ ,]' '{print $2}')
mv ${FILE_ID}.txt "${DATE}_${FILE_ID}_app${APP_ID}.tsv"
