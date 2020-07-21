#!/usr/bin/env bash

# Run from local
# Scripts to copy file from Myriad to SDrive
# Requires S Drive to be mounted first
# https://www.ucl.ac.uk/isd/services/file-storage-sharing/groupfolders-shared-s-drive

DATA_DIR="/lustre/scratch/scratch/rmhiaah/Projects/ukb-phenotype/data"
FILES=(${DATA_DIR}/ukb42306.html ${DATA_DIR}/2020-06-08_ukb42306_app15422.tsv ${DATA_DIR}/ukb

LOCAL_DIR="/Volumes/groupfolders/FPHS_IHI_DataLab_UKBiobank/application-15422/ukb-phenotype"

scp "${FILES[@]}" "$LOCAL_DIR/data/"


# LD_LIBRARY_PATH="$HOME/lib"
# smb://ad.ucl.ac.uk/groupfolders
# $HOME/bin/curl -T fields.ukb -u 'ad\rmhiaah' smb://ad.ucl.ac.uk/groupfolders/FPHS_IHI_DataLab_UKBiobank/application-15422/
