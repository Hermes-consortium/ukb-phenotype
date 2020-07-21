#!/bin/bash -l
#$ -S /bin/bash
#$ -l mem=32G
#$ -l h_rt=48:00:00
#$ -wd /lustre/scratch/scratch/rmhiaah/Projects/ukb-phenotype

cd /lustre/scratch/scratch/rmhiaah/Projects/ukb-phenotype/
Rscript --vanilla scripts/preprocess_data.R
