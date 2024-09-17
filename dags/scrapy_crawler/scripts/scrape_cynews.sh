#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

source ~/miniconda3/bin/activate
conda activate crawler || { echo "Failed to activate Conda environment"; exit 1; }

# Change to the correct directory
cd /home/hanson/crawler/scrapy_crawler/news/news || { echo "Failed to change directory"; exit 1; }

# Run the scrapy command
scrapy crawl cynews -a start="{{ params.start_date }}" -a end="{{ params.end_date }}"

# Deactivate Conda environment
conda deactivate