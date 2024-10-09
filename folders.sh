#!/bin/bash

# Create top-level directories
mkdir -p 01_build 02_analysis 03_document

# Create subdirectories within build and analysis
for dir in 01_build 02_analysis
do
  mkdir -p ./$dir/01_input
  mkdir -p ./$dir/02_scripts
  mkdir -p ./$dir/03_output
  mkdir -p ./$dir/04_temp
done

cd 02_analysis/03_output

mkdir -p Figures Tables

echo "Archivos creados con éxito!"
