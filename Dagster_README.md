## For who execute Dagster for the firs time


## 1. go to your project root folder
cd ~/GroupProject_Brazilian-E-Commerce-Public-Dataset-by-Olist

## 2. install dagster into your existing eltn env
conda env update -f eltn_environment.yml

## 3. ensure all *.py files executable
chmod 755 Dbt_Final/*.py GX/*.py EDA_ML/*.py

## 4. launch dagster dashboard under your project root folder
dagster dev -m dagster_proj.definitions
