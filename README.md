
# ALARM_Study

This repository corresponds to our paper, "Natural Language Processing Framework in Early Detection of Amyloidosis: The ALARM Study," on predicting amyloidosis using NLP from clinical narratives.


## Generating Results and Disclaimer

The code presented here was developed over time to generate the results in our paper. An attempt was made to clean up the code and remove excessive portions of experimental code that was written and run in an iterative manner to check, test, refine, and repeat various parameters and results. The code presented here does not represent the full breadth of the parameters, models, and experiments run, but rather an attempt to deliver a clean pipeline for the main results in the paper. Our goal is to allow a reader to track the methods and results presented.

Due to restrictions, we cannot provide the raw data. Therefore, several modifications would need to be made to our code to regenerate results. All primary modifications are in the data_objects/file_config.py file, which specifies the data columns and output paths. Once these are specified, model_pipeline.py should then run automatically.


## Installation

Clone and enter the repository:
```git
git clone https://github.com/MasriAhmadMD/ALARM_Study.git
```

```
cd ALARM_Study
```


The simplest way to install and run is to use a [conda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands). First, install conda using the directions in the link above.

```python
conda env create -n ALARM_Study_env -f environment.yml
```

To activate the environment:
```python
source activate ALARM_Study_env
```

Then install the ALARM_Study package. Modify data_objects/file_config.py before running:

```python
python setup.py install
```

After any changes to file_config.py, re-run the above setup.py install command. 

## Running 

### Environmental Variables

Two environmental variables must be set:

```python
AMYLOID_RAW_DIR
```
This path should point to all CSV files. Importantly, the CSV file names and columns must be set in
/data_objects/file_config.py for the code to run correctly. For new users, the definitions in file_config.py should be overwritten. 

```python
AMYLOID_SPLIT_DIR
```
This path is the output path for all generated files and results. Data will be fully copied once (sorted by patient ID), then written again in compressed HDF5 files. This directory should have enough space to handle these extra copies.

### Running code

To run models on the data, the ```model_pipeline.py``` file should be used. The default in that file is 
limit_rows=400000 for testing purposes. To run on all data, set limit_rows=0.
