
# amyloidosis_prediction
Repository corresponding to the paper on prediction of amyloidosis using NLP from clinical data sources

## Generating results and Disclaimer

The code presented here was developed over time to generate the results in our paper.   
An attempt was made to clean up the code and remove excessive portions of experimental code 
that was written and run in an iterative manner to check/test/refine/repeat various parameters and results.   The code presented here does not represent the full breadth of the parameters, models and experiments run, but
rather represents an attempt to deliver a clean pipeline of the generation of main results in the paper.   Our goal is allow a reader to hopefully track the methods and results in the paper. 

Due to data restrictions we can not provide the raw data.   Therefore, in order to regenerate results, several modifications would need to be made to our code to regenerate local results. 
All the primary modifications in order to run would be in the data_objecgts/file_config.py file.  
These specify the data columns as well as output paths.   Given these are specified the model_pipeline.py file should simply run to regenerate results.

## Installation

Clone and enter the repository
```git
git clone https://github.com/ssolari/amyloidosis_prediction.git
```

```
cd amyloidosis_prediction
```


The simplest way to then install and run is to use a [conda envrionment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands).
First install conda from directions in the above link.

```python
conda env create -n amyloidosis_prediction_env -f environment.yml
```

To activate the envrionment
```python
source activate amyloidosis_prediction_env
```

Once the environment is created AND activated then the amyloidosis_prediction package can be installed into that envrionment.
Modification of the data_objects/file_config.py should be performed before the following command.

```python
python setup.py install
```

If modifications need to be made to the file_config.py to alter data formats and re-run then the above setup.py install command should be run again. 

## Running 

### Enviornmental variables

Two envrionmental variables must be set

```python
AMYLOID_RAW_DIR
```
This path should point to all csv files.    Importantly the CSV file names and columns must be set in the 
/data_objects/file_config.py for the code to run and work.   For new users the definitions in file_config.py should be overwritten. 

and

```python
AMYLOID_SPLIT_DIR
```
This path is actually the output path for all generated files and results.   Data will be fully copied one time (sorted by patient id),
then will be written again in compressed hdf5 files.   This directory should have enough space to handle these extra copies.

### Running code

To run models on the data the ```model_pipeline.py``` file should be used.    The default in that file is 
limit_rows=400000 in order to test.  In order to run on all data the user should set limit_rows=0.