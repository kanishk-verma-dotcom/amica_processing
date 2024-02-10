# amica_processing
This is a codebase repo for processing Amica (ASK.fm (Van Hee et al 2018)) dataset. 

# Steps.
1. Get access to ASK.fm Amica dataset by [Van Hee et al 2018](https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0203794)
2. Run amica_processing.py to get a .json and a .csv file
   
```python amica_processing.py --dataset_path <PATH to .txt and .ann files> --storage_path <PATH to store .csv and .json with filename for e.g, path/to/folder/amica_processed>```
