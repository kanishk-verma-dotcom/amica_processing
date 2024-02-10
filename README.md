# Processing 
This is a codebase repo for processing and anonymising Amica (ASK.fm (Van Hee et al 2018)) dataset. 

# Folder & file structure

- amica_processing
  - src
    - amica_processing.py
    - twitIE_amica.py
  - README.md

# Steps
1. Get access to ASK.fm Amica dataset by [Van Hee et al 2018](https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0203794)
2. Run amica_processing.py to get a .json and a .csv file
   
```
python src/amica_processing.py --dataset_path <PATH to .txt and .ann files> \
                              --storage_path <PATH to store .csv and .json with filename for e.g, path/to/folder/amica_processed>```

3. Confirm the .csv and .json
4. Once confirmed, we will process the text for anonymisation using [TwitIE](https://gate.ac.uk/wiki/twitie.html) by GATE-NLP

```
python src/twitIE_amica.py --csv_path <Path to csv generated in step 2> \
                              --username <Gate NLP API username> \
                              --password <Gate NLP API password> \
                              --storage_path <path to store anonymised csv and json>```


