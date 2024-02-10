# Importing Project Dependencies

import requests
import pandas as pd
import json
import string
import re
import numpy as np
import argparse

# Helper Functions

def clean(series: pd.Series): 

    """
    :param series: pandas.Series
        A pandas Series containing text data to be cleaned.

    :return: list
        Returns a list of cleaned text data.

    Description:
        Cleans a series of text data by removing punctuation, non-ASCII characters, and extra whitespace.
    """

    punct = set(string.punctuation)

    puncts_needed = {'.', ',', '@', '!', '?', '#', "'", '"'}

    for pn in puncts_needed:
        punct.discard(pn)
    
    clean_caption = []

    for text in series:    
        
        # Escape special characters
        pattern = re.compile('[{}]'.format(re.escape(''.join(punct))))  
        
        # Use compiled pattern for substitution
        clean_text = pattern.sub(" ", text).strip()  
        
        #Use encode/decode instead of regex for ASCII conversion
        clean_text = clean_text.encode("ascii", errors="ignore").decode()
        
        # Use list comprehension instead of split/join for tokenization
        textTokens = [tkns for tkns in clean_text.split() if tkns != ' ']
        
        #Join list directly instead of using ' '.join to avoid additional whitespace
        cleanText = ' '.join(textTokens)
                
        clean_caption.append(cleanText)

    return clean_caption

def get_entities(entities: dict, entity_keys: list):
    
    """
    :param entities: dict
        A dictionary containing entity information.

    :param entity_keys: list
        A list of keys specifying the types of entities to extract.

    :return: tuple
        Returns four lists: URL entities, UserID entities, Person entities, and Location entities.

    Description:
        Extracts entities from a dictionary based on specified entity keys.
    """

    # Initialize lists to store different types of entities
    url_list = []
    usrid_list = []
    person_list = []
    location_list = []

    for ent_kys in entity_keys:
        if 'Location' in ent_kys:
            location_entities = entities[ent_kys]
            
            for i in range(len(location_entities)):
                
                start = location_entities[i]['indices'][0]
                end = location_entities[i]['indices'][-1]
                
                loc_ent_keys = list(location_entities[i].keys())
                
                if 'locType' in loc_ent_keys:
                    
                    locType = location_entities[i]['locType']
                
                else:
                    
                    locType = 'na'
                
                location_tup = (start, end, locType)
                
                location_list.append(location_tup)

        elif 'Person' in ent_kys:
            
            person_entities = entities[ent_kys]
            
            for i in range(len(person_entities)):
                
                start = person_entities[i]['indices'][0]
                end = person_entities[i]['indices'][-1]
                
                person_ent_keys = list(person_entities[i].keys())
                
                if 'gender' in person_ent_keys:
                    
                    gender = person_entities[i]['gender']
                
                else:
                    
                    gender = 'na'
                
                person_tup = (start, end, gender)
                
                person_list.append(person_tup)
                    
        elif 'UserID' in ent_kys:
            
            usrid_entities = entities[ent_kys]
            
            for i in range(len(usrid_entities)):
                
                start = usrid_entities[i]['indices'][0]
                end = usrid_entities[i]['indices'][-1]
                
                usrid_tup = (start, end)
                usrid_list.append(usrid_tup)
            
        elif 'URL' in ent_kys:
            
            url_entities = entities[ent_kys]
            
            for i in range(len(url_entities)):
                
                start = url_entities[i]['indices'][0]
                end = url_entities[i]['indices'][-1]
                
                url_tup = (start, end)
                url_list.append(url_tup)
                
        elif 'Organization' in ent_kys:
            
            url_entities = entities[ent_kys]
            
            for i in range(len(url_entities)):
                
                start = url_entities[i]['indices'][0]
                end = url_entities[i]['indices'][-1]
                
                url_tup = (start, end)
                url_list.append(url_tup)
                
                
    return url_list, usrid_list, person_list, location_list

def process(chunks: list, url: str, username: str, password:str, headers: dict):

    """
    :param chunks: list
        A list of dictionaries containing text data chunks.

    :param url: str
        The URL of the entity extraction service.

    :param username: str
        The username for authentication.

    :param password: str
        The password for authentication.

    :param headers: dict
        A dictionary containing HTTP headers.

    :return: dict
        Returns a dictionary containing processed text data and extracted entities.
    
    Description:
        Processes chunks of text data by sending them to a specified URL for entity extraction.
    """

    # Initialize dictionary to store processed data
    dic = dict()

    # Iterate through chunks
    for i in range(len(chunks)):

        # Combine sentences in chunk
        sentences = " <--> ".join(chunks[i]['clean_text'].values.tolist())

        try:
            print("Processing Chunk:\t{}".format(str(i)))

            # Send request to URL for entity extraction
            response = requests.post(url, auth=(username, password),  data=sentences, headers=headers)
        
            response.raise_for_status()
            
            response_json = response.json()
            
            text = response_json['text']
        
            entities = response_json['entities']
            
            entity_keys = list(entities.keys())
            
            # Extract different types of entities
            url_list, usrid_list, person_list, location_list = get_entities(entities, entity_keys)
         
            # Store processed data in dictionary
            dic[str(i)] = (text, url_list, usrid_list, person_list, location_list)

        except:

            batch_size = 25
            
            print('Chunks were large, dividing in smaller chunks..\n')
            
            # Divide chunk into smaller batches
            new_chunks = np.array_split(chunks[i], batch_size)
            
            # Iterate through smaller batches
            for j in range(len(new_chunks)):
            
                sentences = ' <--> '.join(new_chunks[j]['clean_text'])
                
                ids = str(i)+'.'+str(j)
                
                print('Processing For Chunk: ',ids)
                
                try:
                
                    response = requests.post(url, auth=(username, password),  data=sentences, headers=headers)
                
                    response.raise_for_status()
            
                    response_json = response.json()
    
                    text = response_json['text']
        
                    entities = response_json['entities']
        
                    entity_keys = list(entities.keys())
        
                    url_list, usrid_list, person_list, location_list = get_entities(entities, entity_keys)
    
                    dic[ids] = (text, url_list, usrid_list, person_list, location_list)
        
                    print('Request Processed...')
            
                except:
                    
                    return dic
                     # Break out of the loop if an exception occurs
                    break
            
    return dic 

def process_merge(dictionary1):
    """
    :param dictionary1: dict
        A dictionary containing processed text data and extracted entities.

    :return: tuple
        Returns a tuple containing a list of new sentences and a dictionary mapping original entities to placeholders.
        
    Description:
        Processes data stored in a dictionary, replacing certain entities with placeholders and splitting sentences.
    """
    
    # Initialize lists and dictionaries
    new_sent_list = []
    hash_dict = {}
    
    # Iterate through items in the input dictionary
    for k, v in dictionary1.items():
        
        total_replace = []
        
        text = v[0]
        
        # Extract entities from the input dictionary
        url_list = v[1]
        usrid_list = v[2]
        person_list = v[3]
        location_list = v[-1]
        
        # Process location entities
        for i in range(len(location_list)):
            s = location_list[i][0]
            e = location_list[i][1]
            typ = location_list[i][-1]
            
            # Determine replacement text based on entity type
            if typ in ['na', 'pre', 'post', 'unknown', None]:
                replace_with = '<LOCATION_unknown>'
            else:
                replace_with = '<LOCATION_' + typ + '>'
                
            # Store original text and replacement text in hash dictionary
            raw = text[s:e]
            hash_dict[raw] = replace_with
            tup = (raw, replace_with)
            total_replace.append(tup)
            
        # Process person entities
        for i in range(len(person_list)):
            s = person_list[i][0]
            e = person_list[i][1]
            gender = person_list[i][-1]
            
            # Determine replacement text based on gender
            if gender in ['na', 'None', None]:
                replace_with = '<PERSON_gender_unknown>'
            else:
                replace_with = '<PERSON_' + gender + '>'
                
            # Store original text and replacement text in hash dictionary
            raw = text[s:e]
            hash_dict[raw] = replace_with
            tup = (raw, replace_with)
            total_replace.append(tup)
                
        # Process URL entities
        for i in range(len(url_list)):
            s = url_list[i][0]
            e = url_list[i][-1]
            raw = text[s:e]
            replace_with = '<URL>'
            
            # Store original text and replacement text in hash dictionary
            hash_dict[raw] = replace_with
            tup = (raw, replace_with)
            total_replace.append(tup)
            
        # Process UserID entities
        for j in range(len(usrid_list)):
            s = usrid_list[j][0]
            e = usrid_list[j][-1]
            raw = text[s:e]
            
            # Exclude specific cases for replacement
            if raw != 'i' and raw != 't':
                replace_with = '<USER_ID>'
                
                # Store original text and replacement text in hash dictionary
                hash_dict[raw] = replace_with
                tup = (raw, replace_with)
                total_replace.append(tup)
            
        # Replace entities with placeholders in the text
        new_sentences = text
        for tups in total_replace:
            new_sentences = new_sentences.replace(tups[0], tups[-1])
        
        # Split new sentences based on delimiter
        new_sent_list = new_sent_list + new_sentences.split(' &lt;--&gt; ')
        
    return new_sent_list, hash_dict


def parse_arguments():
    """
    :return: argparse.Namespace
        Returns the parsed arguments.

    Description:
        Parse command-line arguments.
    """
    
    parser = argparse.ArgumentParser(description="Anonymise Amica dataset with TwitIE")
    
    parser.add_argument('--csv_path', 
                        type=str, 
                        help='Path of the csv'
    )

    parser.add_argument('--username',
                        type=str,
                        help="Enter GATE API username"
    )

    parser.add_argument('--password',
                        type=str,
                        help="Enter GATE API password"
    )

    parser.add_argument('--storage_path',
                        type=str, 
                        help='Path to store csv and json'
    )

    return parser.parse_args()



if __name__ == "__main__":

    url = 'https://cloud-api.gate.ac.uk/process-document/twitie-named-entity-recognizer-for-tweets'
    headers = {'Content-Type': 'text/plain'}

    args = parse_arguments()
    csv_path = args.csv_path
    username = args.username
    password = args.password
    storage_path = args.storage_path

    df = pd.read_csv(csv_path)

    # Text has na fields,  need to fill them with " " for processing
    df = df.fillna(' ')

    # Clean text
    df['clean_text'] = clean(df.text)

    # TwitIE API has limit of 1200 requests per day.
    # To overcome limit, we performing chunking, which is as follows
    # At once API request will process 150 sentences, so merge 150 sentences by " <---> " and chunk depending of length of data-frame
    number_of_chunks = len(df)/150

    chunks = np.array_split(df, number_of_chunks)

    # Processing by chunks
    temp_dict = process(chunks, url, username, password, headers)

    # Merge into list of sentences and create a list of processed sentences
    batch_list, batch_hash = process_merge(temp_dict)

    batch_df = pd.DataFrame(batch_list, columns=['sentences'])

    # Append sentences to new column
    df['gate_text'] = batch_df.sentences

    df.to_csv(storage_path+'gate_processed.csv', index=False)

    with open(storage_path+'hashed.json', 'w') as hashFile:
        json.dump(batch_hash, hashFile)

    print("Fin !!!!")