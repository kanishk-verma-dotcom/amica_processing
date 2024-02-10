# Importing Project Dependencies

from glob import glob
from collections import Counter
import csv
import json
from collections import OrderedDict
import argparse


# Helper Functions
def annotations(folder: str):
    """
    :param folder: str
        The directory path where the annotation files and text files are located.

    :return: tuple
        Yields tuples containing the filename and corresponding annotation-text pairs.

    Description:
    This function iterates through the files in the specified folder, reading annotation and text files
    and yielding pairs of the filename along with their corresponding annotations and text.
    """
    # Initialize variables to track files, errors, and loaded files
    files, err, load = set(), 0, 0

    # Iterate through files in the specified folder
    for pair_part in glob(folder + '/*'):
        # Extract filename without extension and add to the set of files
        files.add('.'.join(pair_part.split('.')[:-1]))

    # Iterate through sorted filenames
    for floc in sorted(files):
        try:
            # Open annotation and text files corresponding to the filename
            ann_f, txt_f = open(floc + '.ann', 'r'), open(floc + '.txt', 'r')
            
            # Yield filename along with lists of annotation and text lines
            yield floc, [x.split('\t') for x in ann_f.readlines()
                         ], txt_f.readlines()
            
            # Close files
            ann_f.close()
            txt_f.close()

            # Increment loaded files counter
            load += 1

        except FileNotFoundError:
            # If files not found, increment error counter
            err += 1

    # Print summary of errors and loaded files
    print("Found {0} errors while reading {1} files...".format(err, load))


def convert(ann: list, txt: list):
    """
    :param ann: list
        List containing the lines from the .ann file.

    :param txt: list
        List containing the lines from the .txt file.

    :return: tuple
        Returns a tuple containing two dictionaries: one for the text and one for the annotations.

    Description:
    Converts .txt and .ann files individually to an entry dictionary.
    """
    # get a index, sentence dictionary with trailing newlines removed
    text = {i: t for i, t in enumerate(
        [te.replace('\n', '') for te in txt if te != '\n'])}
    try:
        # Process the annotation lines
        ann = {int(a[1].split()[1]): {
            "t": int(a[0][1:]),
            "index": tuple([int(x) for x in a[1].split()[1:]]),
            "text": None if '¶' in a[2] else a[2].replace('\n', ''),
            "label": a[1].split()[0]
            } for a in ann}
    except IndexError:
        # Handle malformed files
        print("Malformed file!")
        ann = {}

    return text, ann

def err_check(err: str, text: str, sentence: str):
    """
    :param err: str
        Specifies the error type.

    :param text: str
        The text to be checked.

    :param sentence: str
        The original sentence to compare against.

    :return: None

    Description:
        Throws an error if the annotation does not seem to be in the original text.
    """

    # for some reason this yields some weird stuff
    print("\n\n\n\ntext:", text, "\nsnt:", sentence)

    # make a start_index annotation dictionary
    if err == "catch":
        try:
            assert text in sentence
        except AssertionError:
            pass
    else:
        assert text in sentence

def entry_to_data(entry: dict, file_name: str, annotations: dict, text_lines: dict, error_check=False):
    """
    :param entry: dict
        The entry object to be filled with data.

    :param file_name: str
        The name of the file being processed.

    :param annotations: dict
        A dictionary containing annotations extracted from .ann files.

    :param text_lines: dict
        A dictionary containing lines of text extracted from .txt files.

    :param error_check: bool, optional (default=False)
        Flag indicating whether error checking should be performed.

    :return: dict
        Returns the entry object filled with data.

    Description"
        Fills an entry object with data extracted from .txt and .ann files.
    """

    position = 0
    for index, sentence in OrderedDict(sorted(text_lines.items())).items():
        labels, macro = {}, None
        for _ in sentence:
            if position in annotations:
                ann = annotations[position]
                if ann["index"][1] - ann["index"][0] == 1:
                    macro = ann["label"]
                else:
                    try:
                        labels[ann["label"]].append(ann["text"])
                    except KeyError:
                        labels[ann["label"]] = [ann["text"]]
                    if error_check:
                        err_check(error_check, ann["text"], sentence)
            position += 1
        position += 1
        scope = ('q' if not index % 2 else 'a') if 'ask' in file_name else '?'
        entry["data"][index] = {
            "sentence": sentence.replace('¶ ', ''),
            "labels": labels,
            "macro": macro,
            "scope": scope
        }
    return entry

def files_to_dict(dataset_path: str):

    """
    :param dataset_path: str
        The path of the dataset.

    :return: dict
        Returns a dictionary containing the processed data.

    Description:
        Stores the .txt and .ann files in a proper dictionary format.

    """
    data  = {}
    for file_name, annotation, text in annotations(dataset_path):
        text, annotation = convert(annotation, text)
        entry = {"data": {}}
        entry = entry_to_data(entry, file_name, annotation, text)
        entry["annotations"] = annotation
        entry["text"] = text
        fname = file_name.split('/')[-1]
        data[fname] = entry
    return data

def store_csv_json(data: dict, name: str, flat=True):
    """
    :param data: dict
        The dictionary containing the data to be written to .csv.

    :param name: str
        The name of the output file.

    :param flat: bool, optional (default=True)
        Flag indicating whether labels should be flattened.

    :return: None

    Description:
        Writes dictionary format to .csv and .json
    """
    print("Writing data to CSV and JSON...")

    # Writing data to JSON
    json.dump(data, open(name + '.json', 'w'), indent=4,
              separators=(',', ': '), sort_keys=True)
    print("JSON writing complete.")
    
    # Writing data to CSV
    writer = csv.writer(open(name + '.csv', 'w'), quoting=csv.QUOTE_NONNUMERIC)
    writer.writerow(["file_id", "scope", "label", "macro", "text"])
    
    for file_name, data_entry in data.items():
        for _, entry in data_entry["data"].items():
            label = ', '.join([x for x in entry["labels"].keys()] if not flat
                              else [x for x in entry["labels"].keys()][:1])
            label = 'Negative' if not label else label
            macro = 'Negative' if not entry["macro"] else entry["macro"]
            writer.writerow([file_name, entry["scope"], label, macro,
                            entry["sentence"]])
    
    print("CSV writing complete.")

def parse_arguments():
    """
    :return: argparse.Namespace
        Returns the parsed arguments.

    Description:
        Parse command-line arguments.
    """
    
    parser = argparse.ArgumentParser(description="Process Amica dataset.")
    
    parser.add_argument('--dataset_path', 
                        type=str, 
                        help='Path of the dataset'
    )

    parser.add_argument('--storage_path',
                        type=str, 
                        help='Path to store csv and json'
    )

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()

    dataset_path = args.dataset_path
    storage_path = args.storage_path

    print("Begin...")
    print("\nFirst extracting, transformaing and loading...")

    data = files_to_dict(dataset_path)

    store_csv_json(data, storage_path)

    print("\n Fin !!!")