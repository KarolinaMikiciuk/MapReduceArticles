""""
Something similar can be seen in 'Python Cookbook', 
Recipe 4.13 : Creating Data Processing Pipelines
Likewise, the Unix pipeline from chapter 10 of DDIA
illustrates the concept of generators (among other concepts)
"""
from typing import Generator, Dict, List
import os

# defining some types, merely for documentation purposes
# requires Python 3.12+ for this new typing style
type TSVFileRecord = tuple[int, int]


def split_input(data_dir_filepath : str) -> Generator[str, None, None]:
    """
    Generator function *slightly* similar to InputSplit
    in the Hadoop implementation of MapReduce. In Hadoop
    MapReduce each input file counts as one split, unless
    the file occupies multiple file blocks (64MB each) -
    in that case, the file will be split into 
    File_Size_In_MB / 64MB splits. Each split gets its own
    Mapper task to process the data in that split.

    Parameters:
        data_dir_filepath : str, required
            Name of the root directory containing the data files

    Returns:
        A generator producing filepaths of each file relative to
        the root data directory filepath provided as input
    """
    for (root_directory, subdirectories, files) in os.walk(data_dir_filepath): # generator object
        for file in files:
            yield os.path.join(root_directory, file)



def read_records(split : str) -> Generator[str, None, None]:
    """
    Generator function meant to model the RecordReader
    in the Hadoop implementation of MapReduce. The Hadoop
    implementation would take raw bytes as input and
    parse them according to the input data format, to
    interpret the bytes as a series of records. In our
    case, each newline is one record, so the RecordReader
    simply (1) converts bytes to text, (2) splits on newline,
    similar to Hadoop's LineRecordReader.

    Other data encodings would require a more complex process
    to parse (Avro, Parquet and so on) by reading the byte
    offsets of the start and end of each record in the file.
    Moreover - in Hadoop MapReduce, RecordReader produces
    K:V pairs, where key is the byte offset of the start of 
    the record, and the value is the line itself. For the
    purposes of this demonstration, we simply treat the line
    as a string for simplicity, as we haven't yet discussed
    data deserialisation.

    Parameters:
        split : str, required
            Filepath of the data file to process

    Returns:
        A generator producing all the records in
        the input file
    """
    with open(split, 'r') as data_file:
        next(data_file)              # drop the file header
        for record in data_file:     # in case of text files, a record is simply a line
            yield record



def map(record : str) -> TSVFileRecord:
    """
    Function meant to model the Mapper
    in the Hadoop implementation of MapReduce.
    In our specific case, we are reading records of a 
    tab-delimited (TSV) file so we just split them on 
    whitespace. 
    
    The main difference between this and Hadoop MapReduce
    is that the Map Task will write the K:V pairs
    to disk for intermediate materialisation of state,
    as a way to achieve fault-tolerance. This is done
    on the Map Task's local disk as a SSTable, which
    keeps the data sorted, partitioned by reducers, so
    that K:V pairs corresponding to a particular reducer
    task (assigned by the hash of the Key) get written
    to the partition for that particular reducer.
    Meanwhile, our implementation simply returns the pairs
    in-memory.

    Parameters:
        record : str, required
            Filepath of the data file to process

    Returns:
        Tuple of (ItemID, Rating)
        where ItemID is the ID of the movie in the Movie
        Lens dataset and Rating is the user's rating of the movie.
        A tuple was chosen to model the K:V pairing, since it takes
        up less space than a dictionary, making it more memory-efficient.
    """
    record = record.split()          # split tab-separated data on whitespace
    return (record[1],  record[2])   # return K:V pair, ItemID : Rating



def shuffle():
    """
    https://stackoverflow.com/questions/22141631/what-is-the-purpose-of-shuffling-and-sorting-phase-in-the-reducer-in-map-reduce
    partition i.e. SHUFFLING 
    # create partitions: based on hash put in appropriate list
    
    no. of movies / no. of reducers -> assign ranges 
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    The key (or a subset of the key) is used to derive
    * the partition, typically by a hash function. The total number of partitions
    * is the same as the number of reduce tasks for the job.

    basically let's say we have 1000 keys and 5 reducers : it will split
    200 keys per reducer 
    """
    pass



def sort():
    """
    TODO: FOR EACH MAP TASK AS YOU CONSUME ONE OUTPUT, PLACE IT
    AND KEEP IT AS SORTED AND PARTITIONED: MAYBE HAVE STATE WHERE
    DIFFERENT LISTS REPRESENT DIFFERENT PARTITIONS
    """
    pass



def combine(key_value_pairs : List[TSVFileRecord]) -> Dict[int, List[int]]:
    """
    Function meant to model the optional Combiner
    step in the Hadoop implementation of MapReduce.
    A Combiner is a partial reducer, whose purpose
    in the MapReduce framework is to decrease the
    network bandwidth required to send the data from
    a Mapper to a Reducer (they are distributed on
    separate machines). Hence, instead of sending
    {213 : 1}, {213 : 4} we can *combine* these into
    a single pair: {413 : [1,4]}, reducing the size to
    be sent over the network. The combiner will run
    in-memory before the mapper writes to disk. This
    way, we also save on some data storage space.

    Since the in-memory representation of a Map Task's
    transformed data is a self-balancing tree-structure,
    the combiner algorithm is not the same as our naive
    implementation with a dictionary, but it will suffice
    for the first article.

    Parameters:
        key_value_pairs : a list of K:V tuples, required
            The K:V pairs are integers of ItemID : Rating

    Returns:
        A dictionary, where each unique ItemID gets its own
        key, associated with a list of integer values representing
        the ratings for the movie with that ItemID. In MapReduce
        this dictionary would be written to disk.
    """
    combined_result = {}
    for kv_pair in key_value_pairs:
        if kv_pair[0] in combined_result:
            combined_result[kv_pair[0]].append(kv_pair[1])
        else:
            combined_result[kv_pair[0]] = [kv_pair[1]]
    return combined_result



def reduce():
    """
    """
    pass

# --- TRIAL AND ERROR ---------------------------------------------------------------------

data_splits = split_input('data_dir') # byte-oriented view of input data

x = read_records('data_dir/data1.txt')

intermediate_state_materialised = [map(record) for record in x]

print(combine(intermediate_state_materialised))