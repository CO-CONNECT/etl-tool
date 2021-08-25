import os
import glob
import json
import pandas as pd
from coconnect.tools.logger import Logger

class MissingInputFiles(Exception):
    pass
class DifferingColumns(Exception):
    pass
class DifferingRows(Exception):
    pass


class InputData:
    def __init__(self,chunksize):
        self.chunksize = chunksize
        
        self.__file_readers = {}
        self.__dataframe = {}

        self.logger = Logger(self.__class__.__name__)
        self.logger.info("InputData Object Created")
        if self.chunksize is not None:
            self.logger.info(f"Using a chunksize of '{self.chunksize}' nrows")

    def all(self):
        return {
            key:self[key]
            for key in self.keys()
        }
        
    def keys(self):
        return self.__file_readers.keys()

    def next(self):
        #loop over all loaded files
        for key in self.keys():
            #get the next dataframe chunk for this file
            self.__dataframe[key] = self.get_df_chunk(key)

        #check if all __dataframe objects are empty
        #if they are, reaise a StopIteration as processing has finished
        if all([x.empty for x in self.__dataframe.values()]):
            self.logger.debug("All input files have now been processed.")
            raise StopIteration
        
        self.logger.info(f"Moving onto the next chunk of data (of size {self.chunksize})")

        
    def get_df_chunk(self,key):
        #obtain the file by key
        obj = self.__file_readers[key]
        #if it is a TextFileReader, get a dataframe chunk
        if isinstance(obj,pd.io.parsers.TextFileReader):
            try:
                #for this file reader, get the next chunk of data and update self.__dataframe
                return obj.get_chunk(self.chunksize)
            except StopIteration:
                #otherwise, if at the end of the file reader, return an empty frame
                return pd.DataFrame(columns=self.__dataframe[key].columns)
        else:
            #if we're handling non-chunked data
            #return an empty dataframe if we've already loaded this dataframe
            if key in self.__dataframe.keys():
                return pd.DataFrame()
            #otherwise return the dataframe as it's the first time we're getting it
            return obj
            

    def __getitem__(self,key):
        if key not in self.__dataframe.keys():
            self.__dataframe[key] = self.get_df_chunk(key)
        return self.__dataframe[key]
        
    def __setitem__(self,key,obj):
        if not (isinstance(obj,pd.DataFrame) or isinstance(obj,pd.io.parsers.TextFileReader)):
            raise NotImplementedError("When using InputData, the object must be of type "
                                      f"{pd.DataFrame} or {pd.io.parsers.TextFileReader} ")
        self.logger.info(f"Registering  {key} [{type(obj)}]")
        self.__file_readers[key] = obj
        
    

def flatten_json(data):
    cdm = data['cdm']

    for destination_table,rule_set in cdm.items():
        print (destination_table)
        df = pd.DataFrame.from_records(rule_set)
        for colname in df.columns:
            #get a list of 
            column = df.loc[:,colname].values.tolist()
            column_as_str_list =  [
                json.dumps(item)
                for item in column
            ]
            unique = [
                json.loads(str_dict)
                for str_dict in list(set(column_as_str_list))
            ]
        print (unique)

        exit(0)

def load_json(f_in, flatten=True):
    try:
        data = json.load(open(f_in))
    except FileNotFoundError as err:
        try:
            data_dir = os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),'..','data')
                )
            data =  json.load(open(f'{data_dir}{os.path.sep}{f_in}'))
        except FileNotFoundError:
            raise FileNotFoundError(err)

    if flatten:
        data = flatten_json(data)
        
    return data


def load_csv(_map,chunksize=None,nrows=None,lower_col_names=False,load_path="",rules=None):

    logger = Logger("coconnect.tools.load_csv")
    
    if rules is not None:
        logger.debug("rules .json file supplied")
        rules = load_json(rules)
        source_map = get_mapped_fields_from_rules(rules)

        inputs_from_json = list(source_map.keys())
        inputs_from_cli = list(_map.keys())

        if len(inputs_from_cli) == 0:
            raise MissingInputFiles (f"You haven't loaded any input files!")
            
        logger.debug(f"{len(inputs_from_cli)} input files loaded")
        logger.debug(f"{inputs_from_cli}")
        
        missing_inputs = list(set(inputs_from_json) - set(inputs_from_cli))
        if len(missing_inputs) > 0 :
            raise MissingInputFiles (f"Found the following files {missing_inputs} in the json file, that are not in the loaded file list... {inputs_from_cli}")
        
        #reduce the mapping of inputs, if we dont need them all
        _map = {
            k: {
                'file':v,
                'fields':source_map[k]
            }
            for k,v in _map.items()
            if k in source_map
        }

    retval = InputData(chunksize)
        
    for key,obj in _map.items():
        fields = None
        if isinstance(obj,str):
            fname = obj
        else:
            fname = obj['file']
            fields = obj['fields']
            
        df = pd.read_csv(load_path+fname,chunksize=chunksize,nrows=nrows,dtype=str,usecols=fields)

        if isinstance(df,pd.DataFrame):
            #this should be removed
            if lower_col_names:
                df.columns = df.columns.str.lower()

        retval[key] = df

    return retval


def get_file_map_from_dir(_dir):
    if not os.path.isdir(_dir):
        _dir = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),'..','data',_dir)
        )

    _map = {}
    for fname in glob.glob(f"{_dir}{os.path.sep}*.csv"):
        key = os.path.basename(fname)
        _map[key] = fname
    
    return _map
 

def get_mapped_fields_from_rules(rules):
    #extract a tuple of source tables and source fields
    sources = [
        (x['source_table'],x['source_field'])
        for cdm_obj_set in rules['cdm'].values()
        for cdm_obj in cdm_obj_set
        for x in cdm_obj.values()
    ]
    
    source_map = {}
    for (table,field) in sources:
        if table not in source_map:
            source_map[table] = []
        source_map[table].append(field)
            
    source_map = {
        k:list(set(v))
        for k,v in source_map.items()
    }

    return source_map
    

def get_separator_from_filename(fname):
    _, fileExtension = os.path.splitext(fname)
    if fileExtension == '.tsv':
        return '\t'
    else:
        return ','


def diff_csv(file1,file2,separator=None,nrows=None):
    logger = Logger("CSV File Diff")
    
    if separator == None:
        sep1 = get_separator_from_filename(file1)
        sep2 = get_separator_from_filename(file2)
    else:
        sep1 = separator
        sep2 = separator

    df1 = pd.read_csv(file1,sep=sep1,nrows=nrows)
    df2 = pd.read_csv(file2,sep=sep2,nrows=nrows)
    
    exact_match = df1.equals(df2)
    if exact_match:
        return

    df = pd.concat([df1,df2]).drop_duplicates(keep=False)

    if len(df) > 0:
        logger.error(" ======== Differing Rows ========== ")
        logger.error(df)
        m = df1.merge(df2, on=df.columns[0], how='outer', suffixes=['', '_'], indicator=True)[['_merge']]
        m = m[~m['_merge'].str.contains('both')]
        file1 = file1.split('/')[-1]
        file2 = file2.split('/')[-1]
        
        m['_merge'] = m['_merge'].map({'left_only':file1,'right_only':file2})
        m = m.rename(columns={'_merge':'Only Contained Within'})
        m.index.name = 'Row Number'
        logger.error(m.reset_index().to_dict(orient='records'))
        raise DifferingRows("Something not right with the rows, changes detected.")
        
    elif len(df1.columns) != len(df2.columns):
        
        raise DifferingColumns('in df1 but not df2',list(set(df1.columns) - set(df2.columns)),'\n',
                               'in df2 but not df1',list(set(df2.columns) - set(df1.columns)))

    else:
        logger.error(" ======= Rows are likely in a different order ====== ")
        for i in range(len(df1)):
            if not (df1.iloc[i] == df2.iloc[i]).any():
                print ('Row',i,'is in a different location')
        raise Exception("differences detected")
