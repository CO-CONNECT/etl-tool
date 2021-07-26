#!/usr/bin/env python3

import sys
print (sys.version)

import argparse
import json
import os
from coconnect.cdm import (
    CommonDataModel,
    get_cdm_class
)

from coconnect.cdm import (
    Person,
    ConditionOccurrence,
    Measurement,
    Observation
)

from coconnect.tools import (
    load_csv,
    load_json,
    apply_rules,
    get_file_map_from_dir_windows
)


def main():
    parser = argparse.ArgumentParser(description='ETL-CDM: transform a dataset into a CommonDataModel.')
    parser.add_argument('--rules', dest='rules',
                        required=True,
                        help='input .json file')
    parser.add_argument('--out-dir','-o', dest='out_dir',
                        required=True,
                        help='name of the output folder')
    parser.add_argument('--inputs','-i', dest='inputs',
                        required=True,
                        nargs="+",
                        help='input csv files')
    parser.add_argument("-nc","--number-of-rows-per-chunk",
                        dest='number_of_rows_per_chunk',
                        default=None,
                        type=int,
                        help="choose to chunk running the data into nrows")
    parser.add_argument("-np","--number-of-rows-to-process",
                        dest='number_of_rows_to_process',
                        default=None,
                        type=int,
                        help="the total number of rows to process")

    
    args = parser.parse_args()

    config = load_json(args.rules)

    if sys.platform =="win32":
        inputs = load_csv(
         _map=get_file_map_from_dir_windows(args.inputs),
         chunksize=args.number_of_rows_per_chunk,
         nrows=args.number_of_rows_to_process        
         )
    else:
        inputs = load_csv(
         {
             os.path.basename(x):x
             for x in args.inputs
         },
         chunksize=args.number_of_rows_per_chunk,
         nrows=args.number_of_rows_to_process
         )

    name = config['metadata']['dataset']

    #build an object to store the cdm
    cdm = CommonDataModel(name=name,
                          inputs=inputs,
                          output_folder=args.out_dir)

    #loop over the cdm object types defined in the configuration
    #e.g person, measurement etc..
    for destination_table,rules_set in config['cdm'].items():
        #loop over each object instance in the rule set
        #for example, condition_occurrence may have multiple rulesx
        #for multiple condition_ocurrences e.g. Headache, Fever ..
        for i,rules in enumerate(rules_set):
            #make a new object for the cdm object
            #Example:
            # destination_table : person
            # get_cdm_class returns <Person>
            # obj : Person()
            obj = get_cdm_class(destination_table)()
            #set the name of the object
            obj.set_name(f"{destination_table}_{i}")

            #call the apply_rules function to setup how to modify the inputs
            #based on the rules
            obj.rules = rules
            obj.define = lambda self : apply_rules(self)
            
            #register this object with the CDM model, so it can be processed
            cdm.add(obj)
            
    cdm.process()
    print ('Finished Producing',cdm.keys())
    
if __name__ == "__main__":
    main()


