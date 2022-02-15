import argparse

def migrator_parser():
    parser = argparse.ArgumentParser(
        description='Define offshore leaks database schema and insert data.')
    parser.add_argument("-a", "--host", help="Server host address (default: localhost)", default="localhost")
    parser.add_argument("-p", "--port", help="Server port (default: 1729)", default="1729")
    parser.add_argument("-n", "--num_threads", type=int,
                        help="Number of threads to enable multiprocessing (default: 4)", default=4)
    parser.add_argument("-c", "--batch_size", help="Sets the number of queries made per commit (default: 250)",
                        default=250)
    parser.add_argument("-d", "--database", help="Database name (default: offshoreleaks)", default="offshoreleaks")
    parser.add_argument("-e", "--existing", action='store_true',
                        help="Write to database by this name even if it already exists (default: False)",
                        default=False)
    parser.add_argument("-f", "--force", action='store_true',
                        help="If a database by this name already exists, delete and overwrite it (default: False)",
                        default=False)
    
    return parser

import pandas as pd
import os 
import re 
from timeit import default_timer as timer   
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from typedb.client import *

from typedb_data_offshoreleaks.migrate_helpers import (
    prep_entity_insert_queries, 
    prep_relation_insert_queries, 
    write_query_batch, 
    multi_thread_write_query_batches,
    generate_query_batches,
    insert_data_bulk
    )

# relative paths
schema_file = "offshoreleaks_schema.tql"
dir_entities = "data/preprocessed/entities"
dir_relations = "data/preprocessed/relations"

parallellisation = 1



if __name__ == "__main__":
    
    start = timer()
    
    # get cmd line arguments
    parser = migrator_parser()
    args = parser.parse_args()
        
    # 0. all attribute value types as a dict through a query
    with TypeDB.core_client(
            address=f"{args.host}:{args.port}",
            parallelisation=parallellisation
        ) as client:
        # checking whether database already exists; if not, create it
        # databases = [db.name() for db in client.databases().all()]
        if args.force:
            try:
                client.databases().get(args.database).delete()
            except Exception:
                pass 
        if client.databases().contains(args.database):
            if not args.existing:
                raise UserWarning(f"database {args.database} already exists. Use --existing to write into existing database or --force to delete it and start anew.")
        else:
            client.databases().create(args.database)    
        query_define = open(schema_file, "r").read()
        # define schema
        with client.session(args.database, SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as write_transaction:
                write_transaction.query().define(query_define)
                write_transaction.commit()
        # load schema
        # get all attributes and their valuetypes
        with client.session(args.database, SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.READ) as read_transaction:
                iterator_conceptMap = read_transaction.query().match("match $x sub attribute; not {$x type attribute;}; ")
                list_concept = [conceptMap.get("x") for conceptMap in iterator_conceptMap]
                dict_attr_valuetype = {concept.get_label().name():concept.get_value_type().name for concept in list_concept}
        # get relation roles
        with client.session(args.database, SessionType.SCHEMA) as session:
            dict_rel_roles = {}
            with session.transaction(TransactionType.READ) as read_transaction:
                iterator_conceptMap = read_transaction.query().match("match $x sub relation; not {$x type relation;}; $x relates $y; ")
                for conceptMap in iterator_conceptMap:
                    reltype = conceptMap.get("x").get_label().name()
                    if not reltype in dict_rel_roles:
                        dict_rel_roles[reltype] = []
                    dict_rel_roles[reltype].append(conceptMap.get("y").get_label().name())

    # provide pandas read_csv with datatypes to avoid having to load whole df into memory first to guess
    dict_dtype_convert = {
        "LONG": int,
        "DOUBLE": float, 
        "STRING": str, 
        "BOOLEAN": bool, 
        "DATETIME": str
    }

    dict_attr_dtype = {
        attr: dict_dtype_convert[dict_attr_valuetype[attr]] for attr in dict_attr_valuetype.keys()
        }

    # prepare queries
    pattern_rm_thingType = re.compile("^relationships_|_?clean_formatted_?|^nodes-|.csv$")
    pattern_rm_underscore_prefix = re.compile("^_")


    for file in os.listdir(dir_entities):
        thingType = re.sub(
            pattern_rm_thingType, "", file
            )
        if thingType == "addresses":
            thingType = "node_address"
        elif thingType == "entities":
            thingType = "org_entity"
        elif thingType == "intermediaries":
            thingType = "intermediary"
        elif thingType == "officers":
            thingType = "officer"
        elif thingType == "others":
            thingType = "other"
        else:
            raise ValueError(f"unknown thingType {thingType}")
        # construct mappings for each column to schema variable
        
        df = pd.read_csv(
            dir_entities+"/"+file, 
            dtype=dict_attr_dtype
            )
        mappings = [f"has {re.sub(pattern_rm_underscore_prefix, '', colname)} <{colname}>" for colname in df.columns if re.sub(pattern_rm_underscore_prefix, '', colname) in dict_attr_valuetype.keys()]
        print(f"\npreparing {thingType} insert queries")
        queries = prep_entity_insert_queries(
            df,
            thingType,
            mappings=mappings,
            dict_attr_valuetype=dict_attr_valuetype
            )
        with TypeDB.core_client(
            address=f"{args.host}:{args.port}",
            parallelisation=parallellisation
        ) as client:
            print(f"\nperforming {thingType} insert queries")
            insert_data_bulk(
                client,
                args.database,
                queries,
                num_threads = args.num_threads,
                batch_size = args.batch_size,
                typedb_options=None
                )
        print(f"\ndone inserting {thingType} entities")

    # relations
    for file in os.listdir(dir_relations):
        thingType = re.sub(
            pattern_rm_thingType, "", file
            )
        # 3. construct mappings for each columns
        df = pd.read_csv(
            dir_relations+"/"+file,
            dtype = dict_attr_dtype
            )
        # relation attribute
        mappings = [f"has {re.sub(pattern_rm_underscore_prefix, '', colname)} <{colname}>" for colname in df.columns if re.sub(pattern_rm_underscore_prefix, '', colname) in dict_attr_valuetype.keys()]

        if thingType == "registered_address":
            # directed_relation
            start_role = [role for role in dict_rel_roles[thingType] if "has_" in role][0]
            end_role = [role for role in dict_rel_roles[thingType] if "is_" in role][0]
        elif thingType in ["intermediary_of", "officer_of", "underlying"]:
            # directed_relation
            start_role = [role for role in dict_rel_roles[thingType] if "is_" in role][0]
            end_role = [role for role in dict_rel_roles[thingType] if "has_" in role][0]
        else:
            # undirected relation
            start_role = end_role = dict_rel_roles[thingType][0]
        source_mapping =  f"$start isa thing; $start has id <_start> ... {start_role} : $start"
        target_mapping =  f"$end isa thing; $end has id <_end> ... {end_role} : $end"
        mappings.append(source_mapping)
        mappings.append(target_mapping)
        print(f"\npreparing {thingType} insert queries")
        queries = prep_relation_insert_queries(
            df,
            thingType,
            mappings,
            dict_attr_valuetype,
            )
        with TypeDB.core_client(
            address=f"{args.host}:{args.port}",
            parallelisation=parallellisation
        ) as client:
            print(f"\nperforming {thingType} insert queries")
            insert_data_bulk(
                client,
                args.database,
                queries,
                num_threads = args.num_threads,
                batch_size = args.batch_size,
                typedb_options=None
                )
        print(f"\ndone inserting {thingType} relations")
            
    end = timer()
    time_in_sec = end - start
    print("Elapsed time: " + str(time_in_sec) + " seconds.")