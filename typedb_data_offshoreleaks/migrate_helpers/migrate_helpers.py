import pandas as pd
import time 
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from typedb.client import SessionType, TransactionType, TypeDBOptions

def prep_entity_insert_queries(
    df,
    isa_type,
    mappings,
    dict_attr_valuetype
    ):
    '''
    @usage: subroutine to convert a typedb entity subtype and list of string
        containing column-to-schematype mappings into a list of TypeQL insert queries
        everything is vectorized using pandas.Series.str.cat for speed
    @param df: data table
    @param isa_type: typedb_type, string
    @param mappings: list of string: "has {rel_attr_type} <{column_selected}>"
    @param valuetype: attribute valuetype
    @return list of TypeQL insert queries: ["insert $x isa cardealer, has name Lisette;", "insert $x isa ... "]
    '''
    
    list_attr = [mapping.split(" <")[0].split("has ")[1].strip() for mapping in mappings]
    pattern_missing = "|".join([f" has {attr} ?,|has {attr} '',| has {attr} 'nan',| has {attr} nan," for attr in list_attr])
    list_series_stump = []
    # for each input attribute, prepare series of string like ["$x has age 24", "$x has age 64", .. ]
    for mapping in mappings:
        mapping = str(mapping)
        column_selected = mapping.split("<")[1].rstrip(">")
        data = pd.Series(data=df[column_selected], dtype=str)
        attr = mapping.split(" <")[0].split("has ")[1].strip()
        if dict_attr_valuetype[attr]=="STRING":
            stump = " has " + attr + " '"
        else:
            stump =" has " + attr + " "
        list_stump = [stump]*df.shape[0]
        series_stump = pd.Series(data=list_stump, dtype = str)
        # # concatenate with values and the second quotation mark
        series_stump = series_stump.str.cat(others = [data])
        if dict_attr_valuetype[attr] == "STRING":
            series_stump = series_stump.str.cat(others = [pd.Series(data=["'"]*df.shape[0], dtype=str)])
        list_series_stump.append(series_stump)
    # Now concatenate the lists of query stumps onto the initial "insert $x isa plumber; "
    series_queries_init = pd.Series(data=[f"insert $x isa {isa_type}" for i in range(df.shape[0])], dtype=str)
    # concatenate all the above lists to it element-wise; add a final semicolon to complete each query
    series_queries_out = series_queries_init.str.cat(others=list_series_stump, sep=",")
    # remove clauses with missing value attribute
    series_queries_out = series_queries_out.str.replace(pat=pattern_missing, repl="", case=False, regex=True)
    series_queries_out = series_queries_out.str.cat(others=pd.Series(data=["; "]*df.shape[0], dtype=str))
    return list(series_queries_out)


def prep_relation_insert_queries(
    df,
    isa_type,
    mappings,
    dict_attr_valuetype,
    ):
    '''
    @usage: subroutine to convert a typedb relation subtype and list of string
        containing column-to-schematype mappings into a list of TypeQL insert queries
        everything is vectorized using pandas.Series.str.cat for speed
    @param df: data table
    @param isa_type: typedb_type, string
    @param mappings:
        "has {0} <{1}>".format(select_sub,column_selected) for RELATION attributes
        "{0} isa {1}, has {2} <{3}> ... {4} : {5}".format(rp_var, rp_type, rp_attr_type, column_selected, role, rp_var) for ROLEPLAYER attributes
    @param dict_attr_valuetype: attribute valuetype

    @return list of TypeQL insert queries:
        ["match $company isa company, has name 'Telecom'; $customer isa person, has phone-number '+00 091 xxx'; insert (provider: $company, customer: $customer) isa contract;', ... ]
    '''
    
    list_attr = [mapping.split(" <")[0].split("has ")[1].strip() for mapping in mappings]
    pattern_missing = "|".join([f" ?has {attr} ?,| ?has {attr} '',| ?has {attr} 'nan',| ?has {attr} nan," for attr in list_attr])
    series_stump_match_init = pd.Series(data=["match "] * df.shape[0], dtype=str)
    list_series_stump_rp_isa_has = []
    series_stump_insert_init = pd.Series(["insert ("] * df.shape[0], dtype=str)
    list_series_stump_role_rp = []
    series_stump_insert_rel_isa = pd.Series([f") isa {isa_type}" for i in range(df.shape[0])], dtype=str)
    list_series_stump_rel_has = []
    for mapping in mappings:
        if " ... " in mapping:
            # roleplayer attribute
            # "${0} isa {1}, has {2} <{3}>, has {} <{}>, has {} <{}> ... {4} : {5}".format(rp_var, rp_type, rp_attr_type, column_selected, role, rp_var)
            # prepare the isa / has part of the query
            
            series_stump_rp_isa = pd.Series(data=[mapping.split("; ")[0]]*df.shape[0], dtype=str) if "isa" in mapping else None
            list_series_stump_rp_has = []
            list_stump_rp_has = mapping.split(" ... ")[0].split(";")[1:] if type(series_stump_rp_isa) is pd.Series else mapping.split(" ... ")[0].split(";")
            if not list_stump_rp_has:
                raise ValueError(f"role player {mapping.split(' ')[0]} must have unique attributes but has none")
            for stump_rp_has in list_stump_rp_has:
                column_selected = stump_rp_has.split("<")[1].rstrip(">")

                attr = stump_rp_has.split(" <")[0].split("has ")[1].strip()
                if dict_attr_valuetype[attr]=="STRING":
                    stump = f"{mapping.split(' ')[0]} has " + attr + " '"
                else:
                    stump = f"{mapping.split(' ')[0]} has " + attr + " "
                series_stump_rp_has = pd.Series(data=[stump]*df.shape[0], dtype=str)
                # # concatenate with values and the second quotation mark
                data = pd.Series(data=df[column_selected], dtype=str)
                series_stump_rp_has = series_stump_rp_has.str.cat(others = [data])
                if dict_attr_valuetype[attr]=="STRING":
                    series_stump_rp_has = series_stump_rp_has.str.cat(others=[pd.Series(data=["'"]*df.shape[0], dtype=str)])
                list_series_stump_rp_has.append(series_stump_rp_has)
            # concat the isa and the has parts for this roleplayer
            if type(series_stump_rp_isa) is pd.Series:
                series_stump_rp_isa_has = series_stump_rp_isa.str.cat(others=list_series_stump_rp_has, sep="; ") 
            else:
                series_stump_rp_isa_has = list_series_stump_rp_has[0].str.cat(others=list_series_stump_rp_has[1:], sep="; ")  if len(list_series_stump_rp_has)>1 else list_series_stump_rp_has[0]
            # if attr value missing, remove clause
            series_stump_rp_isa_has = series_stump_rp_isa_has.str.replace(pat=pattern_missing, repl="",regex=True)

            list_series_stump_rp_isa_has.append(series_stump_rp_isa_has)

            # append the the (role:$roleplayer) stump to list
            series_stump_role_rp = pd.Series(data=[mapping.split(" ... ")[1]]*df.shape[0], dtype=str)
            list_series_stump_role_rp.append(series_stump_role_rp)

        else:
            # relation attribute
            column_selected = mapping.split("<")[1].rstrip(">")
            attr = mapping.split(" <")[0].split("has ")[1].strip()
            if dict_attr_valuetype[attr]=="STRING":
                stump = "has " + attr + " '"
            else:
                stump = "has " + attr + " "
            series_stump = pd.Series(data=[stump]*df.shape[0], dtype=str)
            # # concatenate with values and the second quotation mark
            data = pd.Series(data=df[column_selected], dtype=str)
            series_stump = series_stump.str.cat(others = [pd.Series(data=data,dtype=str)])
            if dict_attr_valuetype[attr]=="STRING":
                series_stump = series_stump.str.cat(others = [pd.Series(data=["'"]*df.shape[0], dtype=str)])
            list_series_stump_rel_has.append(series_stump)

    # put everything together
    # first concat those lists of series that are separated by commas
    if len(list_series_stump_rp_isa_has)>1:
        series_stump_rp_isa_has = list_series_stump_rp_isa_has[0].str.cat(others=list_series_stump_rp_isa_has[1:], sep="; ")
        series_stump_role_rp = list_series_stump_role_rp[0].str.cat(others=list_series_stump_role_rp[1:], sep=", ")
    elif len(list_series_stump_rp_isa_has)==1:
        series_stump_rp_isa_has = list_series_stump_rp_isa_has[0]
        series_stump_role_rp = list_series_stump_role_rp[0]
    else:
        raise ValueError("relation insert queries must match roleplayers to roles")

    if len(list_series_stump_rel_has)>1:
        series_stump_rel_has = list_series_stump_rel_has[0].str.cat(others=list_series_stump_rel_has[1:], sep=", ")
    elif len(list_series_stump_rel_has)==1:
        series_stump_rel_has = list_series_stump_rel_has[0]
    else:
        series_stump_rel_has = None

    if type(series_stump_rel_has) is pd.Series: # i.e. not None. cannot check the usual way.
        # if attr value missing, remove clause
        series_stump_rel_has = series_stump_rel_has.str.replace(pat=pattern_missing, repl="",regex=True)
    # cat "match" and "$employer isa person, has address '23 Rose Crescent'; ..."
    series_queries_out = series_stump_match_init.str.cat(others=series_stump_rp_isa_has, sep = "")

    # cat "insert (" and "employer: $employer, employee: $employee ... " and ")"
    series_queries_insert = series_stump_insert_init.str.cat(others=[series_stump_role_rp, series_stump_insert_rel_isa], sep="")

    series_queries_out = series_queries_out.str.cat(others=series_queries_insert, sep="; ")
    if type(series_stump_rel_has) is pd.Series:
        series_queries_out = series_queries_out.str.cat(others=[series_stump_rel_has], sep=", ")
    series_queries_out = series_queries_out.str.cat(others=pd.Series(data=["; "]*df.shape[0], dtype=str))
    return list(series_queries_out)


def write_query_batch(
    session,
    batch
    ):
    '''@usage open a write transaction and write all queries in batch
    @param session: a typedb data write session
    @param batch: a list of write queries
    @return None
    '''
    tx = session.transaction(TransactionType.WRITE)
    for query in batch:
        tx.query().insert(query)
    tx.commit()


def multi_thread_write_query_batches(
    session,
    query_batches,
    num_threads=4
    ):
    '''@usage call write_query_batch in parallel on num_threads
    @param session: a typedb data write session
    @param query_batches: an iterator of lists of queries
    @param num_threads integer, max number of threads
    @return None
    '''
    pool = ThreadPool(num_threads)
    pool.map(partial(write_query_batch, session), query_batches)
    pool.close()
    pool.join()


def generate_query_batches(
    queries,
    batch_size
    ):
    '''@usage
    @param queries: a list of queries
    @param batch_size integer, max number of queries to commit in one transaction;
        see https://dev.typedb.ai/docs/examples/phone-calls-migration-python,
        recommended max 500
    @return an iterator that yields batches (lists) of queries
    '''
    batch = []
    for index, data_entry in enumerate(queries):
        batch.append(data_entry)
        if index % batch_size == 0 and index != 0:
            yield batch
            batch = []
    if batch:
        yield batch


def insert_data_bulk(
    client,
    database,
    queries,
    num_threads = 4,
    batch_size = 100,
    typedb_options=None
    ):
    '''
    @usage Carry out insert queries in bulk, for migration
    @param client: open typedb client
    @param database: database
    @param typedb_options: as returned by TypeDBOptions.core() or TypeDBOptions.cluster()
    @param queries: list of string
    @param num_threads integer, max number of threads
    @param batch_size integer, max number of queries to commit in one transaction;
            see https://dev.typedb.ai/docs/examples/phone-calls-migration-python,
            recommended max 500
    @return None
    '''
    if not typedb_options:
        typedb_options = TypeDBOptions.core()
    with client.session(database, session_type = SessionType.DATA, options=typedb_options) as session:
        # carry out write transaction
        # source https://stackoverflow.com/questions/59822987/how-best-to-parallelize-typedb-queries-with-python/59823286#59823286
        start_time = time.time()
        batches = generate_query_batches(queries,  batch_size)
        multi_thread_write_query_batches(session, batches, num_threads)
        elapsed = time.time() - start_time
        print(f'Time elapsed {elapsed:.1f} seconds')
    return None
