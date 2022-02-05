
#!/bin/bash

echo "downloading the data from ICIJ"

mkdir data && mkdir data/raw && mkdir data/preprocessed
wget -P data/raw/ https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb-20220110.zip

# unzip to csv 
unzip data/raw/*.zip -d data/raw
echo "cleaning up characters"
ls data/raw/*s.csv | xargs bash typedb_data_offshoreleaks/preprocess/clean_characters.sh

echo "format dates, names and corporate abbreviations"
echo "formatting entities"
Rscript typedb_data_offshoreleaks/preprocess/format_data.R --file data/raw/nodes-entities_clean.csv --date_column_regex ".*date$" --date_entry_regex "^\\d\\d-\\D\\D\\D-\\d\\d\\d\\d$|^\\d\\d?[/.-]\\d\\d?[/.-]\\d\\d\\d?\\d?$|^\\d\\d\\d\\d\\d\\d\\d\\d$|^\\D\\D\\D \\d\\d \\d\\d\\d\\d$" --date_else "" --to_title_column_regex "name|original_name|formner_name|address" --company_form_column_regex "name|original_name|former_name" --dir_out "data/preprocessed/entities"
echo "formatting officers"
Rscript typedb_data_offshoreleaks/preprocess/format_data.R --file data/raw/nodes-officers_clean.csv --to_title_column_regex "name|address" --company_form_column_regex "name" --dir_out "data/preprocessed/entities"
echo "formatting intermediaries"
Rscript typedb_data_offshoreleaks/preprocess/format_data.R --file data/raw/nodes-intermediaries_clean.csv --to_title_column_regex "name|address" --company_form_column_regex "name" --dir_out "data/preprocessed/entities"
echo "formatting addresses"
Rscript typedb_data_offshoreleaks/preprocess/format_data.R --file data/raw/nodes-addresses_clean.csv --to_title_column_regex "name|address" --company_form_column_regex "name" --dir_out "data/preprocessed/entities"
echo "formatting others"
Rscript typedb_data_offshoreleaks/preprocess/format_data.R --file data/raw/nodes-others_clean.csv --date_column_regex ".*date$" --to_title_column_regex "name|address" --company_form_column_regex "name" --dir_out "data/preprocessed/entities"

echo "formatting relationships"
# NB: output to data/raw, as we still need to split by type
Rscript typedb_data_offshoreleaks/preprocess/format_data.R --file data/raw/relationships_clean.csv --date_column_regex ".*date$" --date_entry_regex "^\\d\\d-\\D\\D\\D-\\d\\d\\d\\d$|^\\d\\d?[/.-]\\d\\d?[/.-]\\d\\d\\d?\\d?$|^\\d\\d\\d\\d\\d\\d\\d\\d$|^\\D\\D\\D \\d\\d \\d\\d\\d\\d$|^\\d\\d\\d\\d$" --date_else "" --dir_out "data/raw"

echo "split relationships by type"
Rscript typedb_data_offshoreleaks/preprocess/split_edges_by_type.R --file data/raw/relationships_clean_formatted.csv --dir_out data/preprocessed/relations  --column_edge_type _type

# remove bad role player (edits file in place)
Rscript typedb_data_offshoreleaks/preprocess/remove_bad_role_players.R

echo "preprocessing done!"
