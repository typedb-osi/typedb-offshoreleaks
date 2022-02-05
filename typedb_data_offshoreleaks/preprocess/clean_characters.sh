#!/bin/bash
# @usage
# ls dir/with/files/*.csv | xargs bash clean_characters.sh

for ((i = 1; i <= $#; i++ )); do
  FILE_OUT=$(echo "${!i}" | sed -E -e 's/\.csv/_clean.csv/g')
  # swap valid delimiters (to remove stray ones) | remove most non-alphanumeric characters including stray commas | reinsert comma delimiters | remove quotes | remove excess fields
  sed -E -e 's/[^A-Za-z0-9 .,_"/-]//g' -e "s/'//g" < ${!i} > $FILE_OUT 
done