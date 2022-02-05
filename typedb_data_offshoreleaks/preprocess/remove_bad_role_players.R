library("data.table")

# remove the 3 officer_of rows where is_officer is an org_entity
dt_officer_of = fread("data/preprocessed/relations/relationships_clean_formatted_officer_of.csv")
dt_org_entity = fread("data/preprocessed/entities/nodes-entities_clean_formatted.csv")
dt_officer_of_filter = dt_officer_of[!dt_officer_of[["_start"]] %in% dt_org_entity[["_id"]]]
fwrite(dt_officer_of_filter, "data/preprocessed/relations/relationships_clean_formatted_officer_of_filter.csv")
print("done!")
