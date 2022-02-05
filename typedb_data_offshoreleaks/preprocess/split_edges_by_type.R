# split edges by type, and within edge type, by roleplayers
# @usage
# Rscript split_edges_by_type.R --file path/to/file.csv  --column_edge_type rel_type

###### packages ######

library("optparse")
library("data.table")
library("here")

###### constants ######


option_list <- list(
  make_option("--file", type="character",
              help = "path to edges file"),
  make_option("--dir_out", type="character", default=".",
              help = "output directory"),
  make_option("--column_edge_type", type="character", default="TYPE",
              help = "edges file column containing edge type [default %default]")
  )

opt <- parse_args(OptionParser(option_list=option_list))

file = opt$file
dir_out = opt$dir_out
column_edge_type = opt$column_edge_type

###### load data ######

# setwd(dir_data)

# edges
dt_edges = data.table::fread(file, fill=TRUE)
# 
# names(list_dt_nodes) = original_node_types

# subset the edges by type and combinations of roleplayer type

if (!substr(dir_out,nchar(dir_out),nchar(dir_out))=="/") {
  dir_out = paste0(dir_out, "/")
}
for (eachtype in unique(dt_edges[[column_edge_type]])) {
    condition = quote(dt_edges[[column_edge_type]] == eachtype)
    dt_edges_sub = dt_edges[eval(condition)]
    eachtype = gsub("\\/|\\ ", "_", eachtype)
    file_split = strsplit(file,"/")[[1]]
    file = file_split[length(file_split)]
    file_out = paste0(gsub("\\.csv$","",file), "_", eachtype, ".csv")
    fwrite(x=dt_edges_sub, file = paste0(dir_out, file_out))
}

message("done splitting edges!")
