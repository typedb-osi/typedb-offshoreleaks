# format dates for TypeDB
# @usage: Rscript format_data.R --file path/to/file.csv --date_entry_regex "^\\d\\d-\\D\\D\\D-\\d\\d\\d\\d$|^\\d\\d?[/.-]\\d\\d?[/.-]\\d\\d\\d?\\d?$|^\\d\\d\\d\\d\\d\\d\\d\\d$|^\\D\\D\\D \\d\\d \\d\\d\\d\\d$|^\\d\\d\\d\\d$" --date_else ""

# @description does several formatting steps.
# 1. convert the following datetime formats: 
# * 30-MAY-1991 # most common. appears as lower-case (30-May-1991) in paradise papers (second half)
# * 04[/.-]02[/.-]2005 # m or y with or without leading "0". Smetimes just yy, e.g. node_id 22013384. bahamas leaks edges
# * ddmmyyyy # appears in Bahamas leaks edges (raw)
# * Sep 25 2012 # appears in paradise papers entity e.g. node_id 59052389. Raw csv even contains a comma (Sep 25, 2012)
# * yyyy # appears in paradise papers
# to TypeDB datetime format: 
# * yyyy-mm-dd
# TypeDB docs: https://docs.vaticle.com/docs/schema/concepts#define-an-attribute
# 2. Put all data in Title Case
# 3. Remove repeated or padding spaces
# 4. Standardize company forms to their appreviations (Limited -> Ltd.) and remove any resulting repetitions
# @return file with file_out_suffix appended to filename

## packages 
for (pkg in c("optparse", "tidyr", "data.table", "stringr", "purrr")) {
  pkg_installed <- require(pkg, character.only = T)
  if (!pkg_installed) {
    install.packages(pkg, repos="https://cloud.r-project.org")
    require(pkg)
  } 
}

option_list <- list(
  make_option("--file", type="character",
              help = "path to delimited file"),
  make_option("--date_column_regex", type="character", default = "date",
              help = "regex to identify columns that may contain dates to reformat, defaults to 'date',[default %default]"),
  make_option("--date_entry_regex", type="character", default = "^\\d\\d-\\D\\D\\D-\\d\\d\\d\\d$|^\\d\\d?[/.-]\\d\\d?[/.-]\\d\\d\\d?\\d?$|^\\d\\d\\d\\d\\d\\d\\d\\d$|^\\D\\D\\D \\d\\d \\d\\d\\d\\d$|^\\d\\d\\d\\d$",
              help = "raw date patterns to detect and modify (is done per pattern). Can only handle these predetermined patterns, [default %default]"),
  make_option("--date_else", type="character", default = NULL,
              help = "What to insert in entries if all date formatting fails. If NULL, throws an error, [default %default]"),
  make_option("--to_title_column_regex", type="character", default = NULL,
              help = "regex to identify columns to change to Title Case, [default %default]"),
  make_option("--company_form_column_regex", type="character", default = NULL,
              help = "regex to identify columns that may contain company types to reformat, [default %default]"),
  make_option("--file_out_suffix", type="character", default = "_formatted",
              help = "suffix for output files, [default %default]"),
  make_option("--dir_out", type="character", default = NULL,
              help = "output directory, if different from file location, [default %default]")
  )

opt <- parse_args(OptionParser(option_list=option_list))

file = opt$file
date_column_regex = opt$date_column_regex
date_entry_regex = opt$date_entry_regex
date_else = opt$date_else
to_title_column_regex = opt$to_title_column_regex
company_form_column_regex = opt$company_form_column_regex
file_out_suffix = opt$file_out_suffix
dir_out = opt$dir_out

# load data
dt <- fread(file, fill=TRUE)
columns_dates = grep(pattern = date_column_regex, x = colnames(dt), value=T)
vec_date_replacement = c("01","02","03","04","05","06","07","08","09","10","11","12")
# convert from format 30-MAY-1991 to 30-05-1991 to yyyy-mm-dd
names(vec_date_replacement) = c("JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC")

all_good = TRUE

# mock data
if (F) dt = data.table(
  "dates1" =c("23-JUN-2011", "01-JAN-2001", "14-DEC-1994", "12-MAR-2011"),
  "dates2"=c("12/24/1999", "2/3/1999", "2/3/01", "2/12/89"),
  "dates3"=c("01092002", "02121999", "02102013", "02092009"),
  "dates4"=c("Feb 23 2012", "Mar 01 2001", "Dec 02 1983", "Apr 10 1989"),
  "dates5"=c("2012", "2001", "1983", "1989"),
  "name"=c("London LIMITED", "GOLDROCK s.a ", "Societe financiere S.P.R.L. ", "IMPORT/EXPORT NV" ))

if (grepl(pattern="^\\d\\d-\\D\\D\\D-\\d\\d\\d\\d$", x=date_entry_regex, fixed=TRUE)) {
  for (column in columns_dates) {
    dt[[column]] <- ifelse(
      str_detect(string = dt[[column]], pattern = "^\\d\\d-\\D\\D\\D-\\d\\d\\d\\d$"),
      yes = dt[[column]] %>%
          str_split(.,pattern = "-") %>% 
          purrrmap(., function(x) {
            if (length(grep(x[2],names(vec_date_replacement), ignore.case=T))>0) {
              x[2] <- vec_date_replacement[grep(x[2],names(vec_date_replacement), ignore.case=T)[1]]  
            }
            return(x)
            }) %>%
          purrrmap(., rev) %>% 
          purrrmap(., function(x)paste(x, collapse = "-")),
      no = dt[[column]])
  }
}

if (grepl(pattern="^\\d\\d?[/.-]\\d\\d?[/.-]\\d\\d\\d?\\d?$", x=date_entry_regex, fixed=TRUE)) {
  for (column in columns_dates) {
    dt[[column]] <- ifelse(
      str_detect(string = dt[[column]], pattern = "^\\d\\d?[/.-]\\d\\d?[/.-]\\d\\d\\d?\\d?$"),
      yes = dt[[column]] %>%
          str_split(.,pattern = "[/.-]") %>% 
          purrrmap(., rev) %>% 
          purrrmap(., function(x) {
            x[1] = ifelse(nchar(x[1])==4, yes=x[1], no=paste0(ifelse(as.integer(x[1])>17,"19","20"),x[1]))
            c(x[1], str_pad(x[2:3],c(2,2),"left",c("0","0")))
          }) %>%
          purrrmap(., function(x) paste(x, collapse = "-")),
      no =dt[[column]]
    )
  }
}

if (grepl(pattern="^\\d\\d\\d\\d\\d\\d\\d\\d$", x=date_entry_regex, fixed=TRUE)) {
  for (column in columns_dates) {
    dt[[column]] <- ifelse(
      str_detect(string = dt[[column]], pattern = "^\\d\\d\\d\\d\\d\\d\\d\\d$"),
      yes = str_c(str_sub(dt[[column]],5,8),str_sub(dt[[column]],3,4),str_sub(dt[[column]],1,2), sep="-"),
      no = dt[[column]])
  }
}

if (grepl(pattern="^\\D\\D\\D \\d\\d \\d\\d\\d\\d$", x=date_entry_regex, fixed=TRUE)) {
  for (column in columns_dates) {
    dt[[column]] <- ifelse(
      str_detect(string = dt[[column]], pattern = "^\\D\\D\\D \\d\\d \\d\\d\\d\\d$"),
        yes = dt[[column]] %>%
          str_split(.,pattern = " ") %>% 
          purrrmap(., function(x) {
            if (length(grep(pattern=x[1],x=names(vec_date_replacement), ignore.case = T))>0) {
                x[1] <- vec_date_replacement[grep(pattern=x[1],x=names(vec_date_replacement), ignore.case = T)[1]]  
            } 
            return(x)
            }) %>%
          purrrmap(., function(x) c(x[3], x[1], x[2])) %>% 
          purrrmap(., function(x)paste(x, collapse = "-")),
        no = dt[[column]])
  }
}

if (grepl(pattern="^\\d\\d\\d\\d$", x=date_entry_regex, fixed=TRUE)) {
  for (column in columns_dates) {
    dt[[column]] <- ifelse(
      str_detect(string = dt[[column]], pattern = "^\\d\\d\\d\\d$"),
      yes = str_c(dt[[column]], "01", "01", sep="-"),
      no = dt[[column]])
  }
}

# check whether the date reformatting succeeded
for (column in columns_dates) {
  # check for non-empty cells that still do not conform to the typedb format
  logical_remaining_problems = str_length(dt[[column]]) > 2  &
    (
      # not formatted correctly for typeql
      str_detect(dt[[column]], pattern="^\\d\\d\\d\\d-\\d\\d-\\d\\d$", negate=T) |
      # not a possible date
        sapply(dt[[column]], function(x) { 
        # check that the date can exist
        attempt = try(as.Date(x), silent = T)
        return(if ("try-error" %in% class(attempt)) T else F)
      })
    )
    # str_split(dt[[column]], "-") %>% 
    # sapply(., function(x) {
    #   tryCatch({
    #     as.integer(x[1])>2018 | as.integer(x[2])>12 | as.integer(x[3])>31  
    #   }, 
    #   warning = function(war) F,
    #   error = function(err) F)
    #   })
  if (sum(logical_remaining_problems, na.rm = T)>0) {
    warning(paste0("column ", column, ": ", sum(logical_remaining_problems), " remaining date formatting problem rows: ", paste(head(which(logical_remaining_problems)), collapse=","), ".."))
    if (!is.null(date_else)) {
      message(paste0("replacing problem date entries with '", date_else, "'"))
      dt[[column]][logical_remaining_problems] <- date_else
    } else {
      all_good = F
    }
  }
}

if (!all_good) stop("some dates not formatted correctly")

if (!is.null(to_title_column_regex)) {
  columns_to_title = grep(pattern = to_title_column_regex, x = colnames(dt), value=T)
  for (column in columns_to_title) {
    dt[[column]] <- str_to_title(dt[[column]])
  }
}

# remove trailing and repeated whitespace
for (column in colnames(dt)) {
  dt[[column]] <- str_squish(dt[[column]])
}

if (!is.null(company_form_column_regex)) {
  columns_company_form = grep(pattern = company_form_column_regex, x = colnames(dt), value=T)
  # https://en.wikipedia.org/wiki/List_of_legal_entity_types_by_country#
  # Purpose: TypeDB rules require exact matches (case-sensitive) to identify e.g. identically named entities
  # NB: only most prevalent company forms, not exhaustive
  # NB: matching here is not case-sensitive. Words have previously been changed to title case
  company_replacement = c(
    # UK, commonwealth
    " Ltd."=c(" Limited$| Ltd$"),
    " PLC"=c(" P\\.?L\\.?C\\.?$"),
    " LP"=c(" L\\.?P\\.?$"),
    " LLP"=c(" L\\.?L\\.?P\\.?$"), # NB: must be replaced after LP
    " SLP"=c(" S\\.?L\\.?P\\.?$"), # Scottish limited partnership)
    # US
    " Inc."=c(" Incorporated$| Inc$| Corp\\.?$| Corporation$"),
    " LP" = c(" L\\.?P\\.?$"),
    " LLC"=c(" L\\.?L\\.?C\\.?$| L\\.?C\\.?$| Ltd\\.? Co\\.?$"),
    " PLLC"=c(" P\\.?L\\.?L\\.?C\\.?$"), # NB: must be replaced after LLC
    " PC"=c(" P\\.?C\\.?$"), # professional corporation
    # Latin
    " Ltda."=c(" Ltda\\.?$"),
    " S.A."=c(" S\\.?A\\.?$"), # societé anonyme / Sociedade anônima / Sociedad Anonima
    " S.A.S."=c(" S\\.?A\\.?S\\.?$"), # Sociedad Anonima Simplificada
    # Francophone
    " SPRL"=c(" S\\.?P\\.?R\\.?L\\.?$"), # société privée à responsabilité limitée 
    " SRL"=c(" S\\.?R\\.?L\\.?$"), # société responsabilité limitée
    # Dutch
    " N.V."=c(" Naamloze vennootschap$| N\\.?V\\.?$"),
    " B.V."=c(" besloten vennootschap$| B\\.?V\\.?$"),
    # Germanophone
    " AG" = c(" A\\.?G\\.?$"), # (Aktiengesellschaft): ≈ plc (UK). Minimum capital €70,000.
    " GmbH" = c(" G\\.?m\\.?b\\.?H\\.?$"), #  (Gesellschaft mit beschränkter Haftung): ≈ Ltd. (UK). Minimum capital €35,000.
    # Nordic
    " I/S"=c(" I\\.?S\\.?$"),
    " IVS"=c(" I\\.?V\\.?S\\.?$"),
    " ApS"=c(" A\\.?P\\.?S\\.?$"),
    " A/S"=c(" A\\.?S\\.?$"),
    " K/S"=c(" Kommanditselskab$|K\\.?S\\.?$"),
    # Aruba
    " A.V.V."=c(" A\\.?V\\.?V\\.?$"), # e.g. ELDON INTERNATIONAL A.V.V. (I.L.), AIRGATE INTERNATIONAL A.V.V. (I.L.) in	Paradise Papers
    # Aruba and Barbados
    " I.L."=c(" I\\.L\\.?$") # e.g. ELDON INTERNATIONAL A.V.V. (I.L.) in	Paradise Papers. 
    )
  
  for (column in columns_company_form) {
    for (name in names(company_replacement)) {
      dt[[column]] <- gsub(company_replacement[name], name, dt[[column]], ignore.case = T) 
      #  unlike str_replace_all, gsub has ignore.case
    }
  }
}

file_split = strsplit(file, "/")[[1]]
file_out = gsub("\\.csv$", paste0(file_out_suffix, ".csv"), file_split[[length(file_split)]])

if (is.null(dir_out)) {
  dir_out = paste(file_split[1:(length(file_split)-1)], collapse="/") 
}

if (!substr(dir_out,nchar(dir_out),nchar(dir_out))=="/") {
  dir_out = paste0(dir_out, "/")
}

fwrite(dt, file=paste0(dir_out, file_out))

message("done!")  
