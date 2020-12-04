

#' Domo CLI
#'
#' @param domo_command
#'
#' @return
#' @export
#'
domoCli <- function(domo_command) {
	readr::write_lines(c(domoConnect(), domo_command, 'quit'), file = 'temp.txt')
	jar_path <- system.file('java', 'domoUtil.jar', package = 'domoclir')
	system(glue::glue("java -jar {jar_path} --script temp.txt"))
	fs::file_delete('temp.txt')
}


# whoami ------------------------------------------------------------------

#' Who am I
#'
#' whoami: displays the current user identity from the Domo server
#'
#' @return
#' @export
#'
whoami <- function() {
	domo_command <- 'whoami'
	domoFn(domo_command)
	}


# dataset-functions -------------------------------------------------------

#' Dataset Get Version
#'
#' get-version: returns the specified Domo dataset version
#'
#' @param ds_id dataset id
#' @param format either `wide` or `long`
#'
#' @return
#' @export
#'
dsGetVersion <- function(ds_id, format = 'wide') {
	filename <- 'temp.json'
	domo_command <- glue::glue("get-version --filename {filename} --id {ds_id}")
	domoFn(domo_command)

	x <- jsonlite::read_json('temp.json', simplifyVector = TRUE) %>% as_tibble() %>%
		mutate_at(vars(datetimeRecorded, datetimeUploadCompleted),
				  list(~anytime::anytime(as.numeric(.x)/1000)))

	fs::file_delete('temp.json')

	if(format == 'wide') {
		return(x)
	}

	if(format == 'long') {
		# long format
		x %>%
			mutate_all(as.character) %>%
			pivot_longer(cols = everything())
		}
}


#' Dataset Derive Schema
#'
#' derive-schema: derives a dataset schema from an input file
#'
#' @param data either a `csv` file or `data.frame`
#' @param row_limit Optional, rows to parse for schema detection, default = 1000
#' @param schema_file filename for schema definition
#'
#' @return
#' @export
#'
dsDeriveSchema <- function(data, row_limit = 1000, schema_file = 'schemafile.json') {
	if('data.frame' %in% class(data)) {
		csv_file <- 'temp.csv'
		write_csv(data, 'temp.csv')
	}
	if('character' %in% class(data)) {
		csv_file <- data
	}
	args <- list(data = csv_file, `row-limit` = row_limit, `schema-file` = schema_file) %>%
		fnArgs()
	domo_command <- glue::glue("derive-schema {args}")
	domoFn(domo_command)
	if('data.frame' %in% class(data)) {
		fs::file_delete('temp.csv')
	}
}


#' Dataset Set Schema
#'
#' set-schema: sets the specified schema defintion on a dataset. This command does not work with data views (use `create-dataview` for data views)
#'
#' @param ds_id dataset id
#' @param schema_file schema definition file
#'
#' @return
#' @export
#'
dsSetSchema <- function(ds_id, schema_file = 'schemafile.json') {
	args <- list(f = schema_file, id = ds_id) %>%
		fnArgs()
	domo_command <- glue::glue("set-schema {args}")
	domoFn(domo_command)
}



#' Dataset Create
#'
#' create-dataset: Creates a Domo Dataset
#' Many variations to how this works depending on supplied params, need more documentation!
#'
#' @param description Optional, dataset description (optional)
#' @param ds_id Optional, dataset id (optional) - if provided, schema of dataset is updated
#' @param name dataset name
#' @param schema_file filename for schema definition
#' @param type dataset type
#' @param index Optional, index dataset after schema change (optional)
#'
#' @return
#' @export
#'
dsCreate <- function(description = NULL, ds_id = NULL, name, schema_file, type, index = NULL) {
	args <- list(description = description, id = ds_id, name = name, `schema-file` = schema_file, type = type, index = index) %>%
		fnArgs()
	domo_command <- glue::glue("create-dataset {args}")
	message(domo_command)
	domoFn(domo_command)
}

#' Dataset Upload
#'
#' upload-dataset: Uploads data to a Domo DataSet. An advantage of the CLI over workbench is that the CLI supports a model where if you can query the source in a way that unloads the result of the query to multiple CSV files, you can then directly push those csv parts in parallel to Domo.  If the CSV part files are precisely formatted according to the Domo spec, we don’t need to read each row as the data is being sent. Workbench has to read every cell so that it can format the data to be sent.  A pipeline setup using an unload method into many parts can take 1/10 of the time vs the row-at-a-time model that workbench uses.
#'
#' @param data either a `csv` file or a `data.frame`
#' @param ds_id dataset id
#' @param headers data file has a header row
#' @param ds_name dataset name
#' @param schema_file filename for schema definition
#' @param type dataset type
#' @param append append to existing data (this options is required when doing Upserts or Partitions)
#'
#' @return
#' @export
#'
dsUpload <- function(data, ds_id, headers = TRUE, ds_name = NULL, schema_file = NULL, type = NULL, append = NULL) {
	if('data.frame' %in% class(data)) {
		csv_file <- 'temp.csv'
		write_csv(data, 'temp.csv')
	}
	if('character' %in% class(data)) {
		csv_file <- data
	}
	args <- list(data = csv_file, headers = headers, id = ds_id, name = ds_name, `schema-file` = schema_file, type = type, append = append) %>%
		fnArgs()
	message(args)
	domo_command <- glue::glue("upload-dataset {args}")
	message(domo_command)
	domoFn(domo_command)
	if('data.frame' %in% class(data)) {
		fs::file_delete('temp.csv')
	}
}

#' Dataset Export
#'
#' export-data: exports datset to a `csv` file
#'
#' @param ds_id dataset id
#' @param filename export filename
#' @param read_file either `TRUE` or `FALSE`
#' @param query optional query
#' @param queryfile query filename
#'
#' @return
#' @export
#'
dsExport <- function(ds_id, filename = NULL, read_file = TRUE, query = NULL, queryfile = NULL) {
	if(is.null(filename)) {
		temp_file <- TRUE
		filename <- tempfile(fileext = '.csv')
		}
	args <- list(id = ds_id, filename = filename, query = query, queryfile = queryfile) %>%
		fnArgs()
	message(args)
	domo_command <- glue::glue("export-data {args}")
	message(domo_command)
	domoFn(domo_command)
	if(read_file) {
		data.table::fread(filename, encoding = 'UTF-8') %>% as_tibble()
	}
}

#' Dataset Export Version
#'
#' export-version: exports a specific dataset version to a `csv` file
#'
#' @param ds_id dataset id
#' @param version version
#' @param filename export filename
#' @param read_file either `TRUE` or `FALSE`
#'
#' @return
#' @export
#'
dsExportVersion <- function(ds_id, version, filename = 'temp.csv', read_file = TRUE) {
	args <- list(id = ds_id, version = version, filename = filename) %>%
		fnArgs()
	domo_command <- glue::glue("export-version {args}")
	message(domo_command)
	domoFn(domo_command)
	if(read_file) {
		data.table::fread(filename, encoding = 'UTF-8') %>% as_tibble()
	}
}


#' Move Data
#'
#' move-data: Moves dataa from one Domo dataset to another
#'
#' @param source source dataset
#' @param destination destination dataset
#' @param append c(TRUE, NULL) append to existing data
#'
#' @return
#' @export
#'
dsMoveData <- function(source, destination, append = NULL) {
	args <- list(source = source, destination = destination, append = append) %>%
		fnArgs()
	domo_command <- glue::glue("move-data {args}")
	message(domo_command)
	domoFn(domo_command)
}




# dataflow-functions ------------------------------------------------------


#' Dataflow List
#'
#' list-dataflow: lists domo dataflows
#'
#' @param df_id dataflow id
#' @param filename export filename
#' @param limit the number of dataflows/executions to return
#' @param name_like name filter
#' @param offset the offset into the pagination
#' @param time the last start time in millis
#' @param versions list dataflow versions
#'
#' @return
#' @export
#'
dfList <- function(df_id = NULL, filename = NULL, limit = 1, name_like = NULL, offset = NULL,  time = NULL, versions = NULL) {
	if(is.null(filename)) {
		filename <- 'temp.json'
	}
	args <- list(filename = filename, id = df_id, limit = limit, `name-like` = name_like, offset = offset, time = time, versions = versions) %>%
		fnArgs()
	message(args)
	domo_command <- glue::glue("list-dataflow {args}")
	message(domo_command)
	domoFn(domo_command)
}


#' Dataflow Read
#'
#' Reads a dataflow's `json` file
#'
#' @param filename filename
#'
#' @return
#' @export
#'
dfRead <- function(filename) {
	readLines(filename, warn = FALSE, encoding = 'ISO-8859-1') %>%
		jsonlite::fromJSON(flatten = TRUE)
	}

#' Dataflow Run Now
#'
#' dataflow-run-now: start a domo dataflow
#'
#' @param df_id dataflow id
#'
#' @return
#' @export
#'
dfRun <- function(df_id) {
	args <- list(id = df_id) %>% fnArgs()
	message(args)
	domo_command <- glue::glue("dataflow-run-now {args}")
	message(domo_command)
	domoFn(domo_command)
}


#' Dataflow Set Properties
#'
#' set-dataflow-properties: set a domo dataflow property
#'
#' @param create create
#' @param definition definition
#' @param enable enable (true/false)
#' @param df_id dataflow id
#' @param df_name name
#' @param subscribe subscriber (true/false)
#'
#' @return
#' @export
#'
dfSetProperties <- function(definition, df_id, df_name = NULL, enable = NULL, create = NULL, subscribe = NULL) {
	args <- list(create = create, definition = definition, enable = enable, id = df_id, name = df_name, subscribe = subscribe) %>%
		fnArgs()
	message(args)
	domo_command <- glue::glue("set-dataflow-properties {args}")
	message(domo_command)
	domoFn(domo_command)
}



# card-functions ----------------------------------------------------------

#' Card Export Data
#'
#' download-card-data: returns card data as a `csv` file
#'
#' @param card_id card id
#' @param filename export filename
#' @param read_file either `TRUE` or `FALSE`
#'
#' @return
#' @export
#'
cardExport <- function(card_id, filename = 'temp.csv', read_file = TRUE) {
	args <- list(id = card_id, filename = filename) %>%
		fnArgs()
	domo_command <- glue::glue("download-card-data {args}")
	domoFn(domo_command)
	if(read_file) {
		data.table::fread(filename, encoding = 'UTF-8') %>% tibble(.name_repair = 'minimal')
	}
}


#' Card Backup Card
#'
#' backup-card: export a specified card as a `json` blob to a file or stdout that can be used to restore it later
#'
#' @param card_id card id
#' @param filename export filename
#' @param read_file either `TRUE` or `FALSE`
#'
#' @return
#' @export
#'
cardBackup <- function(card_id, filename = 'temp.json', read_file = TRUE) {
	args <- list(id = card_id, filename = filename) %>%
		fnArgs()
	domo_command <- glue::glue("backup-card {args}")
	domoFn(domo_command)
	if(read_file) {
		dfRead('temp.json')
	}
}


