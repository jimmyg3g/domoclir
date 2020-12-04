# help functions ----------------------------------------------------------

domoConnect <- function() {
	token <- Sys.getenv('DOMO_CLI_TOKEN')
	base_url <- Sys.getenv('DOMO_CLI_BASE_URL')
	glue::glue("connect --token {token} --server {base_url}.domo.com")
}

domoFn <- function(domo_command) {
	tmp_txt <- tempfile(fileext = '.txt')
	readr::write_lines(c(domoConnect(), domo_command, 'quit'), file = tmp_txt)
	jar_path <- system.file('java', 'domoUtil.jar', package = 'domoclir')
	system(glue::glue("java -jar {jar_path} --script {tmp_txt}"))
	file.remove(tmp_txt)
}

fnArgs <- function(args) {
	args <- purrr::compact(args)
	x <- purrr::map_chr(seq_along(args), function(i) {
		this_name <- names(args)[[i]]
		this_value <- args[[i]]
		if(this_value != TRUE) {
			this_value <- glue::double_quote(this_value)
		}
		glue::glue("--{this_name} {this_value}")
	}) %>%
		glue::glue_collapse(sep = ' ') %>%
		stringr::str_remove_all("TRUE") %>%
		stringr::str_replace_all('\\s+', ' ')
	return(x)
}

