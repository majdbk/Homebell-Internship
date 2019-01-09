#' An R Redshift connection
#'
#' This function allows you to create a connection between R and Redshift given an appropriate 
#' keys file. Connection created with the package RPostgres.
#' @param jsonFile File name of the file containing the keys. Defaults to ''.
#' @keywords redshift, rs
#' @return A redshift connection.
#' @export
#' @examples
#' create_rs_connection('~/keys/envs.json')
create_rs_connection <- function(json_file = '') {
  JsonData <- rjson::fromJSON(file= json_file)
  redshift_details <- JsonData$REDSHIFT_DETAILS
  redshift_profiles <- JsonData$REDSHIFT_PROFILES
  rs_conn <- RPostgres::dbConnect(RPostgres::Postgres(),  
                       host = redshift_details$host,
                       port = redshift_details$port,
                       user = redshift_profiles$admin$user,
                       password = redshift_profiles$admin$password,
                       dbname = redshift_details$database,
                       sslmode='require')
  rs_conn
}

#' The result of a redshift query  
#'
#' This function allows you to create a dataframe out of the result of a given redshift query.
#' @param rs_conn An R Redshift connection (as given by the package RPostgres).
#' @param query A Redshift query.
#' @keywords redshift, rs, sql, query, postgres
#' @return A tibble that stores the query results.
#' @export
#' @examples
#' df_from_query(rs_conn, query = "SELECT COUNT(*) FROM dims.opportunities")
df_from_query <- function(rs_conn, query = "SELECT COUNT(*) FROM dims.opportunities") {
  res <- RPostgres::dbSendQuery(rs_conn, query)
  df <- tibble::as_tibble(RPostgres::dbFetch(res))
  RPostgres::dbClearResult(res)
  df
}

#' Close a redshift connection  
#'
#' This function allows you to close a Redshift connection.
#' @param rs_conn An R Redshift connection (as given by the package RPostgres).
#' @keywords redshift, rs, sql, query, postgres
#' @export
#' @examples
#' close_rs_connection(rs_conn)
close_rs_connection <- function(rs_conn) {
  RPostgres::dbDisconnect(rs_conn)
}