#Helper Functions

#' Function provides a random temp table name
#' @keywords internal
gen_random_temp_table_name <- function() {
  set.seed(NULL)
  paste0("#",paste(sample(c(letters,0:9),16, replace=T), collapse=""))
}

#' Function provides a named list of available db names
#'
#' Without providing name `n`, this function will return
#' a named list of data base projections available. Provide
#' argument `n` to return just the full name of the specific
#' projection
#' @param n nickname of the projection to return
#' @export
pmap_dbs <- function(n=NULL) {

  if(!is.null(n)) {
    n = tolower(n)
  }

  available = list(
    "camp" = 'CAMP_PMCoE_Projection',
    "wsp" =  'PatientSafetyQualityWSP_Projection',
    "vte" =  'PatientSafetyQualityVTE_Projection',
    "ma" =  'PatientSafetyQualityMA_Projection'
    )

  if(!is.null(n)) return(available[[n]])
  else return(available)


}

#' Count rows, optionally by group.
#'
#' This is a convenience function to count rows of a resultset, optionally by group
#' @param qry The result set (query/table/view) to return row count for.
#' @param byvars optional string vector of column names
#' @param sort boolean indicating whether to sort or not (default is F)
#' @export
#' @examples
#' count(qry)
#' count(qry, byvars=c("var1", "var2"), sort=T)

count <- function(qry, byvars=NULL, sort=F) {
  if(!is.null(byvars)) {
    dplyr::count(qry, dplyr::across(dplyr::all_of(byvars)),sort=sort)
  } else {
    dplyr::count(qry, sort=sort)
  }
}
