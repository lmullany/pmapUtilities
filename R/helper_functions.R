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

  available = list(
    "camp" = 'CAMP_PMCoE_Projection',
    "wsp" =  'PatientSafetyQualityWSP_Projection',
    "vte" =  'PatientSafetyQualityVTE_Projection'
    )

  if(!is.null(n)) return(available[[n]])
  else return(available)


}
