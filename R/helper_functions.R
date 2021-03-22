#Helper Functions

#' Function provides a random temp table name
#' @keywords internal
gen_random_temp_table_name <- function() {
  paste0("#",paste(sample(c(letters,0:9),16, replace=T), collapse=""))
}

#' Function provides a vector of available db names
#' @export
available_database_names <- function() {
  c('CAMP_PMCoE_Projection',
    'PatientSafetyQualityWSP_Projection')
}
