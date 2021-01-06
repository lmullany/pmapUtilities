#' Generate a temp table in the database holding a random set of WRK UNITS
#'
#' This function enables the user to generate a temporary table in the database
#' that will persist for this connection/session, and that can be utilized in
#' subsequent queries, in order to consistently restrict to that subset.  This may
#' be helpful for users to execute an analytic pipeline on a random set of work
#' units rather than the full set.
#'
#' @param size number of unique HOFC_WRK_UNIT_UIDs to return (default 1000)
#' @param filter string expressed as a SQL where clause on MHAODS.WRKUNH. The clause must begin with
#' "WHERE " Examples include:
#'
#'  - "WHERE MHAODS.WRKUNH.PRC_LVL_CD='H'"
#'
#'  - "WHERE MHAODS.WRKUNH.PRC_LVL_CD='H' AND MHAODS.WRKUNH.CRNT_HRG_RQSTD_DT>='2017-01-01'"
#'
#'  - "WHERE PRC_LVL_CD='H' AND CRNT_HRG_RQSTD_DT>='2017-01-01'"
#'
#'  The function will return the dbo.tempname of the temporary table (e.g.
#'  "#dplyr_010"); if the results are desired that \code{tempname} can be used in
#'  return_table (i.e. \code{return_table("dbo","#dplyr_010")}). Furthermore, function
#'  \code{return_table()} has an argument \code{random_wrk_units} which can be set
#'  to this \code{tempname} in order to limit the results of that query to this
#'  random subset (See example below).
#'
#' @param seed integer seed for reproducibility; if no seed is provided, a non-reproducible temp table will be produced
#' @param engine a dbConnect connection object; by default will look in namespace for default_engine
#' @export
#' @return a string name of the temporary table created
#' @examples
#' rnduids <- gen_random_wrk_unit_table(size = 1000, filter="WHERE MHAODS.WRKUNH.PRC_LVL_CD='H'", seed = 123)
#' rnduids <- gen_random_wrk_unit_table(size = 100, seed=87634)
#' rnduids <- gen_random_wrk_unit_table(size = 250000, seed=10, engine=myconn)
#'
#' # Use this handle to the temp table
#' return_table("dbo",rnduids)
#'
#' # Use this handle to restrict to another table (for example, pulling all
#' ARCHWKUT information)
#' return_table("MHAODS","ARCHWKUT",rnd_wrk_units=rnduids)
#'
#'
#'
#'
gen_random_wrk_unit_table <- function(size=1000,filter=NULL,seed=NULL, engine=default_engine) {

  #One option: if the filter is NULL, use the method below
  #get random numbers

  if(is.null(filter)) {

    maxlength <- get_table_dim("MHAODS","WRKUNH",engine=engine)[1]

    #gen random indices table and save name
    random_indices_name <- suppressMessages(
      gen_temp_indices(size = size,
                       max_index = maxlength,
                       seed = seed,engine = engine)
    )

    # generate the query
    qry = paste0("select T.HOFC_WRK_UNIT_UID FROM ",
                 "(SELECT HOFC_WRK_UNIT_UID, ROW_NUMBER() OVER(ORDER by HOFC_WRK_UNIT_UID) AS ID from MHAODS.WRKUNH) AS T ",
                 "INNER JOIN [",random_indices_name$tempname,"] AS S ON S.ID=T.ID")
  } else {
    # 1. create a temp table of the filtered WRKUNH,
    qry = paste0("select HOFC_WRK_UNIT_UID,ROW_NUMBER() OVER(ORDER by HOFC_WRK_UNIT_UID) AS ID ",
                 "INTO #SUBWRK ",
                 "FROM MHAODS.WRKUNH ",filter)

    # 2. find out how many rows this, at the same time as executing it
    tryCatch(DBI::dbRemoveTable(conn=engine,"#SUBWRK"),
             error=function(e) {},
             warning=function(w) {})
    rows_affected <- DBI::dbExecute(conn = engine,qry,immediate=TRUE,)

    # 3. get random indicine within 0 to number of rows in the subquery
    random_indices_name <- suppressMessages(
      gen_temp_indices(size = size,
                       max_index = rows_affected,
                       seed = seed,engine=engine)
      )

    # 4. Create the query to select from #subwrk only these indices
    qry= paste0("SELECT S.HOFC_WRK_UNIT_UID FROM #SUBWRK AS S ",
                "INNER JOIN ",random_indices_name$tempname," AS T ON T.ID=S.ID")

  }

  tname <- suppressMessages(dplyr::compute(query_db(query = qry, engine=engine))[[2]]$x)

  #Now, issue a warning if random_indices is less than requested
  if(random_indices_name$tempsize<size) {
    warning(paste0("Fewer (N=",random_indices_name$tempsize),") UIDs available than requested (N=",size,")",
            call.=F)
  }

  return(tname)

}

#' Generate a set of indices
#'
#' This is a helper function, that produces a random set of distinct integers between
#' 1 and maxlength, inclusive.
#'
#' @param size number of unique HOFC_WRK_UNIT_UIDs to return (default 1000)
#' @return a string name of the temporary table created
#' @export
#' @keywords internal
#' @examples
#' indices = gen_temp_indices(size=100000,max_index=14e10, seed=NULL, engine=mycon)
#' indices = gen_temp_indices(size=1000,max_index=250000, seed=12345, engine=mycon)
#'
gen_temp_indices <- function(size=1000, max_index, seed=NULL, engine=default_engine) {

  # set the seed, if provided
  set.seed(seed)

  # if the size requested exceeds or meets max_index, just set the
  # the indices to be all up to max_index
  if(size>=max_index) {
    ret_size=max_index
    rnd_set <- 1:max_index
  } else {
    # else return a n=size sample of the possible indices, without replacement
    ret_size=size
    rnd_set <- sample(x=1:max_index,size=size,replace=F)
  }


  # push to temporary table (lets_give it a specific name)
  #dplyr::db_drop_table(con=engine,table = "#_temp_rnd_indices",force=TRUE)
  tryCatch(
    DBI::dbRemoveTable(conn=engine,"#_temp_rnd_indices"),
    error = function(e) {}
  )
  temp_rnd_indices <- dplyr::copy_to(dest = engine, data.frame(id=rnd_set),name="_temp_rnd_indices",indexes=list("id"))

  #return the temp table name back to calling function, and the size
  return(list(tempname = as.character(temp_rnd_indices$ops$x),tempsize=ret_size))
}

