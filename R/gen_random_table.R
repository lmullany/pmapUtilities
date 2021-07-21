#' Generate a temp table in the database holding a random set of ids
#'
#' This function enables the user to generate a temporary table in the database
#' that will persist for this connection/session, and that can be utilized in
#' subsequent queries, in order to consistently restrict to that subset.  This may
#' be helpful for users to execute an analytic pipeline on a random set of ids.
#'
#' @param size number of unique ids to return (default 1000)
#' @param filter string expressed as a valid SQL where clause. The clause must
#' begin with 'WHERE ' Examples include:
#'
#'  - "WHERE patients.race='Other'"
#'  - "WHERE osler_id = '123456'"
#'
#'  The function will return the dbo.tempname of the temporary table (e.g.
#'  "#dplyr_010"); if the results are desired that \code{tempname} can be used in
#'  return_table (i.e. \code{return_table(table = "#dplyr_010",schema="dbo")}).
#'
#' @param table string table name to query
#' @param idvars vector of string column names that define uniqueness on the table (i.e. the primary key)
#' @param schema name of schema (default "dbo")
#' @param size number of rows to return (default=1000)
#' @param filter string 'WHERE' clause to further restrict the selection
#' @param seed integer seed for reproducibility; if no seed is provided, a non-reproducible temp table will be produced
#' @param engine a dbConnect connection object; by default will look in namespace for default_engine
#' @export
#' @return a string name of the temporary table created
#' @examples
#' rnduids <- gen_random_table(table= "patient", idvars = "osler_id", size = 1000, seed=123)
#' rnduids <- gen_random_table(table= "patient", idvars = "osler_id", size = 100, seed=87634)
#' rnduids <- gen_random_table(table= "patient", idvars = "osler_id", filter = "WHERE gender = 'Male'", size = 100, seed=87634)
gen_random_table <- function(table,
                                      idvars,
                                      schema="dbo",
                                      size=1000,
                                      filter=NULL,
                                      seed=NULL,
                                      engine=default_engine) {

  if(!table %in% list_tables(engine=engine)$table) {
    stop("table not found, check list_tables()",call. = F)
  }

  aliased_idvars = paste(paste0("S.",idvars), collapse=",")
  unaliased_idvars = paste(idvars,collapse=",")

  random_table_name = gen_random_temp_table_name()


  #One option: if the filter is NULL, use the method below
  #get random numbers

  if(is.null(filter)) {

    maxlength <- get_table_dim(table = table, schema = schema, exact = TRUE, engine=engine)[1]

    #gen random indices table and save name
    random_indices_name <- suppressMessages(
      gen_temp_indices(size = size,
                       max_index = maxlength,
                       seed = seed,engine = engine)
    )

    # generate the query
    qry = paste0("select ", aliased_idvars, " FROM ",
                 "(SELECT ", unaliased_idvars, ", ROW_NUMBER() OVER(ORDER by ", unaliased_idvars,") AS ID from ", schema,".",table," ) AS S ",
                 "INNER JOIN [",random_indices_name$tempname,"] AS T ON T.ID=S.ID")
  } else {
    # 1. create a temp table of the filtered table,
    filtered_tt = gen_random_temp_table_name()
    qry = paste0("select ",unaliased_idvars,",ROW_NUMBER() OVER(ORDER by ",unaliased_idvars,") AS ID ",
                 "INTO ", filtered_tt, " ",
                 "FROM ",schema,".",table, " ",filter)

    # before creating, drop if exists temp table
    tryCatch(DBI::dbRemoveTable(conn=engine,filtered_tt),
             error=function(e) {},
             warning=function(w) {})

    # 2. find out how many rows this, at the same time as executing it
    rows_affected <- DBI::dbExecute(conn = engine,qry,immediate=TRUE,)

    # 3. get random indicine within 0 to number of rows in the subquery
    random_indices_name <- suppressMessages(
      gen_temp_indices(size = size,
                       max_index = rows_affected,
                       seed = seed,engine=engine)
      )

    # 4. Create the query to select from #filtered_tt only these indices
    qry= paste0("SELECT ",aliased_idvars," FROM ",filtered_tt," AS S ",
                "INNER JOIN ",random_indices_name$tempname," AS T ON T.ID=S.ID")

  }


  suppressMessages(
    dplyr::compute(
      query_db(query = qry, engine=engine),
      name=random_table_name
      )
  )

  #Now, issue a warning if random_indices is less than requested
  if(random_indices_name$tempsize<size) {
    warning(paste0("Fewer (N=",random_indices_name$tempsize),") UIDs available than requested (N=",size,")",
            call.=F)
  }

  return(random_table_name)

}

#' Generate a set of indices
#'
#' This is a helper function, that produces a random set of distinct integers between
#' 1 and maxlength, inclusive.
#'
#' @param size number of ids to return (default 1000)
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

  indices_table_name = gen_random_temp_table_name()

  tryCatch(
    DBI::dbRemoveTable(conn=engine,indices_table_name),
    error = function(e) {}
  )
  temp_rnd_indices <- dplyr::copy_to(
    dest = engine,
    data.frame(id=rnd_set),
    name=indices_table_name,
    indexes=list("id"))

  #return the temp table name back to calling function, and the size
  return(list(tempname = indices_table_name,tempsize=ret_size))
}

