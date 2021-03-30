

#' Generate a dbConnect object to the SQL database
#'
#' This function generates a connection object. Note that most of the functions in pmapUtilities
#' package require the specification of connection engine in the `engine` parameter of the function.
#' However, all functions in the package requiring a connection will look for `default_engine` in the
#' global environment, thus it is convenient (and recommended) to assign the return value of this
#' to a variable called `default_engine`.
#' @param dbname string name of database to connect to
#' @param username username for the database connection
#' @param server defaults to 'ESMPMDBPR4.WIN.AD.JHU.EDU', but another string server could be provided
#' @param driver defaults to 'FreeTDS'
#' @param tdsver defaults to "8.0"
#' @param verbose (logical, default=FALSE; if TRUE will provide some details of the connection)
#' @param trusted (logical, default=FALSE); if set to TRUE, will attempt a trusted connection using
#' ODBC Driver 17 for SQL SERVER and Trusted_Connection="yes"
#' @return Database connection object as returned by `DBI::dbConnect()`
#' @export
#' @examples
#' default_engine = get_sql_connection(dbname = 'CAMP_PMCoE_Projection', username='<jhedid>')

get_sql_connection <- function(dbname,
                               username,
                               server='ESMPMDBPR4.WIN.AD.JHU.EDU',
                               driver='FreeTDS',
                               tdsver="8.0",
                               verbose=F,
                               trusted=F) {

  port = 1433

  if(trusted) {
    #try microsoft integration
    con <- DBI::dbConnect(odbc::odbc(),
                 driver = "ODBC Driver 17 for SQL Server",
                 server = server,
                 database = dbname,
                 Trusted_Connection="yes",
                 port = 1433)
  } else {
    user = paste0("win\\",username)
    con<-DBI::dbConnect(odbc::odbc(),
                        port=port,
                        driver=driver,
                        server=server,
                        database=dbname,
                        uid=user,
                        pwd=getPass::getPass(paste0("Enter Password for ", username, ": ")),
                        TDS_version=tdsver)
  }

  if(verbose) print(con)
  cat("Note: name/rename your connection as 'default_engine' to avoid\n",
      "specifying an engine in subsequent pmap.utilities:: functions\n")
  return(invisible(con))
}



#' Query the database with user-defined query string
#'
#' This function allows a query on the database, using a string. Is equivalent to tbl(con,sql(query)) or dbGetQuery(con,query)
#' @param query a user-defined query string
#' @param engine a dbConnect connection object
#' @export
#' @return a (lazy) dplyr::tbl / data frame object with results
#' @examples
#' query_db("SELECT COUNT(*) AS CT FROM MYSCHEMA.MYTABLE", myconnection)

query_db <- function(query, engine = default_engine) {
  df <- dplyr::tbl(src=engine, dplyr::sql(query))
  return(df)
}

#' Query the database for a specified table, with optional sub-columns/where filter
#'
#' This function provides a way to query the database for a single table, which might
#' be filtered using a where clause, or sub-setting by specified columns/number of rows
#' @param table database table name, string
#' @param schema database schema name, string; default is "dbo"
#' @param columns character vector of columns; default is NULL, which returns all columns
#' @param max_rows maximum (integer) number of rows to return; default is NULL
#' (all rows), currently returns TOP max_rows from table
#' @param filter_condition string representing 'WHERE' clause of query; default
#' is NULL (no filter).
#' @param engine a dbConnect connection object
#' @export
#' @return a (lazy) dplyr::tbl / data frame object with results
#' @examples
#' return_table(table="encounters", engine = myconnection)
#' return_table(table="encounters", columns = c("osler_id"), max_rows=10000, engine = myconnection)
#' return_table(table="encounters", max_rows=10000, filter="encounter_type='Appointment'", engine = myconnection)

return_table <- function(table,schema="dbo",columns = NULL, max_rows=NULL,
                         filter_condition = NULL,
                         engine = default_engine) {

  if(!is.null(columns)) {
    columns = paste0("[",columns,"]")
  }
  query = construct_table_query(schema = schema,
                                table=table,
                                columns = columns,
                                max_rows = max_rows,
                                filter_condition = filter_condition)
  df = query_db(query,engine)
  return(df)
}

#' Construct a query, given schema, table, columns, filter condition
#'
#' Internal function to construct query given user arguments
#' @param schema database schema name, string;default="dbo"
#' @param table database table name, string
#' @param columns character vector of columns; default is NULL, which returns all columns
#' @param max_rows maximum number of rows to return; default is NULL (all rows), currently returns TOP max_rows from table
#' @param filter_condition string representing 'WHERE' clause of query; default is NULL (no filter)
#' @keywords internal
#' @return query string
#' @examples
#' construct_table_query(myschema, mytable, col_list, 1000, my_filter)
construct_table_query <- function(schema="dbo", table, columns=NULL, max_rows=NULL,filter_condition=NULL) {
  max_row_spec = ifelse(is.null(max_rows),"",paste0("TOP ",as.integer(max_rows)))

  if(is.null(columns)) {
    query = sprintf("SELECT %s * FROM [%s].[%s] ", max_row_spec, schema, table)
  } else {
    query = sprintf("SELECT %s %s FROM [%s].[%s] ", max_row_spec,paste(columns, collapse=", "), schema, table)
  }
  if(!is.null(filter_condition)) {
    query = paste0(query, " WHERE ", filter_condition)
  }
  return(query)
}

#' List schemas in the database
#'
#' Function returns a character vector of all schemas in the database
#' where PRINCIPAL_ID = 1 and where NAME is not 'dbo'; further, if a found
#' schema has no tables, that schema name is excluded
#' @param engine a dbConnect connection object; by default will look in namespace for default_engine
#' @export
#' @return character vector of schema names
#' @examples
#' list_schemas(myconnection)

list_schemas <- function(engine = default_engine) {

  # First, get all the schemas
  schemas <- query_db(query = "SELECT * FROM SYS.schemas WHERE PRINCIPAL_ID=1 AND NAME<>'dbo'", engine=engine) %>%
    dplyr::collect() %>%
    dplyr::pull(name)

  if(length(schemas)==0) {
    cat("No non-dbo schemas found.\n")
    invisible(NULL)
  } else {
    # If any found, we are going to return only those that have tables
    res <- sapply(schemas, function(x) length(list_tables(schema=x,engine=engine))>0)
    return(names(res)[res])
  }


}


#' List tables in a given schema
#'
#' Function returns a character vector of all table names found in a given schema
#' @param schema a string name of schema to search, default="dbo"
#' @param show_dimensions logical (default FALSE); set this to TRUE to additionally return the size of the
#' tables (number of rows and columns)
#' @param exact logical (default FALSE); only relevant for show dimensions; if this is FALSE, then
#' table dimensions will be estimated using sys.dm_db_partition_stats table, for which permission may not
#' be provided to the user. In that case, this parameter can be set to TRUE to get exact dimensions,
#' which will be slower (sometimes substantially so, if many tables, and those tables are large), but will
#' be more accurate.
#' @param engine a dbConnect connection object; by default will look in namespace for default_engine
#' @export
#' @import data.table
#' @return data.table of with column of table names, and optionally, columns for number of rows and columns
#' @examples
#' list_tables(engine=myconnection)
#' list_tables(schema="dbo",show_dimensions=TRUE, engine=myconnection)
list_tables <- function(schema="dbo", show_dimensions=FALSE, exact=FALSE, engine = default_engine) {
  table_names = query_db(query = paste0("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                                        WHERE table_schema='",schema,"' AND TABLE_TYPE = 'BASE TABLE'"),
                         engine = engine) %>%
    dplyr::collect()
  if(nrow(table_names)==0) return(NULL)

  table_names = dplyr::pull(table_names, TABLE_NAME)

  if(!show_dimensions) {
    return(data.table::data.table("table" =  table_names))
  } else {
    tdetail = data.table::rbindlist(
      lapply(table_names, function(x) {
        dims = get_table_dim(x,engine=engine,exact=exact)
        data.table::data.table("table"=x,"rows"=dims[1], "cols"=dims[2])
    }))
    return(tdetail)

  }

}

#' List views in a given schema
#'
#' Function returns a character vector of all view names found in a given schema
#' @param schema a string name of schema to search, default="dbo"
#' @param engine a dbConnect connection object; by default will look in namespace for default_engine
#' @export
#' @return character vector of view names
#' @examples
#' list_views(engine = myconnection)
#'
list_views <- function(schema="dbo", engine = default_engine) {
  view_names = query_db(query = paste0("SELECT TABLE_NAME as VIEW_NAME FROM INFORMATION_SCHEMA.TABLES
                                       WHERE TABLE_SCHEMA='", schema,"' AND TABLE_TYPE='VIEW'"),
                        engine = engine) %>%
    dplyr::collect()
  if(nrow(view_names)==0) return(NULL)
  else return(dplyr::pull(view_names, VIEW_NAME))
}

#' Get a view definition
#'
#' Function returns the view definition for a view name in a given schema
#' @param view a string name of the view for which the definition should be returned
#' @param schema a string name of schema to search;default is dbo
#' @param engine a dbConnect connection object; by default will look in namespace for default_engine
#' @export
#' @return string of view definition
#' @examples
#' get_view_definition("myview", schema="dbo",engine=myconnection)
#'
get_view_definition <- function(view, schema="dbo",engine = default_engine) {

  view_definition = query_db(query = paste0("SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS
                                       WHERE TABLE_SCHEMA='", schema,"' AND TABLE_NAME='", view,"'"),
                        engine = engine) %>%
    dplyr::collect()

  if(nrow(view_definition)==0) return(NULL)
  else return(dplyr::pull(view_definition, VIEW_DEFINITION))

}

#' List all column names for a given schema, table
#'
#' Function returns a character vector of all column names in a table
#' @param table a string name of table to search
#' @param schema a string name of schema to search; default is dbo
#' @param engine a dbConnect connection object; by default will look in namespace for default_engine
#' @export
#' @return character vector of column names
#' @examples
#' list_columns('medications',schema="dbo", engine=myconnection)
#'

list_columns <- function(table, schema="dbo",engine = default_engine) {
  column_names <- colnames(return_table(schema=schema,table=table,max_rows = 0,engine=engine))
  return(column_names)
}


#' Get the dimensions (rows and columns) of a given table in a schema
#'
#' Function returns a named vector of rows (number of rows) and cols (number of cols). This query
#' uses the sys.dm_db_partition_stats table (if permission); in a live database with inserts/deletions, etc this
#' will not be exact; if an exact count is required, or permission to access sys.dm_db_partition_stats table
#' is not granted, the `exact` parameter can be set to TRUE
#' @param table a string name of table to search
#' @param schema a string name of schema to search;default="dbo"
#' @param engine a dbConnect connection object; by default will look in namespace for default_engine
#' @param exact logical, default = FALSE; set to TRUE to get exact row count
#' @export
#' @return a vector of rows and cols
#' @examples
#' get_table_dim(problemlist',schema="dbo", engine=myconnection)
#' get_table_dim(problemlist',schema="dbo", engine=myconnection, exact=TRUE)

get_table_dim <- function(table,schema="dbo", engine=default_engine, exact=FALSE) {
  if(!exact) {
    query = paste0("SELECT nrows = sum(row_count) FROM sys.dm_db_partition_stats WHERE index_id<2 ",
                 "and OBJECT_SCHEMA_NAME(object_id)='",schema,"' ",
                 "and object_name(object_id)='", table,"'")
  } else {
    query = sprintf("SELECT nrows = COUNT(1) FROM [%s].[%s] ", schema, table)
  }

  rows=try(as.double(query_db(query, engine) %>%
                   dplyr::collect() %>%
                   dplyr::pull(nrows)), silent=T)
  if(inherits(rows,"try-error")) {
    stop("Error: Dimension Retrieval Failed; perhaps permission not granted; try setting exact to TRUE",
         call. = F)
  }

  cols=length(list_columns(table=table, schema=schema,engine=engine))
  return(c('rows'=rows,'cols'=cols))
}


#' Get the primary or foreign keys on a table (if any)
#'
#' Function returns a character vector of column name(s) that are the primary or foreign key(s) (see keytype),
#' given a schema, and table name
#' @param table a string name of table to search
#' @param schema a string name of schema to search;default="dbo"
#' @param keytype one of either 'PRIMARY' (default) or 'FOREIGN'
#' @param engine a dbConnect connection object; by default will look in namespace for default_engine
#' @export
#' @return a character vector of column(s) making up the primary key or foreign key(s) on the table
#' @examples
#' get_keys('encounters', schema="dbo", engine = myconnection)
#' get_keys('encounters', schema="dbo", keytype='FOREIGN', engine=myconnection)
#'
get_keys <- function(table, schema="dbo", keytype=c("PRIMARY","FOREIGN"), engine=default_engine) {
  #Get the key type
  keytype = match.arg(keytype)
  #Construct the query
  kquery <- paste0("SELECT Col.Column_Name from INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab,",
                   " INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col",
                   " WHERE Col.Constraint_Name = Tab.Constraint_Name",
                   " AND Col.Table_Name = Tab.Table_Name ",
                   " AND Constraint_Type = '", keytype, " KEY'",
                   " AND Col.Table_Schema = '",schema,"'",
                   " AND Col.Table_Name = '",table,"'")
  #Return the query result
  keys = query_db(kquery, engine) %>%
    dplyr::collect() %>%
    dplyr::pull(Column_Name)
  return(keys)
}
