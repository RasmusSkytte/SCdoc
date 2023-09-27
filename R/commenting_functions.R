#' Read or add a comment to the database
#'
#' @template conn
#' @template db_table
#' @param column
#'   The name of the table/column to get/set comment on (for table comment, use NA)
#' @param comment
#'   If different from NULL (default) the comment will added to the table/column.
#'   Use a reference "@column" too look up in docs.templates. Everything after the reference gets appended.
#' @param auto_generated
#'   Indicates if comment is auto generated or supplied by the user
#' @param timestamp
#'   Timestamp indicating when the comment is valid from
#' @return
#'   The existing comment on the column/table if no comment is provided. Else NULL
#' @importFrom rlang .data
#' @export
db_comment <- function(conn, db_table, column = NA, comment = NULL, auto_generated = FALSE,
                       timestamp = glue::glue("{today()} 09:00:00")) {

  # Check arguments
  checkmate::assert_class(conn, "PqConnection")
  assert_dbtable_like(db_table)
  checkmate::assert_character(column)
  checkmate::assert_character(comment, null.ok = TRUE)
  checkmate::assert_logical(auto_generated)
  assert_timestamp_like(timestamp)

  # Unpack the db_table specification
  if (is.character(db_table)) {
    db_table_id <- mg::id(db_table)
  } else {
    db_table_id <- dbplyr::remote_name(db_table)
    if (is.null(db_table_id)) {
      stop("The remote_name of db_table could not be determined. ",
           "Try giving as a character string (\"schema.table\") or direct query: tbl(conn, \"schema.table\") instead.")
    }
  }
  db_schema     <- purrr::pluck(db_table_id@name, "schema")
  db_table_name <- purrr::pluck(db_table_id@name, "table")

  # Pull current comments from the DB
  if (is.na(column)) {
    existing_comment <- dplyr::tbl(conn, mg::id("information_schema.tables")) |>
      dplyr::filter(.data$table_schema == db_schema, .data$table_name == db_table_name) |>
      tidyr::unite("db_table", "table_schema", "table_name", sep = ".", remove = FALSE) |>
      dplyr::transmute(schema = .data$table_schema, table = .data$table_name, column_name = NA_character_,
                       comment = dbplyr::sql('obj_description(CAST("db_table" AS "regclass"))'))
  } else {
    existing_comment <- dplyr::tbl(conn, mg::id("information_schema.columns")) |>
      dplyr::filter(.data$table_schema == db_schema, .data$table_name == db_table_name, .data$column_name == column) |>
      tidyr::unite("db_table", "table_schema", "table_name", sep = ".", remove = FALSE) |>
      dplyr::transmute(schema = .data$table_schema, table = .data$table_name, .data$column_name,
                       comment = dbplyr::sql('col_description(CAST("db_table" AS "regclass"), "ordinal_position")'))
  }

  # If comment is missing, pull comment from db
  if (is.null(comment)) {
    return(existing_comment |> dplyr::pull("comment"))

  } else { # Update the comment in the DB

    # Look for un-escaped single quotes
    comment <- stringr::str_replace_all(comment, "(?<!\')\'(?!\')", "\'\'")

    # Check for reference to docs.template
    if (comment %like% "^@") {
      tmp <- stringr::str_split_fixed(comment, " ", n = 2)
      reference <- purrr::pluck(tmp, 1) |> gsub("^@", "", .)
      append    <- purrr::pluck(tmp, 2)

      # Find the reference in docs.templates
      reference_comment <- get_table(conn, "docs.templates") |>
        dplyr::filter(.data$column_name == reference) |>
        dplyr::pull(comment)

      # Check reference exists
      if (length(reference_comment) == 0) stop("Reference: '", reference, "' not found in docs.templates")

      # Update the comment
      comment <- paste(reference_comment, append) |> trimws()
    }

    if (is.na(column)) {
      DBI::dbExecute(conn, glue::glue("COMMENT ON TABLE {db_schema}.{db_table_name} IS '{comment}'"))

    } else {
      DBI::dbExecute(conn, glue::glue("COMMENT ON COLUMN {db_schema}.{db_table_name}.{column} IS '{comment}'"))
    }

    # Push to documentation DB
    target_table <- "docs.documentation"

    # Create the documentation table if it does not exist
    if (!table_exists(conn, target_table)) {
      invisible(
        log <- utils::capture.output(
          suppressWarnings(
            update_snapshot(existing_comment |>
                              dplyr::mutate(auto_generated = auto_generated) |>
                              utils::head(0),
                            conn, target_table, timestamp,
                            log_path = NULL, log_table_id = NULL))))
      failed <- log[!is.na(stringr::str_extract(log, "(WARNING|ERROR)"))]
      if (length(failed) > 0) print(failed)
    }

    # Update the record in the docs database
    invisible(log <- utils::capture.output(
      suppressWarnings(
        update_snapshot(
          existing_comment |> dplyr::mutate(comment = !!comment, auto_generated = !!auto_generated),
          conn, target_table, timestamp,
          filters = existing_comment |> dplyr::select(!"comment"),
          log_path = NULL, log_table_id = NULL,
          enforce_chronological_order = FALSE))))
    failed <- log[!is.na(stringr::str_extract(log, "(WARNING|ERROR)"))]
    if (length(failed) > 0) print(failed)
  }
}



#' Add comments to database table from template
#'
#' @template conn
#' @template db_table
#' @param timestamp
#'   Timestamp indicating when the comment is valid from
#' @return NULL
#' @importFrom rlang .data
#' @export
auto_comment <- function(conn, db_table, timestamp = glue::glue("{today()} 09:00:00")) {

  # Check arguments
  checkmate::assert_class(conn, "PqConnection")
  assert_dbtable_like(db_table)
  assert_timestamp_like(timestamp)

  # Unpack the db_table specification
  if (is.character(db_table)) {
    db_table_id <- mg::id(db_table)
  } else {
    db_table_id <- dbplyr::remote_name(db_table)
    if (is.null(db_table_id)) {
      stop("The remote_name of db_table could not be determined. ",
           "Try giving as a character string (\"schema.table\") or direct query: tbl(conn, \"schema.table\") instead.")
    }
  }
  db_schema     <- purrr::pluck(db_table_id@name, "schema")
  db_table_name <- purrr::pluck(db_table_id@name, "table")

  # Get existing auto-generated comments and missing comments
  elligble_comments <- dplyr::tbl(conn, mg::id("information_schema.columns")) |>
    dplyr::filter(.data$table_schema == db_schema, .data$table_name == db_table_name) |>
    tidyr::unite("db_table", "table_schema", "table_name", sep = ".", remove = FALSE) |>
    dplyr::transmute(db_table = .data$db_table, schema = .data$table_schema, table = .data$table_name, .data$column_name,
                     comment = dbplyr::sql('col_description(CAST("db_table" AS "regclass"), "ordinal_position")')) |>
    mg::left_join(mg::get_table(conn, "docs.documentation"),
                  by = c("schema", "table", "comment"), na_by = "column_name") |>
    dplyr::filter(is.na(comment) | .data$auto_generated | comment == "NA")

  # Determine auto comments from templates
  auto_comments <- elligble_comments |>
    dplyr::select(!"comment") |>
    dplyr::left_join(mg::get_table(conn, "docs.templates"), by = "column_name") |>
    dplyr::filter(!is.na(comment)) |>
    dplyr::select(db_table, column_name, comment)

  # Push comments to DB
  purrr::pwalk(dplyr::collect(auto_comments),
               ~ db_comment(conn, ..1, column = ..2, comment = ..3, auto_generated = TRUE, timestamp = timestamp))
}
