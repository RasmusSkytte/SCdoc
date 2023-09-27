test_that("db_comment() works", {

  if (DBI::dbExistsTable(conn, mg::id("test.mg_package", conn))) DBI::dbRemoveTable(conn, mg::id("test.mg_package", conn))

  # Copy docs.documentation
  target <- get_table(conn, "docs.documentation") |>
    copy_to(conn, ., mg::id("test.mg_package"), overwrite = TRUE, temporary = FALSE)

  # Remove comments from table
  DBI::dbExecute(conn, 'COMMENT ON TABLE "test"."mg_package" IS NULL')
  purrr::walk(tbl(conn, mg::id("test.mg_package")) |> colnames(),
              ~ DBI::dbExecute(conn, paste0('COMMENT ON COLUMN "test"."mg_package"."', ., '" IS NULL')))
  DBI::dbExecute(conn, "DELETE FROM docs.documentation WHERE schema = 'test' AND documentation.table = 'mg_package'")

  comment <- paste(sample(letters, 10), collapse = '')
  db_comment(conn, "test.mg_package", comment = comment)
  expect_equal(db_comment(conn, "test.mg_package"), comment)

  # Get a reference from docs.templates
  reference <- get_table(conn, "docs.templates") |>
    dplyr::count(comment) |>
    dplyr::slice_max(n, with_ties = FALSE) |>
    dplyr::pull(comment)

  reference_comment <- mg::get_table(conn, "docs.templates") |>
    dplyr::filter(column_name == !!stringr::str_extract(reference, "(?<=@).*")) |>
    dplyr::pull(comment)

  db_comment(conn, "test.mg_package", comment = paste(reference, comment))
  expect_equal(db_comment(conn, "test.mg_package"), paste(reference_comment, comment))

  # Clean up
  invisible(DBI::dbExecute(conn, "DROP TABLE test.mg_package"))
  DBI::dbExecute(conn, "DELETE FROM docs.documentation WHERE schema = 'test' AND documentation.table = 'mg_package'")
})


test_that("auto_comment() works", {

  if (DBI::dbExistsTable(conn, id("test.mg_package", conn))) DBI::dbRemoveTable(conn, id("test.mg_package", conn))

  # Copy docs.documentation
  target <- get_table(conn, "prod.covid_19_patientlinelist", include_slice_info = TRUE) |>
    head(0) %>%
    copy_to(conn, ., mg::id("test.mg_package"), overwrite = TRUE, temporary = FALSE)

  # Remove comments from table
  DBI::dbExecute(conn, 'COMMENT ON TABLE "test"."mg_package" IS NULL')
  purrr::walk(tbl(conn, mg::id("test.mg_package")) |> colnames(),
              ~ DBI::dbExecute(conn, paste0('COMMENT ON COLUMN "test"."mg_package"."', ., '" IS NULL')))
  DBI::dbExecute(conn, "DELETE FROM docs.documentation WHERE schema = 'test' AND documentation.table = 'mg_package'")

  auto_comment(conn, "test.mg_package")
  expect_equal(db_comment(conn, "test.mg_package", "cpr"),
               mg::get_table(conn, "docs.templates") |>
                 dplyr::filter(column_name == "cprnr") |>
                 dplyr::pull(comment))
  expect_equal(db_comment(conn, "test.mg_package", "until_ts"),
               mg::get_table(conn, "docs.templates") |>
                 dplyr::filter(column_name == "until_ts") |>
                 dplyr::pull(comment))

  # Clean up
  invisible(DBI::dbExecute(conn, "DROP TABLE test.mg_package"))
  DBI::dbExecute(conn, "DELETE FROM docs.documentation WHERE schema = 'test' AND documentation.table = 'mg_package'")
})
