# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


# Test helper: Wraps a raw JSON CRS string into a nanoarrow schema
# to test `RSedonaType` parsing logic without needing full DataFrame creation.


test_that("RSedonaType parses CRS with EPSG", {
  # Use real data from SedonaDB
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 5070) as geom")
  schema <- nanoarrow::infer_nanoarrow_schema(df)
  sd_type <- RSedonaType$new(schema$children$geom)
  res <- sd_type$crs()

  expect_snapshot(res$srid())
  expect_snapshot(res$authority_code())
})

test_that("RSedonaType parses Engineering CRS (no EPSG ID)", {
  # Manually construct a schema with complex EngineeringCRS JSON
  # This simulates reading from a Parquet file with custom metadata
  meta_json <- '{
    "crs": {
      "type": "EngineeringCRS",
      "name": "Construction Site Local Grid",
      "datum": {
        "type": "EngineeringDatum",
        "name": "Local Datum"
      },
      "coordinate_system": {
        "subtype": "Cartesian",
        "axis": [
          {
            "name": "Northing",
            "direction": "north",
            "unit": "metre"
          },
          {
            "name": "Easting",
            "direction": "east",
            "unit": "metre"
          }
        ]
      }
    }
  }'

  s <- nanoarrow::na_binary()
  s$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = meta_json
  )

  sd_type <- RSedonaType$new(s)
  res <- sd_type$crs()

  # No SRID or Authority code
  expect_null(res$srid())
  expect_null(res$authority_code())
  expect_equal(res$name(), "Construction Site Local Grid")
  expect_true(nchar(res$to_crs_string()) > 0)

  expect_snapshot(res$display())
  expect_snapshot(res$name())
})

test_that("RSedonaType returns no CRS if crs field is missing", {
  s <- nanoarrow::na_binary()
  s$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = "{\"something_else\": 123}"
  )
  expect_null(tryCatch(RSedonaType$new(s)$crs(), error = function(e) NULL))
})

test_that("RSedonaType works with plain strings if that's what's in 'crs'", {
  # ST_SetSRID produces {"crs": "EPSG:4326"} which is this case
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 4326) as geom")
  schema <- nanoarrow::infer_nanoarrow_schema(df)
  sd_type <- RSedonaType$new(schema$children$geom)
  res <- sd_type$crs()

  expect_snapshot(res$srid())
  expect_snapshot(res$authority_code())
})

test_that("RSedonaType handles NULL/Empty input", {
  # Empty string metadata
  s <- nanoarrow::na_binary()
  s$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = ""
  )
  # Should result in no CRS or NULL
  res <- tryCatch(RSedonaType$new(s)$crs(), error = function(e) NULL)
  expect_null(res)
})

test_that("RSedonaType handles OGC:CRS84", {
  # We can force this string via manual construction
  s <- nanoarrow::na_binary()
  s$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = "{\"crs\": \"OGC:CRS84\"}"
  )
  sd_type <- RSedonaType$new(s)
  res <- sd_type$crs()

  expect_snapshot(res$to_crs_string())
  expect_snapshot(res$authority_code())
})


test_that("RSedonaCrs provides RSedonaCrs definition", {
  # Use real data from SedonaDB instead of artificial helper
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 4326) as geom")
  schema <- nanoarrow::infer_nanoarrow_schema(df)
  # Extract CRS from the schema field
  sd_type <- RSedonaType$new(schema$children$geom)
  res <- sd_type$crs()

  # Check PROJ string output
  proj_str <- res$definition()
  expect_true(nchar(proj_str) > 0)

  # Snapshot the exact output (likely "OGC:CRS84" or "EPSG:4326")
  expect_snapshot(res$definition())
})


# Explicit tests for Rust wrappers to ensure uppercase casing

test_that("RSedonaType$crs_display() uses uppercase authority codes", {
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 4326) as geom")
  schema <- nanoarrow::infer_nanoarrow_schema(df)
  sd_type <- RSedonaType$new(schema$children$geom)
  expect_snapshot(sd_type$crs_display())

  df5070 <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 5070) as geom")
  sd_type5070 <- RSedonaType$new(
    nanoarrow::infer_nanoarrow_schema(df5070)$children$geom
  )
  expect_snapshot(sd_type5070$crs_display())
})

test_that("RSedonaCrs$display() uses uppercase authority codes", {
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 4326) as geom")
  sd_type <- RSedonaType$new(
    nanoarrow::infer_nanoarrow_schema(df)$children$geom
  )
  crs <- sd_type$crs()
  expect_snapshot(crs$display())
})

# Test to_json() method

test_that("RSedonaCrs$to_json() returns quoted string for authority codes", {
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 4326) as geom")
  sd_type <- RSedonaType$new(
    nanoarrow::infer_nanoarrow_schema(df)$children$geom
  )
  crs <- sd_type$crs()
  json <- crs$to_json()

  # Should be a quoted JSON string like "\"EPSG:4326\""
  expect_true(nchar(json) > 0)
  expect_snapshot(json)
})

test_that("RSedonaCrs$to_json() returns full JSON for PROJJSON CRS", {
  # Use EngineeringCRS as a PROJJSON example
  meta_json <- '{
    "crs": {
      "type": "EngineeringCRS",
      "name": "Local Grid",
      "datum": {
        "type": "EngineeringDatum",
        "name": "Local Datum"
      },
      "coordinate_system": {
        "subtype": "Cartesian",
        "axis": [
          {
            "name": "X",
            "direction": "east",
            "unit": "metre"
          },
          {
            "name": "Y",
            "direction": "north",
            "unit": "metre"
          }
        ]
      }
    }
  }'

  s <- nanoarrow::na_binary()
  s$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = meta_json
  )

  sd_type <- RSedonaType$new(s)
  crs <- sd_type$crs()
  json <- crs$to_json()

  # Should return full JSON object
  expect_true(grepl("EngineeringCRS", json))
  expect_true(grepl("Local Grid", json))
  # Verify it's valid JSON
  parsed <- jsonlite::fromJSON(json)
  expect_equal(parsed$type, "EngineeringCRS")
})

test_that("RSedonaCrs$to_crs_string() returns proper string for PROJJSON", {
  # For PROJJSON types, to_crs_string() should return the full JSON
  meta_json <- '{
    "crs": {
      "type": "EngineeringCRS",
      "name": "Test CRS"
    }
  }'

  s <- nanoarrow::na_binary()
  s$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = meta_json
  )

  sd_type <- RSedonaType$new(s)
  crs <- sd_type$crs()
  crs_string <- crs$to_crs_string()

  expect_true(nchar(crs_string) > 0)
  expect_true(grepl("EngineeringCRS", crs_string))
})

# Test malformed CRS handling

test_that("RSedonaType handles malformed CRS JSON gracefully", {
  # Create schema with invalid JSON in crs field (not a valid PROJJSON object)
  s <- nanoarrow::na_binary()
  s$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = "{\"crs\": [\"invalid\", \"array\"]}"
  )

  # Should either return NULL or error gracefully
  result <- tryCatch(
    {
      sd_type <- RSedonaType$new(s)
      sd_type$crs()
    },
    error = function(e) NULL
  )

  # We expect this to fail gracefully (NULL or error)
  # The important thing is it doesn't crash
  expect_true(is.null(result) || inherits(result, "error"))
})

test_that("definition() returns authority string for authority-code CRS", {
  # PROJ string for authority codes returns the code itself
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 5070) as geom")
  sd_type <- RSedonaType$new(
    nanoarrow::infer_nanoarrow_schema(df)$children$geom
  )
  crs <- sd_type$crs()
  proj_str <- crs$definition()

  # Returns authority string like "EPSG:5070"
  expect_equal(proj_str, "EPSG:5070")

  # Note: This is NOT OGC WKT format
  # For actual WKT, would need PROJ library integration
})

# CRS preservation through data creation paths

test_that("CRS is preserved when creating from data.frame with geometry", {
  df <- as_sedonadb_dataframe(
    data.frame(
      geom = wk::as_wkb(wk::wkt("POINT (0 1)", crs = "EPSG:32620"))
    )
  )

  re_df <- sd_collect(df)
  crs <- wk::wk_crs(re_df$geom)
  expect_false(is.null(crs))
  # Check that the CRS contains EPSG:32620 info
  expect_true(
    grepl("32620", as.character(crs)) ||
      grepl("32620", jsonlite::toJSON(crs, auto_unbox = TRUE))
  )
})

test_that("CRS is preserved through nanoarrow stream roundtrip", {
  r_df <- data.frame(
    geom = wk::as_wkb(wk::wkt("POINT (0 1)", crs = "EPSG:4326"))
  )

  stream <- nanoarrow::as_nanoarrow_array_stream(r_df)
  df <- as_sedonadb_dataframe(stream, lazy = FALSE)
  re_df <- sd_collect(df)

  crs <- wk::wk_crs(re_df$geom)
  expect_false(is.null(crs))
})

test_that("Different CRS values are preserved independently", {
  # Create geometry with non-default CRS
  df <- sd_sql(
    "
    SELECT
      ST_SetSRID(ST_Point(1, 2), 4326) as geom_wgs84,
      ST_SetSRID(ST_Point(3, 4), 32632) as geom_utm
  "
  )

  re_df <- sd_collect(df)

  # Both geometries should have CRS metadata
  crs1 <- wk::wk_crs(re_df$geom_wgs84)
  crs2 <- wk::wk_crs(re_df$geom_utm)

  expect_false(is.null(crs1))
  expect_false(is.null(crs2))
})

# Parquet roundtrip with CRS

test_that("CRS is preserved through parquet write/read", {
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 4326) as geom")

  tmp_parquet_file <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp_parquet_file))

  sd_write_parquet(df, tmp_parquet_file)
  df_roundtrip <- sd_read_parquet(tmp_parquet_file)
  re_df <- sd_collect(df_roundtrip)

  # Verify geometry has CRS
  crs <- wk::wk_crs(re_df$geom)
  expect_false(is.null(crs))
})

test_that("Non-standard CRS is preserved through parquet roundtrip", {
  # Use a less common SRID (NAD83 / Conus Albers)
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 5070) as geom")

  tmp_parquet_file <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp_parquet_file))

  sd_write_parquet(df, tmp_parquet_file)
  df_roundtrip <- sd_read_parquet(tmp_parquet_file)
  re_df <- sd_collect(df_roundtrip)

  crs <- wk::wk_crs(re_df$geom)
  expect_false(is.null(crs))
  # Check CRS info contains 5070
  crs_str <- jsonlite::toJSON(crs, auto_unbox = TRUE)
  expect_true(grepl("5070", crs_str) || grepl("Albers", crs_str))
})

# Multiple geometry columns with different CRS through operations

test_that("Multiple geometry columns preserve their CRS after operations", {
  df <- sd_sql(
    "
    SELECT
      ST_SetSRID(ST_Point(1, 2), 4326) as point_a,
      ST_SetSRID(ST_Point(3, 4), 5070) as point_b,
      'test' as name
  "
  )

  # Collect and check both CRS are preserved

  re_df <- sd_collect(df)

  crs_a <- wk::wk_crs(re_df$point_a)
  crs_b <- wk::wk_crs(re_df$point_b)

  expect_false(is.null(crs_a))
  expect_false(is.null(crs_b))
  # They should be different
  expect_false(identical(crs_a, crs_b))
})

# Integration tests for full DataFrame printing

test_that("print(sedonadb_dataframe) includes CRS in header", {
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 4326) as geom")
  # Use snapshot to verify the full printed output
  # This verifies both the R-side "# Geometry" line and the Rust-side table headers
  expect_snapshot(print(df))
})

test_that("print(sedonadb_dataframe) handles geography columns", {
  # Manually construct a geography schema since sd_sql may not support
  # 'geography' type yet
  s <- nanoarrow::na_binary()
  s$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = "{\"edges\": \"spherical\", \"crs\": \"OGC:CRS84\"}"
  )

  # Use raw vectors to avoid arrow/wk dependency issues in nanoarrow conversion
  wkb_raw <- unclass(wk::as_wkb("POINT (1 2)"))[[1]]
  df_r <- data.frame(geog = wk::wkb(list(wkb_raw)))
  df <- as_sedonadb_dataframe(df_r, schema = nanoarrow::na_struct(list(geog = s)))

  expect_snapshot(print(df))
})

test_that("print(sedonadb_dataframe) handles multiple geometry/geography columns", {
  # Mix of geometry and geography
  s_geog <- nanoarrow::na_binary()
  s_geog$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = "{\"edges\": \"spherical\", \"crs\": \"OGC:CRS84\"}"
  )

  # Use raw vectors to avoid arrow/wk dependency issues in nanoarrow conversion
  wkb_raw <- unclass(wk::as_wkb("POINT (1 2)"))[[1]]

  df_r <- data.frame(
    geom_a = wk::wkb(list(wkb_raw)),
    geom_b = wk::wkb(list(wkb_raw)),
    geog_c = wk::wkb(list(wkb_raw))
  )

  # Manually specify schemas for all columns to ensure CRS metadata is present
  s_a <- nanoarrow::na_binary()
  s_a$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = "{\"crs\": \"EPSG:4326\"}"
  )
  s_b <- nanoarrow::na_binary()
  s_b$metadata <- list(
    "ARROW:extension:name" = "geoarrow.wkb",
    "ARROW:extension:metadata" = "{\"crs\": \"EPSG:5070\"}"
  )

  df <- as_sedonadb_dataframe(df_r, schema = nanoarrow::na_struct(list(
    geom_a = s_a,
    geom_b = s_b,
    geog_c = s_geog
  )))

  expect_snapshot(print(df))
})

test_that("sd_preview() shows CRS info in Rust headers", {
  df <- sd_sql("SELECT ST_SetSRID(ST_Point(1, 2), 5070) as geom")
  # Capturing output of sd_preview()
  expect_snapshot(sd_preview(df, ascii = TRUE))
})
