# RSedonaType parses CRS with EPSG

    Code
      res$srid()
    Output
      [1] 5070

---

    Code
      res$authority_code()
    Output
      [1] "EPSG:5070"

# RSedonaType parses Engineering CRS (no EPSG ID)

    Code
      res$display()
    Output
      [1] "{...}"

---

    Code
      res$name()
    Output
      [1] "Construction Site Local Grid"

# RSedonaType works with plain strings if that's what's in 'crs'

    Code
      res$srid()
    Output
      [1] 4326

---

    Code
      res$authority_code()
    Output
      [1] "OGC:CRS84"

# RSedonaType handles OGC:CRS84

    Code
      res$to_crs_string()
    Output
      [1] "OGC:CRS84"

---

    Code
      res$authority_code()
    Output
      [1] "OGC:CRS84"

# RSedonaCrs provides RSedonaCrs definition

    Code
      res$definition()
    Output
      [1] "OGC:CRS84"

# RSedonaType$crs_display() uses uppercase authority codes

    Code
      sd_type$crs_display()
    Output
      [1] " (CRS: OGC:CRS84)"

---

    Code
      sd_type5070$crs_display()
    Output
      [1] " (CRS: EPSG:5070)"

# RSedonaCrs$display() uses uppercase authority codes

    Code
      crs$display()
    Output
      [1] "OGC:CRS84"

# RSedonaCrs$to_json() returns quoted string for authority codes

    Code
      json
    Output
      [1] "\"OGC:CRS84\""

# print(sedonadb_dataframe) includes CRS in header

    Code
      print(df)
    Output
      # A sedonadb_dataframe with ? rows and 1 column
      # Geometry column: geom
      # CRS:             OGC:CRS84
      +---------------------+
      |         geom        |
      | geometry(ogc:crs84) |
      +---------------------+
      | POINT(1 2)          |
      +---------------------+
      Preview of up to 6 row(s)

# print(sedonadb_dataframe) handles geography columns

    Code
      print(df)
    Output
      # A sedonadb_dataframe with ? rows and 1 column
      # Geometry column: geog
      # CRS:             OGC:CRS84
      +----------------------+
      |         geog         |
      | geography(ogc:crs84) |
      +----------------------+
      | POINT(1 2)           |
      +----------------------+
      Preview of up to 6 row(s)

# print(sedonadb_dataframe) handles multiple geometry/geography columns

    Code
      print(df)
    Output
      # A sedonadb_dataframe with ? rows and 3 columns
      # Geometry columns: geom_a, geom_b, geog_c
      # CRS (geom_a): OGC:CRS84
      # CRS (geom_b): EPSG:5070
      # CRS (geog_c): OGC:CRS84
      +---------------------+---------------------+----------------------+
      |        geom_a       |        geom_b       |        geog_c        |
      | geometry(ogc:crs84) | geometry(epsg:5070) | geography(ogc:crs84) |
      +---------------------+---------------------+----------------------+
      | POINT(1 2)          | POINT(1 2)          | POINT(1 2)           |
      +---------------------+---------------------+----------------------+
      Preview of up to 6 row(s)

# sd_preview() shows CRS info in Rust headers

    Code
      sd_preview(df, ascii = TRUE)
    Output
      +---------------------+
      |         geom        |
      | geometry(epsg:5070) |
      +---------------------+
      | POINT(1 2)          |
      +---------------------+
      Preview of up to 6 row(s)

