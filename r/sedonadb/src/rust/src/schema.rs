
use std::sync::Arc;
use savvy::savvy;
use sedona_schema::crs::CoordinateReferenceSystem;

/// R-exposed wrapper for CRS (Coordinate Reference System) introspection
///
/// This wraps an Arc<dyn CoordinateReferenceSystem> and exposes its methods to R.
#[savvy]
pub struct RSedonaCrs {
    inner: Arc<dyn CoordinateReferenceSystem + Send + Sync>,
}

#[savvy]
impl RSedonaCrs {
    /// Get the SRID (e.g., 4326 for WGS84) or NULL if not an EPSG code
    fn srid(&self) -> savvy::Result<savvy::Sexp> {
        match self.inner.srid() {
            Ok(Some(srid)) => savvy::Sexp::try_from(srid as i32),
            Ok(None) => Ok(savvy::NullSexp.into()),
            Err(e) => Err(savvy::Error::new(format!("Failed to get SRID: {e}"))),
        }
    }

    /// Get the authority code (e.g., "EPSG:4326") or NULL if not available
    fn authority_code(&self) -> savvy::Result<savvy::Sexp> {
        match self.inner.to_authority_code() {
            Ok(Some(code)) => savvy::Sexp::try_from(code.as_str()),
            Ok(None) => Ok(savvy::NullSexp.into()),
            Err(e) => Err(savvy::Error::new(format!(
                "Failed to get authority code: {e}"
            ))),
        }
    }

    /// Get the JSON representation of the CRS
    fn to_json(&self) -> savvy::Result<savvy::Sexp> {
        savvy::Sexp::try_from(self.inner.to_json().as_str())
    }

    /// Get the PROJ-compatible CRS string representation
    fn to_crs_string(&self) -> savvy::Result<savvy::Sexp> {
        savvy::Sexp::try_from(self.inner.to_crs_string().as_str())
    }

    /// Get a formatted display string (e.g., "EPSG:4326" or "{...}")
    fn display(&self) -> savvy::Result<savvy::Sexp> {
        let display = if let Ok(Some(auth)) = self.inner.to_authority_code() {
            auth
        } else {
            format!("{}", self.inner.as_ref())
        };
        savvy::Sexp::try_from(display.as_str())
    }
}

/// R-exposed wrapper for SedonaType introspection
///
/// This allows R code to inspect Arrow schema fields and determine
/// if they are geometry types with CRS information.
#[savvy]
pub struct RSedonaType {
    inner: sedona_schema::datatypes::SedonaType,
    name: String,
}

#[savvy]
impl RSedonaType {
    /// Create a RSedonaType from a nanoarrow schema (external pointer)
    ///
    /// The schema should be a single field (column) schema, not a struct schema.
    fn new(schema_xptr: savvy::Sexp) -> savvy::Result<RSedonaType> {
        use sedona_schema::datatypes::SedonaType;

        let field = crate::ffi::import_field(schema_xptr)?;
        let name = field.name().clone();

        // Use existing SedonaType infrastructure to parse the field
        let inner = SedonaType::from_storage_field(&field)
            .map_err(|e| savvy::Error::new(format!("Failed to create SedonaType: {e}")))?;

        Ok(RSedonaType { inner, name })
    }

    /// Get the logical type name ("geometry", "geography", "utf8", etc.)
    fn logical_type_name(&self) -> savvy::Result<savvy::Sexp> {
        savvy::Sexp::try_from(self.inner.logical_type_name().as_str())
    }

    /// Get the column name
    fn name(&self) -> savvy::Result<savvy::Sexp> {
        savvy::Sexp::try_from(self.name.as_str())
    }

    /// Get the CRS wrapper object, or NULL if no CRS is present
    ///
    /// This returns a RSedonaCrs object that can be used to inspect the CRS.
    fn crs(&self) -> savvy::Result<RSedonaCrs> {
        use sedona_schema::datatypes::SedonaType;

        match &self.inner {
            SedonaType::Wkb(_, crs) | SedonaType::WkbView(_, crs) => {
                if let Some(crs_arc) = crs {
                    Ok(RSedonaCrs {
                        inner: crs_arc.clone(),
                    })
                } else {
                    Err(savvy::Error::new("No CRS available for this geometry type"))
                }
            }
            _ => Err(savvy::Error::new("No CRS available for non-geometry types")),
        }
    }

    /// Get a formatted CRS display string like " (CRS: EPSG:4326)" or empty string
    fn crs_display(&self) -> savvy::Result<savvy::Sexp> {
        use sedona_schema::datatypes::SedonaType;

        match &self.inner {
            SedonaType::Wkb(_, crs) | SedonaType::WkbView(_, crs) => {
                if let Some(crs_ref) = crs {
                    // Try to get authority code first (usually EPSG:XXXX)
                    let auth = crs_ref.to_authority_code().ok().flatten();
                    let display = if let Some(auth) = auth {
                        format!(" (CRS: {})", auth)
                    } else {
                        // Fallback to the Display impl which might be lowercase or PROJJSON
                        format!(" (CRS: {})", crs_ref)
                    };
                    savvy::Sexp::try_from(display.as_str())
                } else {
                    savvy::Sexp::try_from("")
                }
            }
            _ => savvy::Sexp::try_from(""),
        }
    }
}
