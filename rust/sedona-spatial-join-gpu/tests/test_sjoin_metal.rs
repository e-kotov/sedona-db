// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![cfg(all(target_os = "macos", feature = "metal"))]

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    catalog::MemTable,
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use datafusion_common::Result;
use sedona_common::SedonaOptions;
use sedona_functions;
use sedona_geos;
use sedona_query_planner::{
    optimizer::register_spatial_join_logical_optimizer, query_planner::SedonaQueryPlanner,
};
use sedona_schema::datatypes::WKB_GEOMETRY;
use sedona_spatial_join::DefaultSpatialJoinPhysicalPlanner;
use sedona_spatial_join_gpu::options::GpuOptions;
use sedona_spatial_join_gpu::physical_planner::GpuSpatialJoinPhysicalPlanner;
use sedona_testing::create::create_array;

fn setup_context(
    gpu_enable: bool,
    fallback_to_cpu: bool,
    batch_size: usize,
) -> Result<SessionContext> {
    let mut session_config = SessionConfig::from_env()?
        .with_information_schema(true)
        .with_batch_size(batch_size);
    session_config
        .options_mut()
        .optimizer
        .enable_physical_uncorrelated_scalar_subquery = false;
    session_config = session_config.with_option_extension(SedonaOptions::default());
    let mut gpu_options = GpuOptions::default();
    gpu_options.enable = gpu_enable;
    gpu_options.fallback_to_cpu = fallback_to_cpu;
    session_config = session_config.with_option_extension(gpu_options);

    let mut state_builder = SessionStateBuilder::new();
    state_builder = register_spatial_join_logical_optimizer(state_builder)?;

    // Register planners: Default first, then GPU.
    // Planners are evaluated in reverse order, so GPU is checked first.
    let mut planner = SedonaQueryPlanner::new().with_spatial_join_physical_planner(Arc::new(
        DefaultSpatialJoinPhysicalPlanner::new(),
    ));
    planner = planner.with_spatial_join_physical_planner(Arc::new(
        GpuSpatialJoinPhysicalPlanner::new(),
    ));

    state_builder = state_builder.with_query_planner(Arc::new(planner));
    let state = state_builder.with_config(session_config).build();
    let ctx = SessionContext::new_with_state(state);

    let mut function_set = sedona_functions::register::default_function_set();
    let scalar_kernels = sedona_geos::register::scalar_kernels();

    function_set.scalar_udfs().for_each(|udf| {
        ctx.register_udf(udf.clone().into());
    });

    for (name, kernel) in scalar_kernels.into_iter() {
        let udf = function_set.add_scalar_udf_impl(name, kernel)?;
        ctx.register_udf(udf.clone().into());
    }

    Ok(ctx)
}

fn create_table(items: &[(i32, Option<&str>)], batch_size: usize) -> Result<Arc<MemTable>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        WKB_GEOMETRY.to_storage_field("geometry", true)?,
    ]));

    let mut batches = Vec::new();
    for chunk in items.chunks(batch_size.max(1)) {
        let ids: Vec<i32> = chunk.iter().map(|(id, _)| *id).collect();
        let geoms: Vec<Option<&str>> = chunk.iter().map(|(_, g)| *g).collect();
        let id_array = Arc::new(Int32Array::from(ids));
        let geom_array = Arc::new(create_array(&geoms, &WKB_GEOMETRY));
        batches.push(RecordBatch::try_new(
            schema.clone(),
            vec![id_array, geom_array],
        )?);
    }

    Ok(Arc::new(MemTable::try_new(schema, vec![batches])?))
}

async fn assert_differential_query(
    left_table: Arc<MemTable>,
    right_table: Arc<MemTable>,
    sql: &str,
    batch_size: usize,
) -> Result<()> {
    // 1. Run CPU oracle query
    let ctx_cpu = setup_context(false, true, batch_size)?;
    ctx_cpu.register_table("L", left_table.clone())?;
    ctx_cpu.register_table("R", right_table.clone())?;
    let df_cpu = ctx_cpu.sql(sql).await?;
    let cpu_batches = df_cpu.collect().await?;

    // 2. Run GPU accelerated query
    let ctx_gpu = setup_context(true, true, batch_size)?;
    ctx_gpu.register_table("L", left_table.clone())?;
    ctx_gpu.register_table("R", right_table.clone())?;
    let df_gpu = ctx_gpu.sql(sql).await?;

    let plan = df_gpu.clone().create_physical_plan().await?;
    let plan_display = datafusion_physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        plan_display.contains("SpatialJoinExec"),
        "Physical plan must contain SpatialJoinExec, got:\n{}",
        plan_display
    );

    let gpu_batches = df_gpu.collect().await?;

    let cpu_str = arrow::util::pretty::pretty_format_batches(&cpu_batches)?.to_string();
    let gpu_str = arrow::util::pretty::pretty_format_batches(&gpu_batches)?.to_string();

    assert_eq!(
        cpu_str, gpu_str,
        "Mismatch between CPU oracle and Metal GPU execution!\nQuery: {}\n--- CPU Oracle ---\n{}\n--- Metal GPU ---\n{}",
        sql, cpu_str, gpu_str
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// 1. Predicates: ST_Contains, ST_Covers, ST_Within, ST_CoveredBy, ST_Intersects
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_predicates() -> Result<()> {
    let polygons = vec![
        (0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")),
        (1, Some("POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))")),
        (2, Some("POLYGON ((100 100, 110 100, 110 110, 100 110, 100 100))")),
    ];
    let points = vec![
        (0, Some("POINT (5 5)")),     // inside L0
        (1, Some("POINT (25 25)")),   // inside L1
        (2, Some("POINT (50 50)")),   // disjoint
        (3, Some("POINT (0 5)")),     // on boundary of L0
        (4, Some("POINT (10 10)")),   // on corner of L0
    ];

    let left = create_table(&polygons, 10)?;
    let right = create_table(&points, 10)?;

    // ST_Contains (boundary points excluded)
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Contains(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    // ST_Covers (boundary points included)
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Covers(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    // ST_Within (Point within Polygon)
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT R.id r_id, L.id l_id FROM R JOIN L ON ST_Within(R.geometry, L.geometry) ORDER BY r_id, l_id",
        10,
    ).await?;

    // ST_CoveredBy (Point covered by Polygon)
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT R.id r_id, L.id l_id FROM R JOIN L ON ST_CoveredBy(R.geometry, L.geometry) ORDER BY r_id, l_id",
        10,
    ).await?;

    // ST_Intersects
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// 2. Join Types: Inner, LeftOuter, LeftSemi, LeftAnti
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_join_types() -> Result<()> {
    let polygons = vec![
        (0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")),
        (1, Some("POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))")),
        (2, Some("POLYGON ((100 100, 110 100, 110 110, 100 110, 100 100))")), // unmatched
    ];
    let points = vec![
        (0, Some("POINT (5 5)")),
        (1, Some("POINT (25 25)")),
        (2, Some("POINT (200 200)")), // unmatched
    ];

    let left = create_table(&polygons, 10)?;
    let right = create_table(&points, 10)?;

    // Inner Join
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L INNER JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    // Left Outer Join
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L LEFT JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    // Left Semi Join
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id FROM L LEFT SEMI JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id",
        10,
    ).await?;

    // Left Anti Join
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id FROM L LEFT ANTI JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id",
        10,
    ).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Join Input Orders (Natural vs Swapped)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_join_order_swap() -> Result<()> {
    let polygons = vec![
        (0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")),
        (1, Some("POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))")),
    ];
    let points = vec![
        (0, Some("POINT (5 5)")),
        (1, Some("POINT (25 25)")),
        (2, Some("POINT (50 50)")),
    ];

    let left = create_table(&polygons, 10)?;
    let right = create_table(&points, 10)?;

    // Swapped inputs in SQL with ST_Contains: FROM R JOIN L
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM R JOIN L ON ST_Contains(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    // ST_Within with L on left: FROM L JOIN R ON ST_Within(R.geom, L.geom)
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Within(R.geometry, L.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    // ST_CoveredBy with L on left: FROM L JOIN R ON ST_CoveredBy(R.geom, L.geom)
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_CoveredBy(R.geometry, L.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// 4. Boundary Points (Contains vs Covers divergence & Uncertain -> CPU path)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_boundary_points_contains_vs_covers() -> Result<()> {
    let polygon = vec![
        (0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")),
    ];
    let points = vec![
        (0, Some("POINT (5 5)")),    // interior
        (1, Some("POINT (0 5)")),    // west edge
        (2, Some("POINT (10 10)")),  // NE vertex
        (3, Some("POINT (10 5)")),   // east edge
        (4, Some("POINT (5 0)")),    // south edge
        (5, Some("POINT (5 10)")),   // north edge
        (6, Some("POINT (15 5)")),   // exterior
    ];

    let left = create_table(&polygon, 10)?;
    let right = create_table(&points, 10)?;

    // ST_Contains: boundary points (1..=5) MUST NOT match
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Contains(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    // ST_Covers: boundary points (1..=5) MUST match
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Covers(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// 5. Coordinate Scales: Lon/Lat (~1e1-1e2) and EPSG:3857 (~1e6-1e7)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_coordinate_scales() -> Result<()> {
    // Geographic lon/lat
    let geo_polygons = vec![
        (0, Some("POLYGON ((-74.05 40.70, -73.95 40.70, -73.95 40.80, -74.05 40.80, -74.05 40.70))")),
    ];
    let geo_points = vec![
        (0, Some("POINT (-74.00 40.75)")), // inside
        (1, Some("POINT (-73.90 40.75)")), // outside
        (2, Some("POINT (-74.05 40.75)")), // on edge
    ];
    let geo_left = create_table(&geo_polygons, 10)?;
    let geo_right = create_table(&geo_points, 10)?;

    assert_differential_query(
        geo_left.clone(),
        geo_right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Contains(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;
    assert_differential_query(
        geo_left.clone(),
        geo_right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Covers(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    // Projected EPSG:3857 coordinates (~1e6-1e7)
    let proj_polygons = vec![
        (0, Some("POLYGON ((600000 7000000, 610000 7000000, 610000 7010000, 600000 7010000, 600000 7000000))")),
    ];
    let proj_points = vec![
        (0, Some("POINT (605000 7005000)")), // inside
        (1, Some("POINT (620000 7005000)")), // outside
        (2, Some("POINT (600000 7005000)")), // on boundary
    ];
    let proj_left = create_table(&proj_polygons, 10)?;
    let proj_right = create_table(&proj_points, 10)?;

    assert_differential_query(
        proj_left.clone(),
        proj_right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Contains(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;
    assert_differential_query(
        proj_left.clone(),
        proj_right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Covers(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// 6. Special Geometries: Holes, MultiPolygons, Empty, Null
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_special_geometries() -> Result<()> {
    let complex_geoms = vec![
        // Polygon with interior hole
        (0, Some("POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))")),
        // MultiPolygon
        (1, Some("MULTIPOLYGON (((30 30, 35 30, 35 35, 30 35, 30 30)), ((40 40, 45 40, 45 45, 40 45, 40 40)))")),
        // Empty geometry
        (2, Some("POLYGON EMPTY")),
        // Null geometry
        (3, None),
    ];
    let test_points = vec![
        (0, Some("POINT (2 2)")),     // inside polygon 0
        (1, Some("POINT (10 10)")),   // inside hole of polygon 0 -> outside
        (2, Some("POINT (32 32)")),   // inside part 1 of MultiPolygon 1
        (3, Some("POINT (42 42)")),   // inside part 2 of MultiPolygon 1
        (4, Some("POINT (37 37)")),   // between parts of MultiPolygon 1
        (5, Some("POINT EMPTY")),     // empty point
        (6, None),                    // null point
    ];

    let left = create_table(&complex_geoms, 10)?;
    let right = create_table(&test_points, 10)?;

    // ST_Contains
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Contains(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    // ST_Intersects
    assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// 7. Multiple Probe Batches (exercising P1 batch slicing)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_multiple_probe_batches_slicing() -> Result<()> {
    let polygons = vec![
        (0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")),
        (1, Some("POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))")),
    ];
    let points = vec![
        (0, Some("POINT (5 5)")),
        (1, Some("POINT (25 25)")),
        (2, Some("POINT (2 2)")),
        (3, Some("POINT (22 22)")),
        (4, Some("POINT (50 50)")),
        (5, Some("POINT (60 60)")),
    ];

    // Build side: 1 batch of 2
    let left = create_table(&polygons, 10)?;
    // Probe side: batch size 2 -> 3 batches of 2 rows each
    let right = create_table(&points, 2)?;

    assert_differential_query(
        left,
        right,
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id, r_id",
        2,
    ).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// 8. Planner Fallback: ST_Touches
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_planner_fallback() -> Result<()> {
    let polygons = vec![
        (0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")),
    ];
    let points = vec![
        (0, Some("POINT (0 5)")), // touches edge
        (1, Some("POINT (5 5)")), // inside (does not touch)
    ];

    let left = create_table(&polygons, 10)?;
    let right = create_table(&points, 10)?;

    let sql = "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Touches(L.geometry, R.geometry) ORDER BY l_id, r_id";

    // 1. With fallback_to_cpu = true, query MUST succeed and match CPU oracle
    assert_differential_query(left.clone(), right.clone(), sql, 10).await?;

    // 2. With fallback_to_cpu = false, query planning MUST fail with error
    let ctx_no_fallback = setup_context(true, false, 10)?;
    ctx_no_fallback.register_table("L", left)?;
    ctx_no_fallback.register_table("R", right)?;

    let df = ctx_no_fallback.sql(sql).await?;
    let err = df.create_physical_plan().await.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("not supported on GPU"),
        "Expected unsupported predicate error, got: {}",
        err_msg
    );

    Ok(())
}
