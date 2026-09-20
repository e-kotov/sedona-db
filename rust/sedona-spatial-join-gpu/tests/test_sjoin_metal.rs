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
use sedona_query_planner::{
    optimizer::register_spatial_join_logical_optimizer, query_planner::SedonaQueryPlanner,
};
use sedona_schema::datatypes::WKB_GEOMETRY;
use sedona_spatial_join::DefaultSpatialJoinPhysicalPlanner;
use sedona_spatial_join_gpu::options::GpuOptions;
use sedona_spatial_join_gpu::physical_planner::GpuSpatialJoinPhysicalPlanner;
use sedona_testing::create::create_array;

use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::ExecutionPlan;

fn find_spatial_join_node(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == "SpatialJoinExec" {
        return Some(plan.clone());
    }
    for child in plan.children() {
        if let Some(node) = find_spatial_join_node(child) {
            return Some(node);
        }
    }
    None
}

fn find_spatial_join_metrics(plan: &Arc<dyn ExecutionPlan>) -> Option<MetricsSet> {
    find_spatial_join_node(plan).and_then(|p| p.metrics())
}

fn get_metric_count(metrics: &MetricsSet, name: &str) -> usize {
    metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0)
}

fn setup_context(
    gpu_enable: bool,
    fallback_to_cpu: bool,
    batch_size: usize,
) -> Result<SessionContext> {
    setup_context_with_builder(
        SessionStateBuilder::new(),
        gpu_enable,
        fallback_to_cpu,
        batch_size,
    )
}

fn setup_context_with_builder(
    mut state_builder: SessionStateBuilder,
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

    state_builder = register_spatial_join_logical_optimizer(state_builder)?;

    // Register planners: Default first, then GPU.
    // Planners are evaluated in reverse order, so GPU is checked first.
    let mut planner = SedonaQueryPlanner::new()
        .with_spatial_join_physical_planner(Arc::new(DefaultSpatialJoinPhysicalPlanner::new()));
    planner =
        planner.with_spatial_join_physical_planner(Arc::new(GpuSpatialJoinPhysicalPlanner::new()));

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

async fn assert_differential_query_opts(
    left_table: Arc<MemTable>,
    right_table: Arc<MemTable>,
    sql: &str,
    batch_size: usize,
    fallback_to_cpu: bool,
    expect_gpu_provider: bool,
) -> Result<(MetricsSet, Arc<dyn ExecutionPlan>)> {
    // 1. Run CPU oracle query
    let ctx_cpu = setup_context(false, true, batch_size)?;
    ctx_cpu.register_table("L", left_table.clone())?;
    ctx_cpu.register_table("R", right_table.clone())?;
    let df_cpu = ctx_cpu.sql(sql).await?;
    let cpu_batches = df_cpu.collect().await?;

    // 2. Run GPU accelerated query
    let ctx_gpu = setup_context(true, fallback_to_cpu, batch_size)?;
    ctx_gpu.register_table("L", left_table.clone())?;
    ctx_gpu.register_table("R", right_table.clone())?;
    let df_gpu = ctx_gpu.sql(sql).await?;

    let plan = df_gpu.create_physical_plan().await?;
    let plan_display = datafusion_physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        plan_display.contains("SpatialJoinExec"),
        "Physical plan must contain SpatialJoinExec, got:\n{}",
        plan_display
    );
    if expect_gpu_provider {
        assert!(
            plan_display.contains("provider=Gpu"),
            "Physical plan must use Gpu provider, got:\n{}",
            plan_display
        );
    }

    let gpu_batches = datafusion_physical_plan::collect(plan.clone(), ctx_gpu.task_ctx()).await?;

    let cpu_str = arrow::util::pretty::pretty_format_batches(&cpu_batches)?.to_string();
    let gpu_str = arrow::util::pretty::pretty_format_batches(&gpu_batches)?.to_string();

    assert_eq!(
        cpu_str, gpu_str,
        "Mismatch between CPU oracle and Metal GPU execution!\nQuery: {}\n--- CPU Oracle ---\n{}\n--- Metal GPU ---\n{}",
        sql, cpu_str, gpu_str
    );

    let metrics = find_spatial_join_metrics(&plan).ok_or_else(|| {
        datafusion_common::DataFusionError::Plan("No SpatialJoinExec metrics found".into())
    })?;

    Ok((metrics, plan))
}

async fn assert_differential_query(
    left_table: Arc<MemTable>,
    right_table: Arc<MemTable>,
    sql: &str,
    batch_size: usize,
) -> Result<(MetricsSet, Arc<dyn ExecutionPlan>)> {
    assert_differential_query_opts(left_table, right_table, sql, batch_size, false, true).await
}

// ---------------------------------------------------------------------------
// 1. Predicates: ST_Contains, ST_Covers, ST_Within, ST_CoveredBy, ST_Intersects
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_predicates() -> Result<()> {
    let polygons = vec![
        (0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")),
        (1, Some("POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))")),
        (
            2,
            Some("POLYGON ((100 100, 110 100, 110 110, 100 110, 100 100))"),
        ),
    ];
    let points = vec![
        (0, Some("POINT (5 5)")),   // inside L0
        (1, Some("POINT (25 25)")), // inside L1
        (2, Some("POINT (50 50)")), // disjoint
        (3, Some("POINT (0 5)")),   // on boundary of L0
        (4, Some("POINT (10 10)")), // on corner of L0
    ];

    let left = create_table(&polygons, 10)?;
    let right = create_table(&points, 10)?;

    // ST_Contains (boundary points excluded)
    let (metrics_contains, _) = assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Contains(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;
    assert!(
        get_metric_count(&metrics_contains, "gpu_verified") > 0,
        "Expected ST_Contains to verify candidate points directly on Metal GPU"
    );

    // ST_Covers (boundary points included)
    let (metrics_covers, _) = assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Covers(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;
    assert!(
        get_metric_count(&metrics_covers, "gpu_verified") > 0,
        "Expected ST_Covers to verify candidate points directly on Metal GPU"
    );

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
        (
            2,
            Some("POLYGON ((100 100, 110 100, 110 110, 100 110, 100 100))"),
        ), // unmatched
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
    let (metrics_within, plan_within) = assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Within(R.geometry, L.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;
    assert!(
        get_metric_count(&metrics_within, "gpu_verified") > 0,
        "ST_Within must swap inputs to place polygons on build side and verify on GPU"
    );
    let join_node = find_spatial_join_node(&plan_within).expect("SpatialJoinExec must be present");
    let build_child = join_node.children()[0];
    let build_schema = build_child.schema();
    assert!(
        build_schema.fields().iter().any(|f| f.name() == "geometry"),
        "Build child must contain geometry field from polygon table"
    );

    // ST_CoveredBy with L on left: FROM L JOIN R ON ST_CoveredBy(R.geom, L.geom)
    let (metrics_covered_by, _) = assert_differential_query(
        left.clone(),
        right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_CoveredBy(R.geometry, L.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;
    assert!(
        get_metric_count(&metrics_covered_by, "gpu_verified") > 0,
        "ST_CoveredBy must swap inputs to place polygons on build side and verify on GPU"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// 4. Boundary Points (Contains vs Covers divergence & Uncertain -> CPU path)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_boundary_points_contains_vs_covers() -> Result<()> {
    let polygon = vec![(0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"))];
    let points = vec![
        (0, Some("POINT (5 5)")),   // interior
        (1, Some("POINT (0 5)")),   // west edge
        (2, Some("POINT (10 10)")), // NE vertex
        (3, Some("POINT (10 5)")),  // east edge
        (4, Some("POINT (5 0)")),   // south edge
        (5, Some("POINT (5 10)")),  // north edge
        (6, Some("POINT (15 5)")),  // exterior
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
    let geo_polygons = vec![(
        0,
        Some("POLYGON ((-74.05 40.70, -73.95 40.70, -73.95 40.80, -74.05 40.80, -74.05 40.70))"),
    )];
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
    assert_differential_query(
        geo_left.clone(),
        geo_right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Touches(L.geometry, R.geometry) ORDER BY l_id, r_id",
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
    assert_differential_query(
        proj_left.clone(),
        proj_right.clone(),
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Touches(L.geometry, R.geometry) ORDER BY l_id, r_id",
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
        (0, Some("POINT (2 2)")),   // inside polygon 0
        (1, Some("POINT (10 10)")), // inside hole of polygon 0 -> outside
        (2, Some("POINT (32 32)")), // inside part 1 of MultiPolygon 1
        (3, Some("POINT (42 42)")), // inside part 2 of MultiPolygon 1
        (4, Some("POINT (37 37)")), // between parts of MultiPolygon 1
        (5, Some("POINT EMPTY")),   // empty point
        (6, None),                  // null point
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
// 8. Planner Fallback: ST_Crosses (unsupported predicate falls back to CPU)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_planner_fallback() -> Result<()> {
    let polygons = vec![(0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"))];
    let lines = vec![
        (0, Some("LINESTRING (-5 5, 15 5)")), // crosses polygon
    ];

    let left = create_table(&polygons, 10)?;
    let right = create_table(&lines, 10)?;

    let sql = "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Crosses(L.geometry, R.geometry) ORDER BY l_id, r_id";

    // 1. With fallback_to_cpu = true, query MUST succeed and match CPU oracle
    assert_differential_query_opts(left.clone(), right.clone(), sql, 10, true, false).await?;

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

// ---------------------------------------------------------------------------
// 9. ST_Touches: Point vs Polygon edge, vertex, hole, interior, exterior, 1-ulp, MultiPolygon
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_touches_point_vs_polygon() -> Result<()> {
    // Polygon with hole: outer [0, 20] x [0, 20], hole [5, 15] x [5, 15]
    // MultiPolygon: part1 [30, 40] x [0, 10], part2 [40, 50] x [0, 10] sharing edge x=40
    let polygons = vec![
        (
            0,
            Some("POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))"),
        ),
        (
            1,
            Some(
                "MULTIPOLYGON (((30 0, 40 0, 40 10, 30 10, 30 0)), ((40 0, 50 0, 50 10, 40 10, 40 0)))",
            ),
        ),
    ];

    let ulp_down = 20.0f64.next_down();
    let ulp_up = 20.0f64.next_up();
    let pt_ulp_down = format!("POINT ({:.16} 10)", ulp_down);
    let pt_ulp_up = format!("POINT ({:.16} 10)", ulp_up);

    let points = vec![
        (0, Some("POINT (0 10)")),       // on outer edge of poly 0 -> Touches
        (1, Some("POINT (0 0)")),        // on outer vertex of poly 0 -> Touches
        (2, Some("POINT (5 10)")),       // on hole boundary of poly 0 -> Touches
        (3, Some("POINT (15 15)")),      // on hole vertex of poly 0 -> Touches
        (4, Some("POINT (2 2)")),        // strictly interior -> Does NOT touch
        (5, Some("POINT (10 10)")),      // in hole (exterior) -> Does NOT touch
        (6, Some("POINT (25 25)")),      // strictly exterior -> Does NOT touch
        (7, Some(pt_ulp_down.as_str())), // 1 ulp inside outer edge x=20 -> interior, Does NOT touch
        (8, Some(pt_ulp_up.as_str())), // 1 ulp outside outer edge x=20 -> exterior, Does NOT touch
        (9, Some("POINT (40 5)")),     // on shared boundary of MultiPolygon 1 -> Touches
        (10, Some("POINT (35 5)")),    // interior of part 1 -> Does NOT touch
    ];

    let left = create_table(&polygons, 10)?;
    let right = create_table(&points, 10)?;

    let (metrics, _) = assert_differential_query(
        left,
        right,
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Touches(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    assert_eq!(
        get_metric_count(&metrics, "gpu_verified"),
        0,
        "ST_Touches must emit zero verified pairs from GPU"
    );
    assert!(
        get_metric_count(&metrics, "cpu_resolved") > 0,
        "ST_Touches must resolve true boundary matches on CPU"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// 10. ST_Touches: Mixed geometry batches (lines, polygons, multipoint on probe)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_touches_mixed_geometry_batches() -> Result<()> {
    let polygons = vec![(0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"))];
    let probe_geoms = vec![
        (0, Some("POINT (0 5)")),                                // touches boundary
        (1, Some("POINT (5 5)")),                                // interior -> no touch
        (2, Some("LINESTRING (-5 5, 0 5)")), // line touches outer boundary at (0, 5)
        (3, Some("LINESTRING (2 2, 8 8)")),  // line strictly inside -> no touch
        (4, Some("POLYGON ((10 0, 20 0, 20 10, 10 10, 10 0))")), // adjacent polygon touches edge
        (5, Some("POLYGON ((2 2, 8 2, 8 8, 2 8, 2 2))")), // interior polygon -> no touch
        (6, Some("MULTIPOINT ((0 5), (100 100))")), // touches at (0, 5)
        (7, Some("MULTIPOINT ((5 5), (100 100))")), // interior -> no touch
        (8, Some("POINT (15 15)")),          // exterior -> no touch
    ];

    let left = create_table(&polygons, 10)?;
    let right = create_table(&probe_geoms, 10)?;

    let (metrics, _) = assert_differential_query(
        left,
        right,
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Touches(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;

    assert_eq!(
        get_metric_count(&metrics, "gpu_verified"),
        0,
        "ST_Touches with mixed geometries must emit zero verified pairs from GPU"
    );
    assert!(
        get_metric_count(&metrics, "cpu_resolved") > 0,
        "ST_Touches with mixed geometries must resolve matches via CPU"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// 11. ST_Equals: Point vs Polygon, Point vs Point, Polygon vs Polygon (Invariant C)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_metal_equals() -> Result<()> {
    // 1. Point vs Polygon: always empty
    let poly_table = create_table(&[(0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"))], 10)?;
    let pt_table = create_table(&[(0, Some("POINT (5 5)")), (1, Some("POINT (0 0)"))], 10)?;

    let (metrics_pt_poly, _) = assert_differential_query(
        poly_table,
        pt_table,
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Equals(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;
    assert_eq!(get_metric_count(&metrics_pt_poly, "gpu_verified"), 0);

    // 2. Point vs Point: equal points match, non-equal do not
    let pts_left = create_table(&[(0, Some("POINT (5 5)")), (1, Some("POINT (10 10)"))], 10)?;
    let pts_right = create_table(&[(0, Some("POINT (5 5)")), (1, Some("POINT (20 20)"))], 10)?;

    let (metrics_pt_pt, _) = assert_differential_query(
        pts_left,
        pts_right,
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Equals(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;
    assert_eq!(get_metric_count(&metrics_pt_pt, "gpu_verified"), 0);
    assert_eq!(get_metric_count(&metrics_pt_pt, "cpu_resolved"), 1);

    // 3. Polygon vs Polygon: equal polygons match
    let poly_left = create_table(
        &[
            (0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")),
            (1, Some("POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))")),
        ],
        10,
    )?;
    let poly_right = create_table(
        &[
            (0, Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")),
            (1, Some("POLYGON ((50 50, 60 50, 60 60, 50 60, 50 50))")),
        ],
        10,
    )?;

    let (metrics_poly_poly, _) = assert_differential_query(
        poly_left,
        poly_right,
        "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Equals(L.geometry, R.geometry) ORDER BY l_id, r_id",
        10,
    ).await?;
    assert_eq!(get_metric_count(&metrics_poly_poly, "gpu_verified"), 0);
    assert_eq!(get_metric_count(&metrics_poly_poly, "cpu_resolved"), 1);

    Ok(())
}

// ---------------------------------------------------------------------------
// 12. End-to-End Production CPU Cost & Timing Benchmark (10 Repeats, Real CPU Refiner)
// ---------------------------------------------------------------------------
fn make_coastline_polygon_wkt(cx: f64, cy: f64, r: f64, n: usize, seed: u64) -> String {
    use std::fmt::Write;
    let mut wkt = String::with_capacity(n * 28 + 64);
    wkt.push_str("POLYGON ((");
    let mut state = seed;
    let mut next_f64 = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 11) as f64) / 9007199254740992.0
    };
    let mut phases = [0.0f64; 16];
    let mut amps = [0.0f64; 16];
    for k in 1..16 {
        phases[k] = next_f64() * std::f64::consts::TAU;
        amps[k] = (0.2 / (k as f64).powf(0.7)) * (0.8 + 0.4 * next_f64());
    }
    let mut first_pt = (0.0, 0.0);
    for i in 0..n {
        let theta = (i as f64) * std::f64::consts::TAU / (n as f64);
        let mut rad_scale = 1.0;
        for k in 1..16 {
            rad_scale += amps[k] * (k as f64 * theta + phases[k]).cos();
        }
        let rad = r * rad_scale.max(0.2);
        let px = cx + rad * theta.cos();
        let py = cy + rad * theta.sin();
        if i == 0 {
            first_pt = (px, py);
        } else {
            wkt.push_str(", ");
        }
        write!(&mut wkt, "{:.6} {:.6}", px, py).unwrap();
    }
    write!(&mut wkt, ", {:.6} {:.6}))", first_pt.0, first_pt.1).unwrap();
    wkt
}

#[tokio::test]
#[ignore]
async fn test_end_to_end_production_cpu_cost_benchmark() -> Result<()> {
    let vertex_counts = [1_000, 5_000, 10_000, 25_000, 50_000, 100_000];
    let num_probes = 5_000;
    let num_runs = 10;
    let warmup_runs = 2;

    println!("\n==========================================================================================================================");
    println!(
        " END-TO-END PRODUCTION SEDONADB SPANNING JOIN BENCHMARK (10 Repeated Runs, Median Timing)"
    );
    println!(" Engine: SedonaDB DataFusion SpatialJoinExec + Metal Spatial Index & Refiner + Production GEOS Refiner");
    println!(" Refiner Execution Mode: ExecutionMode::Speculative -> PrepareBuild (Prepared Build Polygon in GEOS, Point probes)");
    println!("==========================================================================================================================\n");

    println!(
        "{:<10} | {:<12} | {:<12} | {:<16} | {:<12} | {:<10} | {:<17} | {:<14} | {:<10}",
        "Vertices",
        "Candidates",
        "GPU Verified",
        "Uncertain Routed",
        "CPU Resolved",
        "Unc Rate %",
        "Median Total (ms)",
        "CPU GEOS (ms)",
        "CPU Share %"
    );
    println!("{:-<128}", "");

    for &n_verts in &vertex_counts {
        let cx = 500_000.0;
        let cy = 500_000.0;
        let r = 10_000.0;

        let poly_wkt = make_coastline_polygon_wkt(cx, cy, r, n_verts, 42);
        let polys = vec![(0, Some(poly_wkt.as_str()))];

        let span = r * 1.5;
        let mut state = 99999u64;
        let mut next_f64 = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 11) as f64) / 9007199254740992.0
        };

        let mut pts_wkt = Vec::with_capacity(num_probes);
        let mut probe_coords = Vec::with_capacity(num_probes);
        for _ in 0..num_probes {
            let px = cx - span + next_f64() * 2.0 * span;
            let py = cy - span + next_f64() * 2.0 * span;
            probe_coords.push((px, py));
            pts_wkt.push(format!("POINT ({:.6} {:.6})", px, py));
        }
        let points: Vec<(i32, Option<&str>)> = pts_wkt
            .iter()
            .enumerate()
            .map(|(i, s)| (i as i32, Some(s.as_str())))
            .collect();

        let left = create_table(&polys, 1024)?;
        let right = create_table(&points, 4096)?;

        let ctx = setup_context(true, true, 4096)?;
        ctx.register_table("L", left.clone())?;
        ctx.register_table("R", right.clone())?;

        let sql = "SELECT L.id l_id, R.id r_id FROM L JOIN R ON ST_Intersects(L.geometry, R.geometry) ORDER BY l_id, r_id";

        // Warm up runs
        for _ in 0..warmup_runs {
            let df = ctx.sql(sql).await?;
            let _ = df.collect().await?;
        }

        // 10 measured runs
        let mut total_times = Vec::with_capacity(num_runs);
        let mut last_metrics = None;

        for _ in 0..num_runs {
            let df = ctx.sql(sql).await?;
            let t0 = std::time::Instant::now();
            let plan = df.create_physical_plan().await?;
            let _results = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx()).await?;
            let elapsed = t0.elapsed();
            total_times.push(elapsed);
            last_metrics = find_spatial_join_metrics(&plan);
        }

        total_times.sort();
        let median_total = total_times[num_runs / 2];

        let metrics = last_metrics.unwrap();
        let verified = get_metric_count(&metrics, "gpu_verified");
        let cpu_resolved = get_metric_count(&metrics, "cpu_resolved");

        // Measure GPU refiner candidate & uncertain routing and production GEOS resolution time
        let mut refiner = sedona_metalspatial::MetalSpatialRefiner::try_new().unwrap();
        let poly_array: Arc<dyn arrow_array::Array> =
            Arc::new(create_array(&[Some(poly_wkt.as_str())], &WKB_GEOMETRY));
        let probe_slices: Vec<Option<&str>> = points.iter().map(|(_, s)| *s).collect();
        let probe_array: Arc<dyn arrow_array::Array> =
            Arc::new(create_array(&probe_slices, &WKB_GEOMETRY));

        refiner.push_build(&poly_array).unwrap();
        refiner.finish_building().unwrap();

        // Candidates: probes falling within polygon bounding box [cx - r, cx + r] x [cy - r, cy + r]
        let mut cand_b = Vec::new();
        let mut cand_p = Vec::new();
        for (i, &(px, py)) in probe_coords.iter().enumerate() {
            if px >= cx - r && px <= cx + r && py >= cy - r && py <= cy + r {
                cand_b.push(0u32);
                cand_p.push(i as u32);
            }
        }
        let cand_count = cand_b.len();

        let mut vb = Vec::new();
        let mut vp = Vec::new();
        let mut ub = Vec::new();
        let mut up = Vec::new();
        refiner
            .refine(
                &probe_array,
                sedona_metalspatial::ContainerSide::Build,
                &cand_b,
                &cand_p,
                &mut vb,
                &mut vp,
                &mut ub,
                &mut up,
            )
            .unwrap();

        let num_uncertain = ub.len();
        let unc_rate = if cand_count > 0 {
            (num_uncertain as f64) / (cand_count as f64) * 100.0
        } else {
            0.0
        };

        // Measure GEOS CPU resolution time on the exact uncertain candidates
        let mut cpu_elapsed = std::time::Duration::ZERO;
        if num_uncertain > 0 {
            use sedona_spatial_join::refine::IndexQueryResultRefinerFactory;
            let cpu_refiner_factory =
                sedona_spatial_join::refine::DefaultIndexQueryResultRefinerFactory;
            let stats = sedona_expr::statistics::GeoStatistics::empty()
                .with_total_geometries(1)
                .with_total_points(n_verts as i64);
            let cpu_refiner = cpu_refiner_factory
                .create_refiner(
                    &sedona_spatial_join::SpatialPredicate::Relation(
                        sedona_query_planner::spatial_predicate::RelationPredicate::new(
                            Arc::new(datafusion_physical_expr::expressions::Column::new("l", 0)),
                            Arc::new(datafusion_physical_expr::expressions::Column::new("r", 0)),
                            sedona_query_planner::spatial_predicate::SpatialRelationType::Intersects,
                        ),
                    ),
                    sedona_common::SpatialJoinOptions::default(),
                    1,
                    stats,
                )
                .unwrap();

            let poly_bin = poly_array
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            let probe_bin = probe_array
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            let poly_wkb = poly_bin.value(0);
            let poly_geom = wkb::reader::read_wkb(poly_wkb).unwrap();

            let query_results = vec![sedona_spatial_join::IndexQueryResult {
                wkb: &poly_geom,
                distance: None,
                geom_idx: 0,
                position: (0, 0),
            }];

            let t_cpu = std::time::Instant::now();
            for &p_idx in &up {
                let p_wkb = probe_bin.value(p_idx as usize);
                let p_geom = wkb::reader::read_wkb(p_wkb).unwrap();
                let _ = cpu_refiner.refine(&p_geom, &query_results).unwrap();
            }
            cpu_elapsed = t_cpu.elapsed();
        }

        let median_ms = median_total.as_secs_f64() * 1000.0;
        let cpu_geos_ms = cpu_elapsed.as_secs_f64() * 1000.0;
        let cpu_share = if median_ms > 0.0 {
            (cpu_geos_ms / median_ms) * 100.0
        } else {
            0.0
        };

        println!(
            "{:<10} | {:<12} | {:<12} | {:<16} | {:<12} | {:>9.3}% | {:>17.2} | {:>14.2} | {:>10.2}%",
            n_verts,
            cand_count,
            verified,
            num_uncertain,
            cpu_resolved,
            unc_rate,
            median_ms,
            cpu_geos_ms,
            cpu_share
        );
    }

    Ok(())
}

fn sum_spatial_join_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let own = if plan.name() == "SpatialJoinExec" {
        plan.metrics()
            .map(|m| get_metric_count(&m, name))
            .unwrap_or(0)
    } else {
        0
    };
    own + plan
        .children()
        .into_iter()
        .map(|child| sum_spatial_join_metric(child, name))
        .sum::<usize>()
}

/// SpatialBench double `ST_Within` join from docs/gpu-acceleration.md, CPU vs Metal.
///
/// Needs `SEDONA_SPATIALBENCH_DIR` pointing at a directory with `zone/` and `trip/`
/// parquet folders (e.g. `hf-data/v0.1.0/sf1`). `SEDONA_BENCH_MEMORY_LIMIT_GB`
/// (default 12) bounds the DataFusion memory pool; `SEDONA_BENCH_RUNS` (default 5),
/// `SEDONA_BENCH_BATCH_SIZE` (default 8192).
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_spatialbench_within_join_cpu_vs_metal_benchmark() -> Result<()> {
    let Ok(data_dir) = std::env::var("SEDONA_SPATIALBENCH_DIR") else {
        println!("SEDONA_SPATIALBENCH_DIR not set; skipping");
        return Ok(());
    };
    let env_usize = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let memory_limit_gb = env_usize("SEDONA_BENCH_MEMORY_LIMIT_GB", 12);
    let num_runs = env_usize("SEDONA_BENCH_RUNS", 5);
    let batch_size = env_usize("SEDONA_BENCH_BATCH_SIZE", 8192);

    let sql = "SELECT COUNT(*) AS cross_zone_trip_count
        FROM trip t
            JOIN zone pickup_zone
                ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(pickup_zone.z_boundary))
            JOIN zone dropoff_zone
                ON ST_Within(ST_GeomFromWKB(t.t_dropoffloc), ST_GeomFromWKB(dropoff_zone.z_boundary))
        WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey";

    println!("\nSpatialBench ST_Within join: {data_dir}, memory pool {memory_limit_gb} GB, batch size {batch_size}, {num_runs} runs + 1 warmup");

    let mut counts = Vec::new();
    for (label, gpu_enable) in [("CPU", false), ("Metal", true)] {
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit_gb << 30, 1.0)
            .build_arc()?;
        let state_builder = SessionStateBuilder::new()
            .with_default_features()
            .with_runtime_env(runtime);
        // No CPU fallback: a Metal failure must surface, not be timed as a Metal run.
        let ctx = setup_context_with_builder(state_builder, gpu_enable, false, batch_size)?;
        for table in ["zone", "trip"] {
            ctx.register_parquet(table, format!("{data_dir}/{table}/"), Default::default())
                .await?;
        }

        let mut times = Vec::with_capacity(num_runs);
        let mut count = 0i64;
        for run in 0..=num_runs {
            let df = ctx.sql(sql).await?;
            let t0 = std::time::Instant::now();
            let plan = df.create_physical_plan().await?;
            let batches = datafusion_physical_plan::collect(plan.clone(), ctx.task_ctx()).await?;
            let elapsed = t0.elapsed();
            count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0);

            if run == 0 {
                let plan_display = datafusion_physical_plan::displayable(plan.as_ref())
                    .indent(true)
                    .to_string();
                for line in plan_display
                    .lines()
                    .filter(|l| l.contains("SpatialJoinExec"))
                {
                    println!("  [{label}] {}", line.trim());
                }
                if gpu_enable {
                    assert!(
                        plan_display.contains("provider=Gpu"),
                        "Physical plan must use Gpu provider, got:\n{plan_display}"
                    );
                }
                println!("  [{label}] warmup: {:.3} s", elapsed.as_secs_f64());
                continue;
            }
            println!(
                "  [{label}] run {run}: {:.3} s | rows {count} | candidates {} | gpu_verified {} | cpu_resolved {}",
                elapsed.as_secs_f64(),
                sum_spatial_join_metric(&plan, "join_result_candidates"),
                sum_spatial_join_metric(&plan, "gpu_verified"),
                sum_spatial_join_metric(&plan, "cpu_resolved"),
            );
            // Time metrics are summed over both joins and all partitions, so they can exceed wall time.
            let ms = |name: &str| sum_spatial_join_metric(&plan, name) as f64 / 1e6;
            println!(
                "  [{label}]        summed ms: build_input_collection {:.0} | build {:.0} | join {:.0} | partition_probe {:.0}",
                ms("build_input_collection_time"),
                ms("build_time"),
                ms("join_time"),
                ms("partition_probe_time"),
            );
            times.push(elapsed);
        }
        times.sort();
        println!(
            "  [{label}] median {:.3} s | min {:.3} s | rows {count}",
            times[times.len() / 2].as_secs_f64(),
            times[0].as_secs_f64()
        );
        counts.push(count);
    }

    assert_eq!(counts[0], counts[1], "CPU and Metal row counts differ");
    Ok(())
}
