
CORE_ATHENA_SYNTAX_RULES = """
### AWS ATHENA (TRINO SQL) CORE SYNTAX RULES:

1. **UNNESTING ARRAYS - CRITICAL ALIAS RULE:**
   - For array<struct<field1, field2, field3>>:
     ✅ Option 1 (RECOMMENDED): UNNEST(array_col) AS t(item) → access item.field1
     ✅ Option 2 (EXPLICIT): UNNEST(array_col) AS t(field1, field2, field3) → access field1 directly
     ❌ WRONG: UNNEST(array_col) AS t → Missing alias causes MISMATCHED_COLUMN_ALIASES
     ❌ WRONG: UNNEST(array_col) AS lga → Only 1 alias for 3-field struct causes error

   - For simple arrays (array<int>, array<varchar>):
     ✅ CORRECT: UNNEST(array_col) AS t(value) → Single alias for single value
     ❌ WRONG: UNNEST(array_col) AS t(val1, val2) → Too many aliases

   **RULE**: Alias count MUST EXACTLY match the number of fields in the array element.

2. **ACCESSING UNNESTED FIELDS:**
   - After unnesting, use: alias.field_name
   - Field names are CASE-SENSITIVE and must match schema exactly
   - Example: CROSS JOIN UNNEST(names) AS t(name_item)
            SELECT name_item.name, name_item.languageCode

3. **COLUMN REFERENCES:**
   - ALL table columns MUST be enclosed in double quotes
   - ✅ CORRECT: SELECT "id", "geometry", "search_info"
   - ❌ WRONG: SELECT id, geometry, search_info
   - This prevents parsing errors with special characters

4. **ARRAY CONCATENATION:**
   - Use: concat(array1, array2) or flatten(array) for nested arrays
   - ❌ WRONG: array_concat, array_flatten (not supported)
"""

UNNEST_EXAMPLES = """
### CRITICAL: UNNEST Column Alias Matching

**Rule**: Alias column count MUST match struct field count EXACTLY, or use single alias for struct.

❌ WRONG Examples (THESE CAUSE ERRORS!):
-- Error: "Column alias list has 1 entries but 'lga' has 3 columns"
CROSS JOIN UNNEST(vpa."lane_group_lane_associations") AS lga
-- ❌ WRONG! lane_group_lane_associations has 3 struct fields: vpRange, fittedLane, interpolatedRoute

-- Error: "Column alias list has 2 entries but 't' has 1 columns"
CROSS JOIN UNNEST(simple_array) AS t(val1, val2)  -- simple_array is array<int> (1 field)

✅ CORRECT Examples (USE THESE!):

-- For lane_group_lane_associations (3 struct fields):
-- Method 1: Single alias (RECOMMENDED)
CROSS JOIN UNNEST(vpa."lane_group_lane_associations") AS t(lga_item)
-- Then access: lga_item.vpRange, lga_item.fittedLane, lga_item.interpolatedRoute

-- Method 2: Explicit field names (3 aliases for 3 fields)
CROSS JOIN UNNEST(vpa."lane_group_lane_associations") AS t(vpRange, fittedLane, interpolatedRoute)
-- Direct access: vpRange, fittedLane, interpolatedRoute

-- For simple array<int>
CROSS JOIN UNNEST(simple_array) AS t(value)

-- For array<struct<x:int, y:int>> with single alias
CROSS JOIN UNNEST(coordinates) AS t(coord)  -- Then access coord.x, coord.y

**How to Determine Alias Count**:
1. Check schema for struct field count: array<struct<field1, field2, field3>> → COUNT = 3
2. lane_group_lane_associations has 3 top-level fields: vpRange, fittedLane, interpolatedRoute → USE 1 OR 3 aliases
3. Simple arrays (array<int>) → always 1 alias
4. Array of structs → 1 alias (access via dot) OR N aliases (N = field count)
"""

SYNTAX_VALIDATION_RULES = """
1. MISMATCHED_COLUMN_ALIASES
   Problem: UNNEST alias columns don't match array element structure
   Fix: For array<struct<a,b,c>> with 3 fields, use EITHER:
        - UNNEST(arr) AS t(single_alias) then access single_alias.a, single_alias.b, single_alias.c
        - OR UNNEST(arr) AS t(a, b, c) for direct field access
   For simple arrays: UNNEST(arr) AS t(value) with exactly 1 alias

2. EXPRESSION_NOT_AGGREGATE
   Problem: Non-aggregated column in SELECT without GROUP BY
   Fix: Add all non-aggregate columns to GROUP BY, OR compute in grouped CTE then JOIN

3. INVALID_FUNCTION_ARGUMENT
   Problem: Function called with wrong parameter types or geometry type
   Fix: ST_Length needs LINE_STRING/MULTI_LINE_STRING for SphericalGeography. st_geometryn expects INTEGER not BIGINT. Use CAST.

4. SYNTAX_ERROR
   Problem: Missing JOIN conditions, misplaced clauses, unbalanced parentheses
   Fix: All JOINs need ON/USING BEFORE next clause. GROUP BY after all JOINs complete. Check parentheses balanced.

5. COLUMN_NOT_FOUND
   Problem: Referenced column doesn't exist or misspelled
   Fix: Verify column spelling (case-sensitive), check table alias, ensure column in schema

6. TYPE_MISMATCH
   Problem: Data type incompatibility in operation
   Fix: Use explicit CAST. Check function return types match usage.

7. JOIN_MISSING_CONDITION
   Problem: JOIN without ON or USING
   Fix: Add ON clause with join condition, or ON TRUE for UNNEST joins

8. GROUP_BY_MISSING_COLUMN
   Problem: SELECT has non-aggregated column not in GROUP BY
   Fix: Add column to GROUP BY or wrap in aggregate function

9. AGGREGATE_IN_WHERE
   Problem: Aggregate function in WHERE clause
   Fix: Move to HAVING clause or compute in subquery

10. NULL_GEOMETRY_OPERATION
    Problem: Geometry operation on NULL geometry
    Fix: Add WHERE geometry IS NOT NULL before ST_ operations

11. SPHERICAL_GEOGRAPHY_TYPE_ERROR
    Problem: SphericalGeography function on wrong geometry type
    Fix: ST_Length on SphericalGeography only accepts LINE_STRING/MULTI_LINE_STRING. Check with ST_Dimension first.

12. LAMBDA_SYNTAX_ERROR
    Problem: Incorrect lambda expression in TRANSFORM/FILTER/REDUCE
    Fix: Use arrow syntax: x -> expression

13. ARRAY_INDEX_OUT_OF_BOUNDS
    Problem: Accessing array with invalid index
    Fix: Check array size with cardinality(array) first, or use TRY

14. GEOMETRY_TYPE_ERROR
    Problem: Geometry operation on wrong type (ST_Length on POINT)
    Fix: Guard with ST_Dimension and ST_IsEmpty. ST_Length needs dimension=1, ST_Area needs dimension=2.

15. UNNEST_WITHOUT_CROSS_JOIN
    Problem: UNNEST without CROSS JOIN syntax
    Fix: Use CROSS JOIN UNNEST(array_col) AS t(alias)

16. PARTITION_FILTER_MISSING
    Problem: Query on partitioned table without partition filter
    Fix: Add WHERE clause on partition column (version, date) to reduce cost

17. TOO_MANY_COLUMNS
    Problem: Row size exceeds 32MB limit
    Fix: Reduce column count in SELECT or split query
"""

GEOMETRY_SPECIFIC_RULES = """
### GEOMETRY HANDLING (CRITICAL FOR GEOSPATIAL QUERIES):

⚠️  **MOST COMMON MISTAKES - READ THIS FIRST!** ⚠️

1. **NEVER DECLARE TYPE "Geometry"**
   - ❌ WRONG: `WITH results(id BIGINT, geom Geometry) AS ...`
   - ❌ WRONG: `CAST(... AS Geometry)`
   - ✅ CORRECT: Athena doesn't have a "Geometry" type! Use VARCHAR for WKT strings, then convert with ST_GeometryFromText

2. **ST_UNION vs GEOMETRY_UNION_AGG - They are DIFFERENT!**
   - ❌ WRONG: `ST_UNION(geometry_column)` - This takes TWO geometries, not one!
   - ❌ WRONG: `ST_Union(geom)` - Not supported in Athena
   - ✅ CORRECT: `geometry_union_agg(geometry_column)` - For aggregating multiple geometries into one
   - ✅ CORRECT: `ST_Union(geom1, geom2)` - For merging exactly TWO geometries (but prefer geometry_union_agg for aggregation)

3. **ST_LENGTH ONLY WORKS ON LINE_STRING/MULTI_LINE_STRING**
   - ❌ WRONG: `ST_Length(to_spherical_geography(geom))` on GEOMETRY_COLLECTION
   - ❌ WRONG: `ST_Length(to_spherical_geography(geom))` on POINT or POLYGON
   - ✅ CORRECT: Use guard clauses:
     ```sql
     CASE
       WHEN geometry IS NOT NULL AND NOT ST_IsEmpty(geometry) AND ST_Dimension(geometry) = 1
         THEN ST_Length(to_spherical_geography(geometry))
       ELSE 0
     END
     ```
   - **ST_Dimension(geometry) = 1** means LINE_STRING (0=POINT, 1=LINESTRING, 2=POLYGON)

4. **GEOMETRY_COLLECTION HANDLING:**
   - ❌ WRONG: `ST_CollectionExtract(geom)` - NOT SUPPORTED in Athena
   - ✅ CORRECT: Filter out GEOMETRY_COLLECTIONs using ST_Dimension or ST_IsEmpty checks
   - ✅ CORRECT: Extract individual geometries using ST_GeometryN(geom, index) if needed

5. **COORDINATE SYSTEM CONVERSION:**
   - ✅ USE: to_spherical_geography(geometry)
   - ❌ NEVER USE: ST_GeometryToSphericalGeography (not supported)

6. **GEOMETRY CONSTRUCTION:**
   - ✅ USE: ST_GeometryFromText(wkt_string)  -- (Trino/Athena canonical constructor for WKT)
   - ❌ NEVER USE: ST_GeomFromGeoJSON, ST_GeometryFromJson (not supported)
   - Use this wherever possible for WKT input

6a. **CRITICAL: GEOJSON STRUCT TO GEOMETRY CONVERSION (YOUR SCHEMA USES THIS!)** ⚠️

   **THE SCHEMA STORES GEOMETRY AS GEOJSON STRUCT, NOT NATIVE GEOMETRY TYPE!**

   Schema structure:
   - LINESTRING geometries: `struct<"type": varchar, "coordinates": array(array(double))>`
   - POLYGON geometries: `struct<"type": varchar, "coordinates": array(array(array(double)))>`

   **CORRECT PATTERN - Manual WKT Construction (ALWAYS USE THIS):**

   For LINESTRING (e.g., vehicle paths):
   ```sql
   ST_GeometryFromText(
     FORMAT('LINESTRING(%s)',
       array_join(
         transform(vp."geometry"."coordinates",
           p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))
         ),
         ','
       )
     )
   ) AS vp_geom
   ```

   For POLYGON (e.g., lane groups):
   ```sql
   ST_GeometryFromText(
     FORMAT('POLYGON((%s))',
       array_join(
         transform(lg."geometry"."coordinates"[1],
           p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))
         ),
         ','
       )
     )
   ) AS lg_geom
   ```

   **WHY THIS PATTERN?**
   - The struct is GeoJSON format, not native Geometry
   - You MUST manually construct WKT from the coordinates array
   - Then use ST_GeometryFromText() to convert WKT → Geometry type

   **❌ DO NOT USE from_geojson_geometry() FOR GEOMETRY OPERATIONS!**
   - ❌ WRONG: `from_geojson_geometry(json_format(vp."geometry")))`
   - **Why:** from_geojson_geometry() returns **SphericalGeography**, NOT Geometry!
   - This causes errors: "Unexpected parameters (SphericalGeography) for function geometry_union_agg. Expected: geometry_union_agg(geometry)"

   **WHEN can you use from_geojson_geometry()?**
   - ✅ ONLY when immediately converting to SphericalGeography for distance calculations:
     ```sql
     ST_Distance(
       from_geojson_geometry(json_format(vp."geometry")),
       from_geojson_geometry(json_format(lg."geometry"))
     )
     ```
   - ✅ NEVER use it if you need Geometry type for: geometry_union_agg, ST_Intersection, ST_AsText

   **TYPE COMPATIBILITY CHART:**
   ```
   Function                    Accepts Geometry?  Accepts SphericalGeography?
   -------------------------------------------------------------------------
   geometry_union_agg()        ✅ YES            ❌ NO (ERROR!)
   ST_Intersection()           ✅ YES            ❌ NO (ERROR!)
   ST_AsText()                 ✅ YES            ❌ NO (ERROR!)
   to_spherical_geography()    ✅ YES            ❌ NO (already is!)
   ST_Distance()               ❌ NO             ✅ YES
   ST_Length()                 ❌ NO             ✅ YES
   ```

   **RULE SUMMARY:**
   1. For ANY geometry operation that needs Geometry type → Use manual WKT construction pattern above
   2. For distance/length calculations → Can use from_geojson_geometry() directly (returns SphericalGeography)
   3. NEVER pass struct directly to geometry functions → Will error "Unexpected parameters (row(...))"

7. **WKT STRING FORMATTING:**
   - Always use FORMAT function, never CONCAT
   - Pattern: FORMAT('LINESTRING(%s)', array_join(transform(coordinates, p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))), ','))
   - This ensures proper WKT syntax for geometry output

8. **GEOMETRY TYPE PARAMETER RULES:**
   - st_geometryn expects: st_geometryn(geometry, integer)
   - ❌ WRONG: st_geometryn(geometry, bigint)
   - ✅ CORRECT: st_geometryn(geom, CAST(index AS integer))

9. **GROUP BY RESTRICTION:**
   - NEVER put geometry columns in GROUP BY clause
   - This will cause a query error
   - Group by IDs instead, then join geometry

10. **FORBIDDEN GEOMETRY FUNCTIONS:**
    - st_covers (not supported in Athena)
    - geometry_type (not supported)
    - st_collectionextract (not supported)
    - ST_GeometryToSphericalGeography (not supported)
"""

GUARD_CLAUSE_RULES = """
### MANDATORY GUARD CLAUSES (PREVENT RUNTIME ERRORS):

1. **NULL GEOMETRY CHECKS:**
   - ALWAYS check: WHERE geometry IS NOT NULL
   - Check before ANY geometry operation (ST_Length, ST_Distance, etc.)

2. **GEOMETRY TYPE VALIDATION:**
   - Before ST_Length(to_spherical_geography(...)): verify geometry is LINE_STRING or MULTI_LINE_STRING using ST_Dimension and ST_IsEmpty
   - Guard pattern example:
      CASE
        WHEN geometry IS NOT NULL AND NOT ST_IsEmpty(geometry) AND ST_Dimension(geometry) = 1
          THEN ST_Length(to_spherical_geography(geometry))
        ELSE 0
      END

3. **SPHERICAL GEOGRAPHY TYPE CHECKS:**
   - Error: "When applied to SphericalGeography inputs, ST_Length only supports LINE_STRING or MULTI_LINE_STRING"
   - Guard: Verify geometry type before applying to_spherical_geography and ST_Length

4. **ARRAY ACCESS GUARDS:**
   - Check array is not empty: WHERE cardinality(array_column) > 0
   - Check index bounds before accessing array[n]

5. **COLUMN DECLARATION ORDER:**
   - Ensure columns are declared/selected BEFORE they are accessed
   - In subqueries, derived columns must exist in SELECT before WHERE/HAVING
"""

FUNCTION_COMPATIBILITY_RULES = """
### ATHENA FUNCTION COMPATIBILITY (BLACKLIST):

FORBIDDEN FUNCTIONS (will cause errors):
- array_exists(array, lambda) - not supported, use alternative logic
- st_covers - not available
- geometry_type - not available  
- st_collectionextract - not available
- ST_GeometryToSphericalGeography - use to_spherical_geography instead

CORRECT ALTERNATIVES:
- For array filtering: Use CROSS JOIN UNNEST with WHERE clause
- For geometry type checking: Use conditional logic with ST_AsText
- For union: Use geometry_union_agg
"""

DOMAIN_SPECIFIC_RULES = """
### DOMAIN-SPECIFIC BUSINESS LOGIC:

1. **VEHICLE PATH AND LANEGROUP ASSOCIATIONS:**
   - When a vehicle path has multiple lanegroup associations, merge the lanegroups
   - Use geometry_union_agg to combine multiple lanegroup geometries
   - Pattern: geometry_union_agg(lanegroup_geometry) GROUP BY vehicle_path_id

2. **REQUIRED OUTPUT COLUMNS:**
   - Always include WKT for vehicle paths (AS vehicle_path_wkt)
   - Always include WKT for lanegroups (AS lanegroup_wkt)
   - Include relevant IDs for joining/filtering

3. **SPATIAL RELATIONSHIP QUERIES:**
   - When comparing vehicle paths to lanegroups (distance, containment, etc.):
     * Convert to spherical geography for accurate distance calculations
     * Use appropriate buffer/distance functions
     * Guard against null geometries on both sides
"""

QUERY_OPTIMIZATION_RULES = """
### QUERY PERFORMANCE & OPTIMIZATION (CRITICAL FOR LARGE DATASETS):

⚠️  **THESE RULES PREVENT TIMEOUTS AND ERRORS - FOLLOW STRICTLY** ⚠️

0. **ALWAYS FILTER BY VERSION IN BASE CTEs (CRITICAL - REDUCES DATA BY 3X+)**
   - ❌ WRONG: `FROM latest_vehiclepath vp WHERE vp."geometry" IS NOT NULL`
   - ✅ CORRECT: `FROM latest_vehiclepath vp WHERE vp."version" = 888 AND vp."geometry" IS NOT NULL`
   - **Why:** Tables named `latest_*` contain MULTIPLE versions (888, 751, 723, 667, etc.)
   - **Rule:** ALWAYS add `WHERE table."version" = 888` to EVERY base CTE for tables: latest_vehiclepath, latest_lanesgroup, latest_vehicle_path_associations, latest_roadtopology
   - **Impact:** Without this, you process 86M rows instead of 27M → guaranteed resource exhaustion
   - **Example:**
     ```sql
     vp_base AS (
       SELECT
         vp."id" AS vp_id,
         vp."iso_country_code" AS vp_country_code,
         ...
       FROM fastmap_prod2_v2_13_base.latest_vehiclepath vp
       WHERE vp."version" = 888  -- ✅ MANDATORY!
         AND vp."geometry" IS NOT NULL
     )
     ```
   - **Apply to ALL latest_* tables in the query for consistency**

1. **NEVER SELECT RAW GEOMETRY OBJECTS IN FINAL OUTPUT**
   - ❌ WRONG: `SELECT vp_geom AS vehicle_path_geom`
   - ❌ WRONG: `SELECT lg.geometry, vp.geometry`
   - ✅ CORRECT: `SELECT ST_AsText(vp_geom) AS vehicle_path_wkt`
   - **Why:** CTAS cannot store Geometry type → causes "Unsupported Hive type: Geometry" error
   - **Rule:** ONLY use ST_AsText() for geometry columns in final SELECT

2. **NEVER CREATE ARRAYS OF WKT STRINGS IN INTERMEDIATE CTEs**
   - ❌ WRONG:
     ```sql
     array_agg(DISTINCT FORMAT('LINESTRING(%s)', array_join(...))) AS vpa_wkts
     array_agg(DISTINCT FORMAT('POLYGON(%s)', ...)) AS lg_wkts
     ```
   - ✅ CORRECT:
     ```sql
     array_agg(DISTINCT vpa_id) AS vpa_ids  -- Store IDs only
     ```
   - **Why:** WKT arrays consume gigabytes (1000s of 5KB strings per row) → causes timeouts
   - **Rule:** In intermediate CTEs, aggregate IDs only. Single WKT columns in final output are fine.

3. **COMPUTE EXPENSIVE GEOMETRY OPERATIONS ONCE, REUSE EVERYWHERE**
   - ❌ WRONG: Computing ST_Intersection multiple times:
     ```sql
     SELECT
       ST_Length(to_spherical_geography(ST_Intersection(a, b))),  -- Computed
     WHERE
       ST_Intersection(a, b) IS NOT NULL  -- Computed again!
       AND ST_Dimension(ST_Intersection(a, b)) = 1  -- Computed again!
     ```
   - ✅ CORRECT: Store in CTE, reference it:
     ```sql
     WITH intersections AS (
       SELECT
         vp_id,
         ST_Intersection(vp_geom, lg_geom) AS int_geom
       FROM ...
     ),
     metrics AS (
       SELECT
         vp_id,
         int_geom,  -- Just reference it
         CASE
           WHEN int_geom IS NOT NULL
                AND NOT ST_IsEmpty(int_geom)
                AND ST_Dimension(int_geom) = 1
             THEN ST_Length(to_spherical_geography(int_geom))
           ELSE 0.0
         END AS overlap_length
       FROM intersections
     )
     ```
   - **Rule:** Expensive operations (ST_Intersection, ST_Union, geometry_union_agg) → compute once in CTE

4. **STRUCTURE: STORE IDS IN INTERMEDIATE CTEs, MATERIALIZE WKT IN FINAL OUTPUT**
   - ✅ CORRECT Pattern:
     ```sql
     -- Intermediate: IDs only
     vpa_mapping AS (
       SELECT
         vp_id,
         array_agg(DISTINCT vpa_id) AS vpa_ids  -- Small
       GROUP BY vp_id
     ),

     -- Intermediate: Union geometry for operations
     lg_unions AS (
       SELECT
         vp_id,
         geometry_union_agg(lg_geom) AS lg_union_geom  -- Keep as geometry
       GROUP BY vp_id
     ),

     -- Final output: Convert to WKT
     SELECT
       vp_id,
       ST_AsText(vp_geom) AS vehicle_path_wkt,  -- Single WKT per row ✅
       ST_AsText(lg_union_geom) AS lanegroup_wkt,  -- Single WKT per row ✅
       vpa_ids  -- Array of IDs from intermediate ✅
     ```

5. **FILTER NULL GEOMETRIES EARLY IN BASE CTEs**
   - ✅ Add to base table queries:
     ```sql
     FROM latest_vehiclepath vp
     WHERE vp.geometry IS NOT NULL  -- Filter early
     ```
   - **Why:** Reduces rows through expensive geometry operations
   - **Rule:** Add WHERE geometry IS NOT NULL to base CTEs before joins/operations

6. **AVOID REDUNDANT COLUMNS IN INTERMEDIATE AGGREGATIONS**
   - ❌ WRONG: Aggregating data already available elsewhere
   - ✅ CORRECT: Only aggregate what's needed for filtering/joins
   - Example: Don't aggregate country_codes if they're already in the base table
"""

VERSION_PARTITION_RULES = """
### VERSION AND PARTITION FILTERING (PERFORMANCE CRITICAL):

1. **VERSION FILTERING:**
   - Tables have a "version" field (not snapshot_version)
   - ALWAYS include: WHERE version = <specific_version>
   - Use version value from user guardrails or default to latest known
   - DO NOT use snapshot_version column (deprecated)

2. **PARTITION AWARENESS:**
   - Many tables are partitioned by version or date
   - Filtering on partition columns reduces cost dramatically
   - Always include partition filters in WHERE clause

3. **COST OPTIMIZATION:**
   - Project only needed columns (avoid SELECT *)
   - Add LIMIT clause unless specifically asked for all data
   - Use partition filters first in WHERE clause
"""

OUTPUT_REQUIREMENTS = """
### OUTPUT FORMATTING REQUIREMENTS:

1. **GEOMETRY OUTPUT:**
   - ALWAYS keep WKT representations in output when dealing with geometry data
   - Use AS alias to name WKT columns clearly (e.g., AS vehicle_path_wkt, AS lanegroup_wkt)
   - Format WKT using FORMAT function, not string concatenation
   - Example: FORMAT('LINESTRING(%s)', array_join(transform(vp_geom_struct."coordinates", p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))), ',')) AS vp_wkt

2. **COLUMN NAMING:**
   - Use descriptive aliases for computed columns
   - Example: ST_Length(geom) AS length_meters

3. **RESULT SIZE:**
   - For exploratory queries, always limit results
"""

# ============================================================================
# ERROR PATTERN CATALOG - For adaptive fixing
# ============================================================================

ERROR_PATTERNS = {
    "MISMATCHED_COLUMN_ALIASES": """
    UNNEST alias count doesn't match the array element's field count.

    For array<struct<field1, field2, field3>> (3 fields):
    WRONG: CROSS JOIN UNNEST(array_col) AS lga  -- Only 1 alias for 3 fields!

    CORRECT Option 1: CROSS JOIN UNNEST(array_col) AS t(single_alias)
                      Then access: single_alias.field1, single_alias.field2, single_alias.field3

    CORRECT Option 2: CROSS JOIN UNNEST(array_col) AS t(field1, field2, field3)
                      Direct access: field1, field2, field3

    Count the struct fields in the schema and match aliases exactly!
    """,
    
    "INVALID_FUNCTION_ARGUMENT": """
    Function parameter type mismatch. Common issues:
    - st_geometryn expects INTEGER, not BIGINT: CAST(value AS integer)
    - SphericalGeography type errors: check you're using to_spherical_geography correctly
    - Geometry type incompatibility: verify input geometry type matches function requirements
    """,

    "Unexpected parameters \\(row\\(.*\\)\\) for function geometry_union_agg": """
    ❌ ERROR: You passed a raw GeoJSON STRUCT to geometry_union_agg, which expects Geometry type!

    WRONG: geometry_union_agg(lg."geometry")  -- This is a struct, not Geometry!

    CORRECT: First convert struct → Geometry using manual WKT construction:
    ```sql
    geometry_union_agg(
      ST_GeometryFromText(
        FORMAT('POLYGON((%s))',
          array_join(
            transform(lg."geometry"."coordinates"[1],
              p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))
            ),
            ','
          )
        )
      )
    )
    ```

    See GEOMETRY_SPECIFIC_RULES section 6a for the correct pattern!
    """,

    "Unexpected parameters \\(SphericalGeography\\) for function geometry_union_agg": """
    ❌ ERROR: You used from_geojson_geometry() which returns SphericalGeography, not Geometry!

    WRONG: geometry_union_agg(from_geojson_geometry(...))  -- Returns SphericalGeography!

    CORRECT: Use manual WKT construction to get Geometry type:
    ```sql
    geometry_union_agg(
      ST_GeometryFromText(
        FORMAT('POLYGON((%s))',
          array_join(
            transform(lg."geometry"."coordinates"[1],
              p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))
            ),
            ','
          )
        )
      )
    )
    ```

    Remember: from_geojson_geometry() is ONLY for distance calculations (ST_Distance, ST_Length).
    For geometry operations (geometry_union_agg, ST_Intersection, ST_AsText), use manual WKT construction!
    """,

    "Unexpected parameters \\(SphericalGeography\\) for function st_astext": """
    ❌ ERROR: You passed SphericalGeography to ST_AsText, which expects Geometry type!

    WRONG: ST_AsText(from_geojson_geometry(...))  -- from_geojson_geometry returns SphericalGeography!

    CORRECT: Use manual WKT construction:
    ```sql
    ST_AsText(
      ST_GeometryFromText(
        FORMAT('LINESTRING(%s)',
          array_join(
            transform(vp."geometry"."coordinates",
              p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))
            ),
            ','
          )
        )
      )
    )
    ```

    from_geojson_geometry() should NEVER be used with ST_AsText!
    """,
    
    "Function .* not registered": """
    This function doesn't exist in Athena. Check the blacklist:
    - st_covers, geometry_type, st_collectionextract: NOT supported
    - array_exists: NOT supported, use UNNEST with WHERE
    - ST_GeometryToSphericalGeography: use to_spherical_geography
    """,
    
    "ST_Length only supports LINE_STRING": """
    You're passing wrong geometry type to ST_Length.
    
    Add guard clause:
    WHERE geometry IS NOT NULL
      AND geometry_is_valid(geometry)
      AND (check geometry type is LINE_STRING or MULTI_LINE_STRING)
    """,
    
    "group by.*geometry": """
    You cannot use geometry columns in GROUP BY.

    Solution: Group by ID columns, then join geometry later.
    Or aggregate geometries using geometry_union_agg.
    """,

    "Query exhausted resources at this scale factor|exhausted resources": """
    ❌ ERROR: Query ran out of memory/processing resources during execution.

    **MOST COMMON CAUSE:** Missing version filter on latest_* tables!

    IMMEDIATE FIX - Add version filters to ALL base CTEs:
    ```sql
    vp_base AS (
      SELECT ...
      FROM latest_vehiclepath vp
      WHERE vp."version" = 888  -- ✅ ADD THIS!
        AND vp."geometry" IS NOT NULL
    ),
    lg_map AS (
      SELECT ...
      FROM latest_vehicle_path_associations vpa
      WHERE vpa."version" = 888  -- ✅ ADD THIS!
        AND ...
    ),
    -- Apply to ALL latest_* tables!
    ```

    **Why this happens:**
    - Tables named latest_* contain MULTIPLE versions (888, 751, 723, 667, 542, etc.)
    - Without version filter: Processing 86M rows across all versions
    - With version = 888: Processing only 27M rows (3x reduction)
    - This is THE #1 cause of resource exhaustion

    **Other causes (if version filter already present):**
    1. Too many geometry unions per row (100s-1000s of LGs per VP)
    2. Computing expensive operations (ST_Intersection) on massive datasets
    3. Building WKT strings for millions of geometries

    **Additional optimizations if version filter doesn't help:**
    - Add LIMIT to final SELECT to test query logic first
    - Simplify geometry operations (use ST_Distance instead of ST_Intersection)
    - Add early filtering (reduce rows before expensive operations)
    """
}

MANDATORY_CONTEXT_COLUMNS = """
### CRITICAL: Always Include Context Columns

**For EVERY table in your query, ALWAYS include this columns in your SELECT statement:**

1. **id** - Primary identifier (always required)
   
2. **iso_country_code** - Country code (if table has it)
   - Format: `table_alias."iso_country_code" AS {alias}_country_code`
   - Example: `vp."iso_country_code" AS vp_country_code`
   - Example: `lg."iso_country_code" AS lg_country_code`
   - This enables users to filter results by country after CTAS creation

3. **Geometry columns** (always include as WKT for visualization)
   - Convert to WKT text: `ST_AsText(geometry) AS {name}_wkt`
   - Also keep original geometry for operations

**Why these are mandatory:**
- Users will query the CTAS table after creation
- They need to filter by location (country) and visualize geometries
- Without these columns, the CTAS is not useful for exploration

**Where to place them:**
- Place ID first
- Then iso_country_code
- Then geometry/WKT columns
- Then any computed metrics

**Example SELECT structure:**
```sql
SELECT
  vp."id" AS vp_id,
  vp."iso_country_code" AS vp_country_code,        -- Context!
  ST_AsText(vp."geometry") AS vp_wkt,              -- For viz
  lg."id" AS lg_id,
  lg."iso_country_code" AS lg_country_code,        -- Context!
  ST_AsText(lg."geometry") AS lg_wkt,              -- For viz
  overlap_length_meters,                            -- Computed metric
  is_outside_lanegroup                              -- Rule result
FROM ...
```

**IMPORTANT:** Even if the user's query doesn't mention country or location, ALWAYS include iso_country_code!
"""
# ============================================================================
# PROMPT BUILDERS
# ============================================================================

def create_sql_generation_prompt(schema: str, query: str, guardrails: str) -> str:
    """
    Build comprehensive SQL generation prompt with all relevant rules.
    
    Args:
        schema: Full DDL of approved tables/columns
        query: User's natural language query
        guardrails: User-provided additional constraints
        
    Returns:
        Complete prompt string for SQL generation
    """
    
    prompt = f"""You are an expert AWS Athena SQL programmer. Your task is to write a single, Optimized, Efficient and syntactically correct AWS Athena (Trino SQL) query to answer the user's question based on the provided schema.

### USER QUESTION:
{query}

### DATABASE SCHEMA:
{schema}

### USER-PROVIDED GUARDRAILS:
{guardrails if guardrails else "No specific guardrails provided."}

{MANDATORY_CONTEXT_COLUMNS}

{CORE_ATHENA_SYNTAX_RULES}

{GEOMETRY_SPECIFIC_RULES}

{GUARD_CLAUSE_RULES}

{FUNCTION_COMPATIBILITY_RULES}

{DOMAIN_SPECIFIC_RULES}

{QUERY_OPTIMIZATION_RULES}

{VERSION_PARTITION_RULES}

{OUTPUT_REQUIREMENTS}

### CRITICAL INSTRUCTIONS:
1. Adhere strictly to ALL syntax rules above - they prevent common errors
2. Include ALL mandatory guard clauses for NULL checks and type validation
3. Use ONLY supported functions - check the blacklist
4. If user provides specific version in guardrails, use that; otherwise filter on version field
5. Enclose all column names in double quotes
6. Follow domain-specific business logic for vehicle path/lanegroup queries
7. Generate ONLY the SQL query - no explanations, no markdown formatting
9. Make sure this error doesn't happen - [MISMATCHED_COLUMN_ALIASES]: UNNEST alias must match array element field count. For array<struct<a,b,c>> (3 fields), use EITHER: UNNEST(arr) AS t(single_alias) then access single_alias.a, single_alias.b, single_alias.c OR UNNEST(arr) AS t(a, b, c) for direct access. Count struct fields in schema EXACTLY!

### SQL QUERY:
"""
    return prompt


def create_sql_fixing_prompt(schema: str, query: str, broken_sql: str, error_message: str, schema_summary: str = "") -> str:
    """
    Build adaptive SQL fixing prompt based on error type.

    Args:
        schema: Full DDL of approved tables/columns
        query: Original user query
        broken_sql: The SQL that failed
        error_message: Exact error from Athena
        schema_summary: Token-optimized schema summary showing nested field counts

    Returns:
        Prompt with error-specific guidance
    """


    specific_guidance = ""
    import re
    for pattern, guidance in ERROR_PATTERNS.items():
        try:
            if re.search(pattern, error_message, re.IGNORECASE):
                specific_guidance = f"\n### SPECIFIC ERROR GUIDANCE:\n{guidance}\n"
                break
        except re.error:
        # fallback to simple substring if pattern is not a regex
            if pattern.lower() in error_message.lower():
                specific_guidance = f"\n### SPECIFIC ERROR GUIDANCE:\n{guidance}\n"
                break


    if not specific_guidance:
        specific_guidance = """
### GENERAL DEBUGGING GUIDANCE:
- Read the error message carefully - it tells you exactly what's wrong
- Common issues: function not found, column not found, type mismatch, syntax error
- Check the blacklist of unsupported functions
- Verify all column names match the schema exactly (case-sensitive)
- Ensure proper unnesting syntax for arrays of structs
- Ensure this rule is being followed - "ST_Length only supports LINE_STRING or MULTI_LINE_STRING, got GEOMETRY_COLLECTION"
"""

    # Format schema summary section
    schema_summary_section = ""
    if schema_summary.strip():
        schema_summary_section = f"""
---

## SCHEMA SUMMARY (For Quick Reference)

{schema_summary}

**UNNEST REMINDER:**
- If column shows "(nested: field1, field2, field3)", it has 3 struct fields
- Use EITHER: UNNEST(col) AS t(item) OR UNNEST(col) AS t(field1, field2, field3)
- Mismatched alias count causes "MISMATCHED_COLUMN_ALIASES" error!
"""

    prompt = f"""You are an expert AWS Athena SQL programmer. Your previous attempt to write a query failed. Analyze the error and write a corrected query.

### ORIGINAL USER QUESTION:
{query}

{schema_summary_section}

### FULL DATABASE SCHEMA:
{schema}

### BROKEN SQL QUERY:
```sql
{broken_sql}
```

### DATABASE ERROR MESSAGE:
{error_message}

{specific_guidance}

{CORE_ATHENA_SYNTAX_RULES}

{GEOMETRY_SPECIFIC_RULES}

{FUNCTION_COMPATIBILITY_RULES}

{QUERY_OPTIMIZATION_RULES}

### CRITICAL: NEVER USE THESE INVALID FUNCTIONS:
- ST_GeometryFromJson, ST_GeomFromJson → Use from_geojson_geometry OR build WKT strings
- array_flatten → Use flatten(array) instead
- array_length → Use cardinality(array)
- CAST row() to Geometry → Build WKT strings with FORMAT/array_join, then ST_GeometryFromText

### FIXING INSTRUCTIONS:
1. Analyze the error message - it pinpoints the exact problem
2. If UNNEST-related error, check schema summary to verify field counts!
3. If timeout or performance error, apply QUERY OPTIMIZATION RULES above!
4. Review the rules above that relate to this error
5. DO NOT use any invalid functions from the list above
6. For geometry conversion: Use array_join + transform + FORMAT to build WKT strings
7. DO NOT repeat the same mistake
8. Rewrite the ENTIRE query with the fix applied
9. Ensure the corrected query follows ALL Athena syntax rules
10. Generate ONLY the corrected SQL query - no explanations, no markdown

### CORRECTED SQL QUERY:
"""
    return prompt


# RAG-ENHANCED PROMPT BUILDER TO HELP LLM WITH MORE CONTEXT

def create_rag_sql_fixing_prompt(
    schema: str,
    query: str,
    broken_sql: str,
    error_message: str,
    relevant_docs: list,
    schema_summary: str = ""
) -> str:
    """
    Build RAG-enhanced SQL fixing prompt.
    Combines error patterns with dynamically retrieved documentation.

    Args:
        schema: Full DDL of approved tables/columns
        query: Original user query
        broken_sql: The SQL that failed
        error_message: Exact error from Athena
        relevant_docs: List of Document objects from vector store retrieval
        schema_summary: Token-optimized schema summary showing nested field counts

    Returns:
        Complete prompt string for SQL fixing with RAG context
    """

    # Format retrieved documentation
    if relevant_docs:
        doc_context = "\n\n".join([
            f"--- Relevant Documentation {i+1} ---\n{doc.page_content[:800]}"
            for i, doc in enumerate(relevant_docs[:3])
        ])

        rag_section = f"""
### RELEVANT DOCUMENTATION FOR THIS ERROR:
The following documentation will most certainly help fix this specific error:

{doc_context}

**USE THIS**: Check if the error relates to any functions/syntax shown above.
"""
    else:
        rag_section = ""

    # Find specific error guidance from patterns
    specific_guidance = ""
    import re
    for pattern, guidance in ERROR_PATTERNS.items():
        try:
            if re.search(pattern, error_message, re.IGNORECASE):
                specific_guidance = f"\n### SPECIFIC ERROR GUIDANCE:\n{guidance}\n"
                break
        except re.error:
            if pattern.lower() in error_message.lower():
                specific_guidance = f"\n### SPECIFIC ERROR GUIDANCE:\n{guidance}\n"
                break

    if not specific_guidance:
        specific_guidance = """
### GENERAL DEBUGGING GUIDANCE:
- Read the error message carefully - it tells you exactly what's wrong
- Common issues: function not found, column not found, type mismatch, syntax error
- Check the blacklist of unsupported functions
- Verify all column names match the schema exactly (case-sensitive)
- Ensure proper unnesting syntax for arrays of structs
"""

    # Format schema summary section
    schema_summary_section = ""
    if schema_summary.strip():
        schema_summary_section = f"""
---

## SCHEMA SUMMARY (For Quick Reference)

{schema_summary}

**UNNEST REMINDER:**
- If column shows "(nested: field1, field2, field3)", it has 3 struct fields
- Use EITHER: UNNEST(col) AS t(item) OR UNNEST(col) AS t(field1, field2, field3)
- Mismatched alias count causes "MISMATCHED_COLUMN_ALIASES" error!
"""

    prompt = f"""You are an expert AWS Athena SQL programmer. Your previous attempt to write a query failed. Analyze the error and write a corrected query.

### ORIGINAL USER QUESTION:
{query}

{schema_summary_section}

### FULL DATABASE SCHEMA:
{schema}

### BROKEN SQL QUERY:
```sql
{broken_sql}
```

### DATABASE ERROR MESSAGE:
{error_message}

{rag_section}

{specific_guidance}

{CORE_ATHENA_SYNTAX_RULES}

{GEOMETRY_SPECIFIC_RULES}

{FUNCTION_COMPATIBILITY_RULES}

{QUERY_OPTIMIZATION_RULES}

### CRITICAL: NEVER USE THESE INVALID FUNCTIONS:
- ST_GeometryFromJson, ST_GeomFromJson → Use from_geojson_geometry OR build WKT strings
- array_flatten → Use flatten(array) instead
- array_length → Use cardinality(array)
- CAST row() to Geometry → Build WKT strings with FORMAT/array_join, then ST_GeometryFromText

### FIXING INSTRUCTIONS:

1. CHECK THE DOCUMENTATION ABOVE FIRST - it may contain the exact syntax you need
2. If UNNEST-related error, check schema summary to verify field counts!
3. If timeout or performance error, apply QUERY OPTIMIZATION RULES above!
4. Analyze the error message - it pinpoints the exact problem
5. DO NOT use any invalid functions from the list above
6. For geometry conversion: Use array_join + transform + FORMAT to build WKT strings
7. DO NOT repeat the same mistake
8. Rewrite the ENTIRE query with the fix applied
9. Ensure the corrected query follows ALL Athena syntax rules
10. Generate ONLY the corrected SQL query - no explanations, no markdown

### CORRECTED SQL QUERY:
"""
    return prompt


def create_function_validation_prompt(
    generated_sql: str,
    all_functions_with_docs: dict,
    suspicious_functions: list,
    invalid_functions: list,
    schema: str
) -> str:
    
    # Format suspicious functions
    suspicious_list = "\n".join([
        f"  - {func} (not in known-good list - verify against docs, but if they are alias/WKT type, don't alter alias/WKT type)"
        for func in suspicious_functions
    ]) if suspicious_functions else "  None"
    
    # Format invalid functions
    invalid_list = "\n".join([
        f"  - {item['function']}: {item['issue']}"
        for item in invalid_functions
    ]) if invalid_functions else "  None"
    
    # Format function documentation
    functions_section = ""
    for func_name, docs in all_functions_with_docs.items():
        if not docs:
            continue
        
        functions_section += f"\n### {func_name}\n"
        
        # Mark if suspicious/invalid
        if func_name in suspicious_functions:
            functions_section += "STATUS: Suspicious (not in known-good list)\n"
        elif any(item['function'] == func_name for item in invalid_functions):
            functions_section += "STATUS: Invalid (known to be unsupported)\n"
        else:
            functions_section += "STATUS: Valid\n"
        
        functions_section += "\nFunction Reference:\n"
        for doc in docs[:2]:
            content = doc.page_content
            
            # Indent for readability
            # indented_content = "\n".join([f"  {line}" for line in content.split('\n')])
            functions_section += content + "\n"
        
        functions_section += "\n"
    
    prompt = f"""You are an AWS Athena SQL function validator. Your task is to validate that all functions in the SQL query are:
1. Supported by Athena/Trino
2. Used correctly (correct parameters, order, types)

### GENERATED SQL TO VALIDATE:
```sql
{generated_sql}
```
### DATABASE SCHEMA (for type checking)
{schema[:4000]}....
---

## FUNCTION STATUS

Suspicious Functions (not in known-good list):
{suspicious_list}

Invalid Functions (known unsupported):
{invalid_list}

---

## ALL FUNCTIONS WITH DOCUMENTATION

{functions_section}

---

## VALIDATION INSTRUCTIONS

### Step 1: Replace Invalid Functions
For each function in the "Invalid" list:
- Find the suggested Athena alternative in the "issue" description
- Replace in the SQL

### Step 2: Verify Suspicious Functions
For each suspicious function:
- Check if it appears in the Function Reference section
- If YES and usage matches → it's valid, keep it
- If NO and not a table alias/WKT type → it's unsupported, remove or replace
- IGNORE if it's: table/CTE alias (AC, LGA, PL, RT, vp, lg), WKT type (LINESTRING, POLYGON), SQL type (VARCHAR, BIGINT, DOUBLE)

### Step 3: Validate ALL Function Usage
For EVERY function (even valid ones), verify:
- **Parameter count**: Must match "Parameters" count in reference
- **Parameter types**: Must match expected types (use CAST if needed)
- **Parameter order**: Must match "Syntax" pattern exactly
- **Return type usage**: If function returns geometry, don't use in arithmetic; if returns number, don't use in ST_ operations

### Common Mistakes to Check:
1. **Geospatial parameter order**:
   - ST_Buffer(geometry, distance) ← geometry FIRST
   - ST_Point(longitude, latitude) ← longitude FIRST
   - ST_Distance(geom1, geom2) ← both must be same type

2. **Type restrictions**:
   - ST_Length on SphericalGeography: only LINE_STRING or MULTI_LINE_STRING
   - st_geometryn(geom, index): index must be INTEGER (use CAST if BIGINT)

3. **Array functions**:
   - TRANSFORM(array, x -> expression): arrow syntax required
   - ARRAY_JOIN(array, delimiter, null_replacement): 3rd param optional
   - FILTER(array, x -> condition): condition must return boolean

---

## OUTPUT REQUIREMENTS

Return the corrected SQL query:
- Fix all invalid functions
- Fix all parameter issues
- If no issues found, return original SQL unchanged

CRITICAL:
- Return ONLY the SQL query
- NO markdown blocks (```sql)
- NO explanations
- NO comments unless they were in original

### VALIDATED SQL:
"""
    return prompt



def create_syntax_validation_prompt(
    function_validated_sql: str,
    errors_txt_content: str,
    schema: str,
    schema_summary: str = ""
) -> str:
    """
    Creates prompt for syntax-only validation.

    Args:
        function_validated_sql: SQL after function validation
        errors_txt_content: Content from errors.txt (daily populated)
        schema: Database schema (full DDL)
        schema_summary: Token-optimized schema summary showing nested field counts

    Returns:
        Syntax validation prompt
    """

    # Format dynamic errors from errors.txt
    dynamic_errors_section = ""
    if errors_txt_content.strip():
        dynamic_errors_section = f"""
---

## PRODUCTION ERRORS (From Recent Failures)

{errors_txt_content}

These errors occurred in production. Check if your SQL might trigger them.
"""

    # Format schema summary section
    schema_summary_section = ""
    if schema_summary.strip():
        schema_summary_section = f"""
---

## SCHEMA SUMMARY (For Quick Reference)

{schema_summary}

CRITICAL: Use this summary to verify UNNEST alias counts!
- If a column shows "(nested: field1, field2, field3)", it has 3 struct fields
- Your UNNEST must have EITHER:
  - Method 1: Single alias (recommended) - UNNEST(column) AS t(item)
  - Method 2: Explicit aliases matching count - UNNEST(column) AS t(field1, field2, field3)
"""

    prompt = f"""You are an AWS Athena SQL syntax validator.

CRITICAL INSTRUCTION: The SQL functions have ALREADY been validated in a previous step. DO NOT modify any functions or their usage. Your ONLY job is to fix SQL syntax/structure issues.

### SQL TO VALIDATE (Functions Already Validated):

```sql
{function_validated_sql}
```

---

{SYNTAX_VALIDATION_RULES}

{dynamic_errors_section}

{UNNEST_EXAMPLES}

{schema_summary_section}

### FULL SCHEMA DDL:

{schema}

---

## CRITICAL CHECK BEFORE RETURNING

**MANDATORY UNNEST VALIDATION:**
Before returning the SQL, you MUST:
1. Find ALL UNNEST statements in the query
2. For each UNNEST on a nested column:
   a) Check the schema summary to count how many fields the column has
   b) Verify the alias count matches OR uses single-alias method
   c) If mismatch found, FIX IT immediately

Example check:
- SQL has: `UNNEST(vpa.lane_group_lane_associations) AS lga`
- Schema shows: "lane_group_lane_associations (nested: vpRange, fittedLane, interpolatedRoute)" = 3 fields!
- Alias count: 1 (just "lga")
- **THIS IS WRONG!** Fix to: `UNNEST(vpa.lane_group_lane_associations) AS t(lga_item)`


## OUTPUT REQUIREMENTS

CRITICAL RULES:
1. DO NOT change any functions (they're already validated)
2. DO NOT change function parameters or usage
3. ONLY look for and fix syntax issues (especially UNNEST mismatches!)

If syntax issues found:
- Fix all structural problems (especially UNNEST alias counts)
- Keep all functions unchanged
- Return corrected SQL

If no syntax issues:
- Return original SQL unchanged

FORMAT:
- Return ONLY the SQL query
- NO markdown blocks
- NO explanations outside SQL

### SYNTAX-VALIDATED SQL:
"""

    return prompt