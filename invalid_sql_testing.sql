WITH vp_geoms AS (
  SELECT
    vp."id" AS vp_id,
    -- Error 1: Invalid function STRING_TO_ARRAY (should be split)
    STRING_TO_ARRAY(
      array_join(
        transform(vp."geometry"."coordinates", p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))),
        ','
      ),
      ','
    ) AS vp_coords_array,
    FORMAT(
      'LINESTRING(%s)',
      array_join(
        transform(vp."geometry"."coordinates", p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))),
        ','
      )
    ) AS vp_wkt,
    ST_GeometryFromText(
      FORMAT(
        'LINESTRING(%s)',
        array_join(
          transform(vp."geometry"."coordinates", p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))),
          ','
        )
      )
    ) AS vp_geom
  FROM fastmap_prod2_v2_13_base."latest_vehiclepath" vp
  WHERE vp."geometry" IS NOT NULL
),
vp_lg_union AS (
  SELECT
    a."vehicle_path"."id" AS vp_id,
    -- Error 2: Invalid function ST_UNION_AGG (should be geometry_union_agg)
    ST_UNION_AGG(
      ST_GeometryFromText(
        FORMAT(
          'POLYGON(%s)',
          array_join(
            transform(lg."geometry"."coordinates", ring ->
              FORMAT(
                '(%s)',
                array_join(
                  -- Error 3: Misuse ARRAY_LENGTH instead of cardinality
                  transform(ring[1:ARRAY_LENGTH(ring)], p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))),
                  ','
                )
              )
            ),
            ','
          )
        )
      )
    ) AS lg_union_geom
  FROM fastmap_prod2_v2_13_base."latest_vehicle_path_associations" a
  -- Syntax Error 1: JOIN missing ON/USING condition
  CROSS JOIN UNNEST(a."lane_group_lane_associations") AS lga
  JOIN fastmap_prod2_v2_13_base."latest_lanesgroup" lg
  WHERE lg."geometry" IS NOT NULL
  GROUP BY a."vehicle_path"."id"
),
topo_flags AS (
  SELECT
    rt."id" AS topo_id,
    -- Error 4: Invalid function IFNULL (should be COALESCE)
    (count_if(IFNULL(pr."value", false)) > 0) AS is_private_road,
    (count_if(COALESCE(pl."value", false)) > 0) AS is_parking_lot_road,
    (count_if(COALESCE(pub."value", false)) > 0) AS is_public_access_road,
    -- Error 5: Misuse DATE_FORMAT on boolean (valid function, wrong context/type)
    (count_if(DATE_FORMAT(ac."auto", '%Y-%m-%d')) > 0) AS is_auto_allowed
  FROM fastmap_prod2_v2_13_base."latest_roadtopology" rt
  LEFT JOIN UNNEST(rt."topology_characteristics"."isPrivateRoad") AS pr ON TRUE
  LEFT JOIN UNNEST(rt."topology_characteristics"."isParkingLotRoad") AS pl ON TRUE
  LEFT JOIN UNNEST(rt."topology_characteristics"."isPublicAccessRoad") AS pub ON TRUE
  LEFT JOIN UNNEST(rt."access_characteristics") AS ac ON TRUE
  GROUP BY rt."id"
),
vp_topo_cond AS (
  SELECT
    a."vehicle_path"."id" AS vp_id,
    -- Syntax Error 2: EXPRESSION_NOT_AGGREGATE - tf.is_private_road not aggregated but used alongside count_if
    tf.is_private_road,
    (count_if(
       (NOT tf.is_private_road) OR
       (NOT tf.is_parking_lot_road) OR
       (NOT tf.is_public_access_road) OR
       (NOT tf.is_auto_allowed)
     ) > 0) AS has_any_non_compliant_topology
  FROM fastmap_prod2_v2_13_base."latest_vehicle_path_associations" a
  CROSS JOIN UNNEST(a."topology_associations") AS ta
  LEFT JOIN topo_flags tf
    ON tf.topo_id = ta."matchedSegment"."segment"."id"
  GROUP BY a."vehicle_path"."id"
)
SELECT
  vpg.vp_id,
  -- Error 6: Invalid function TO_CHAR (should be CAST or format)
  TO_CHAR(vpg.vp_geom) AS vehicle_path_wkt,
  ST_AsText(lgu.lg_union_geom) AS lanegroup_wkt,
  CASE
    WHEN lgu.lg_union_geom IS NOT NULL AND vpg.vp_geom IS NOT NULL
      THEN NOT ST_Contains(lgu.lg_union_geom, vpg.vp_geom)
    ELSE NULL
  END AS is_outside_lanegroup,
  CASE
    WHEN lgu.lg_union_geom IS NOT NULL
         AND vpg.vp_geom IS NOT NULL
         AND NOT ST_IsEmpty(lgu.lg_union_geom)
         AND NOT ST_IsEmpty(vpg.vp_geom)
         -- Error 7: Misuse ST_Dimension - passing wrong number of arguments or wrong type
         AND ST_Dimension(ST_Intersection(vpg.vp_geom, lgu.lg_union_geom), 'INVALID_EXTRA_ARG') = 1
      -- Error 8: Misuse ST_Length - passing potentially non-linear geometry (GeometryCollection risk from ST_Intersection)
      THEN ST_Length(to_spherical_geography(ST_Intersection(vpg.vp_geom, lgu.lg_union_geom)))
    ELSE 0.0
  END AS overlap_length_meters,
  -- Syntax Error 3: Column mismatch - referencing non-existent column
  lgu.non_existent_column AS fake_col
FROM vp_geoms vpg
JOIN vp_lg_union lgu
  ON lgu.vp_id = vpg.vp_id
JOIN vp_topo_cond vpt
  ON vpt.vp_id = vpg.vp_id
WHERE lgu.lg_union_geom IS NOT NULL
  AND vpg.vp_geom IS NOT NULL
  AND NOT ST_IsEmpty(lgu.lg_union_geom)
  AND NOT ST_IsEmpty(vpg.vp_geom)
  AND NOT ST_Contains(lgu.lg_union_geom, vpg.vp_geom)
  AND (
    CASE
      WHEN ST_Dimension(ST_Intersection(vpg.vp_geom, lgu.lg_union_geom)) = 1
        THEN ST_Length(to_spherical_geography(ST_Intersection(vpg.vp_geom, lgu.lg_union_geom)))
      ELSE 0.0
    END
  ) > 5.0
  AND vpt.has_any_non_compliant_topology
LIMIT 1000;
