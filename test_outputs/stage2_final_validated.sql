WITH vp_geoms AS (
  SELECT
    vp."id" AS vp_id,
    split(
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
    geometry_union_agg(
      ST_GeometryFromText(
        FORMAT(
          'POLYGON(%s)',
          array_join(
            transform(lg."geometry"."coordinates", ring ->
              FORMAT(
                '(%s)',
                array_join(
                  transform(ring[1:cardinality(ring)], p -> FORMAT('%s %s', CAST(p[1] AS varchar), CAST(p[2] AS varchar))),
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
  CROSS JOIN UNNEST(a."lane_group_lane_associations") AS lga
  JOIN fastmap_prod2_v2_13_base."latest_lanesgroup" lg ON TRUE
  WHERE lg."geometry" IS NOT NULL
  GROUP BY a."vehicle_path"."id"
),
topo_flags AS (
  SELECT
    rt."id" AS topo_id,
    (count_if(COALESCE(pr."value", false)) > 0) AS is_private_road,
    (count_if(COALESCE(pl."value", false)) > 0) AS is_parking_lot_road,
    (count_if(COALESCE(pub."value", false)) > 0) AS is_public_access_road,
    (count_if(COALESCE(ac."auto", false)) > 0) AS is_auto_allowed
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
  GROUP BY a."vehicle_path"."id", tf.is_private_road
)
SELECT
  vpg.vp_id,
  ST_AsText(vpg.vp_geom) AS vehicle_path_wkt,
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
         AND ST_Dimension(ST_Intersection(vpg.vp_geom, lgu.lg_union_geom)) = 1
      THEN ST_Length(to_spherical_geography(ST_Intersection(vpg.vp_geom, lgu.lg_union_geom)))
    ELSE 0.0
  END AS overlap_length_meters
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