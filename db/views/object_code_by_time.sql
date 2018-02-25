-- View: object_code_by_time

-- DROP VIEW object_code_by_time;

CREATE OR REPLACE VIEW object_code_by_time AS 
 SELECT a.d2_id,
    dim_2.d2_name AS product_id,
    dim_3.d3_id,
    dim_3.d3_name AS currency_id,
    dim_4.d4_id,
    dim_4.d4_name AS datasrc_id,
    dim_5.d5_id,
    dim_5.d5_name AS objectcode_id,
    dim_8.d8_id,
    dim_8.d8_name AS scenario_id,
    dim_9.d9_id,
    dim_9.d9_name AS time_id,
    dim_11.d11_id,
    dim_11.d11_name AS variation_id,
    a.d6_id,
    dim_6.d6_name AS organization_id,
    a.r6 AS expense_balance
   FROM ( SELECT grain_202.d2_id,
            root_6.d3_id,
            root_6.d4_id,
            g5.goofy_d5_id AS d5_id,
            root_6.d8_id,
            g9.goofy_d9_id AS d9_id,
            root_6.d11_id,
            root_6.d6_id,
            sum(root_6.r6) AS r6
           FROM root_6
             JOIN grain_202 USING (d2_id)
             JOIN grain_301 USING (d3_id, epoch_id)
             JOIN grain_406 USING (epoch_id, d4_id)
             JOIN ( SELECT grain_505.goofy_d5_id AS d5_id,
                    grain_505.d5_id AS goofy_d5_id
                   FROM grain_505
                  WHERE grain_505.d5_id = 2205) g5 USING (d5_id)
             JOIN grain_801 USING (epoch_id, d8_id)
             JOIN ( SELECT grain_905.goofy_d9_id AS d9_id,
                    grain_905.d9_id AS goofy_d9_id
                   FROM grain_905
                  WHERE grain_905.d9_id = 192) g9 USING (d9_id)
             JOIN grain_1102 USING (epoch_id, d11_id)
             JOIN grain_602 USING (epoch_id, d6_id)
          WHERE root_6.active
          GROUP BY grain_202.d2_id, root_6.d3_id, root_6.d4_id, g5.goofy_d5_id, root_6.d8_id, g9.goofy_d9_id, root_6.d11_id, root_6.d6_id
        UNION
         SELECT grain_202.d2_id,
            root_6.d3_id,
            root_6.d4_id,
            root_6.d5_id,
            root_6.d8_id,
            g9.goofy_d9_id AS d9_id,
            root_6.d11_id,
            root_6.d6_id,
            sum(root_6.r6) AS r6
           FROM root_6
             JOIN grain_202 USING (d2_id)
             JOIN grain_301 USING (d3_id, epoch_id)
             JOIN grain_406 USING (epoch_id, d4_id)
             JOIN grain_503 USING (epoch_id, d5_id)
             JOIN grain_801 USING (epoch_id, d8_id)
             JOIN ( SELECT grain_905.goofy_d9_id AS d9_id,
                    grain_905.d9_id AS goofy_d9_id
                   FROM grain_905
                  WHERE grain_905.d9_id = 192) g9 USING (d9_id)
             JOIN grain_1102 USING (epoch_id, d11_id)
             JOIN grain_602 USING (epoch_id, d6_id)
          WHERE root_6.active
          GROUP BY grain_202.d2_id, root_6.d3_id, root_6.d4_id, root_6.d5_id, root_6.d8_id, g9.goofy_d9_id, root_6.d11_id, root_6.d6_id
        UNION
         SELECT grain_202.d2_id,
            root_6.d3_id,
            root_6.d4_id,
            g5.goofy_d5_id AS d5_id,
            root_6.d8_id,
            root_6.d9_id,
            root_6.d11_id,
            root_6.d6_id,
            sum(root_6.r6) AS r6
           FROM root_6
             JOIN grain_202 USING (d2_id)
             JOIN grain_301 USING (d3_id, epoch_id)
             JOIN grain_406 USING (epoch_id, d4_id)
             JOIN ( SELECT grain_505.goofy_d5_id AS d5_id,
                    grain_505.d5_id AS goofy_d5_id
                   FROM grain_505
                  WHERE grain_505.d5_id = 2205) g5 USING (d5_id)
             JOIN grain_801 USING (epoch_id, d8_id)
             JOIN grain_903 USING (epoch_id, d9_id)
             JOIN grain_1102 USING (epoch_id, d11_id)
             JOIN grain_602 USING (epoch_id, d6_id)
          WHERE root_6.active
          GROUP BY grain_202.d2_id, root_6.d3_id, root_6.d4_id, g5.goofy_d5_id, root_6.d8_id, root_6.d9_id, root_6.d11_id, root_6.d6_id
        UNION
         SELECT grain_202.d2_id,
            root_6.d3_id,
            root_6.d4_id,
            root_6.d5_id,
            root_6.d8_id,
            root_6.d9_id,
            root_6.d11_id,
            root_6.d6_id,
            root_6.r6
           FROM root_6
             JOIN grain_202 USING (d2_id)
             JOIN grain_301 USING (d3_id, epoch_id)
             JOIN grain_406 USING (epoch_id, d4_id)
             JOIN grain_503 USING (epoch_id, d5_id)
             JOIN grain_801 USING (epoch_id, d8_id)
             JOIN grain_903 USING (epoch_id, d9_id)
             JOIN grain_1102 USING (epoch_id, d11_id)
             JOIN grain_602 USING (epoch_id, d6_id)
          WHERE root_6.active) a
     JOIN dim_2 USING (d2_id)
     JOIN dim_3 USING (d3_id)
     JOIN dim_4 USING (d4_id)
     JOIN dim_5 USING (d5_id)
     JOIN dim_8 USING (d8_id)
     JOIN dim_9 USING (d9_id)
     JOIN dim_11 USING (d11_id)
     JOIN dim_6 USING (d6_id);

ALTER TABLE object_code_by_time
  OWNER TO canopy_db_admin;
