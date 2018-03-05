-- View: elt.app_net_rev

-- DROP VIEW elt.app_net_rev;

CREATE OR REPLACE VIEW elt.app_net_rev AS
 SELECT a.d2_id,
    dim_2.d2_name AS product_id,
    dim_3.d3_id,
    dim_3.d3_name AS currency_id,
    dim_4.d4_id,
    dim_4.d4_name AS datasrc_id,
    dim_5.d5_id,
    dim_5.d5_name AS objectcode_id,
    dim_5.d5_desc AS objectcode_desc,
    dim_8.d8_id,
    dim_8.d8_name AS scenario_id,
    dim_9.d9_id,
    dim_9.d9_name AS time_id,
    dim_11.d11_id,
    dim_11.d11_name AS variation_id,
    a.d6_id,
    dim_6.d6_name AS organization_id,
    a.a20 AS app_net_rev
   FROM ( SELECT grain_206.goofy_d2_id AS d2_id,
            app_20.d3_id,
            app_20.d4_id,
            grain_515.goofy_d5_id AS d5_id,
            app_20.d8_id,
            app_20.d9_id,
            app_20.d11_id,
            grain_606.goofy_d6_id AS d6_id,
            sum(app_20.a20) AS a20
           FROM app_20
             JOIN grain_206 USING (epoch_id, d2_id)
             JOIN grain_515 USING (epoch_id, d5_id)
             JOIN grain_606 USING (epoch_id, d6_id)
          GROUP BY grain_206.goofy_d2_id, app_20.d3_id, app_20.d4_id, grain_515.goofy_d5_id, app_20.d8_id, app_20.d9_id, app_20.d11_id, grain_606.goofy_d6_id) a
     JOIN dim_2 USING (d2_id)
     JOIN dim_3 USING (d3_id)
     JOIN dim_4 USING (d4_id)
     JOIN dim_5 USING (d5_id)
     JOIN dim_8 USING (d8_id)
     JOIN dim_9 USING (d9_id)
     JOIN dim_11 USING (d11_id)
     JOIN dim_6 USING (d6_id);

ALTER TABLE elt.app_net_rev
    OWNER TO canopy_db_admin;

