-- View: elt.sales_revenue_by_product

-- DROP VIEW elt.sales_revenue_by_product;

CREATE OR REPLACE VIEW elt.sales_revenue_by_product AS
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
    a.a30 AS bb_revenue
   FROM ( SELECT grain_208.goofy_d2_id AS d2_id,
            root_3.d3_id,
            root_3.d4_id,
            root_3.d5_id,
            grain_607.goofy_d6_id AS d6_id,
            root_3.d8_id,
            root_3.d9_id,
            root_3.d11_id,
            sum(root_3.r3) AS a30
           FROM root_3
             JOIN grain_208 USING (d2_id)
             JOIN grain_607 USING (d6_id, epoch_id)
             JOIN grain_903 USING (epoch_id, d9_id)
          WHERE root_3.active AND root_3.d3_id = 5 AND root_3.d4_id = 52 AND root_3.d5_id = 461 AND root_3.d8_id = 4 AND root_3.d11_id = 7
          GROUP BY grain_208.goofy_d2_id, root_3.d3_id, root_3.d4_id, root_3.d5_id, grain_607.goofy_d6_id, root_3.d8_id, root_3.d9_id, root_3.d11_id) a
     JOIN dim_2 USING (d2_id)
     JOIN dim_3 USING (d3_id)
     JOIN dim_4 USING (d4_id)
     JOIN dim_5 USING (d5_id)
     JOIN dim_8 USING (d8_id)
     JOIN dim_9 USING (d9_id)
     JOIN dim_11 USING (d11_id)
     JOIN dim_6 USING (d6_id);

ALTER TABLE elt.sales_revenue_by_product
    OWNER TO canopy_db_admin;

