-- View: gross_profit_by_time

-- DROP VIEW gross_profit_by_time;

CREATE OR REPLACE VIEW gross_profit_by_time AS 
 SELECT r.d2_id,
    dim_2.d2_name AS product_id,
    r.d3_id,
    dim_3.d3_name AS currency_id,
    r.d4_id,
    dim_4.d4_name AS datasrc_id,
    r.d5_id,
    dim_5.d5_name AS objectcode_id,
    r.d8_id,
    dim_8.d8_name AS scenario_id,
    r.d9_id,
    dim_9.d9_name AS time_id,
    r.d11_id,
    dim_11.d11_name AS variation_id,
    r.d6_id,
    dim_6.d6_name AS organization_id,
    r.net_rev_trans_exp_balance
   FROM ( SELECT g2.goofy_d2_id AS d2_id,
            r_1.d3_id,
            g4.goofy_d4_id AS d4_id,
            g5.goofy_d5_id AS d5_id,
            g9.goofy_d9_id AS d9_id,
            g6.goofy_d6_id AS d6_id,
            r_1.d8_id,
            r_1.d11_id,
            sum(r_1.b15) AS net_rev_trans_exp_balance
           FROM branch_15 r_1
             JOIN grain_207 g2 ON r_1.d2_id = g2.d2_id
             JOIN grain_407 g4 ON r_1.d4_id = g4.d4_id
             JOIN grain_516 g5 ON r_1.d5_id = g5.d5_id
             JOIN grain_909 g9 ON r_1.d9_id = g9.d9_id
             JOIN grain_607 g6 ON r_1.d6_id = g6.d6_id
          WHERE r_1.d3_id = 5 AND (r_1.d8_id = ANY (ARRAY[3, 4]))
          GROUP BY g2.goofy_d2_id, r_1.d3_id, g4.goofy_d4_id, g5.goofy_d5_id, g9.goofy_d9_id, g6.goofy_d6_id, r_1.d8_id, r_1.d11_id) r
     JOIN dim_2 ON r.d2_id = dim_2.d2_id
     JOIN dim_3 ON r.d3_id = dim_3.d3_id
     JOIN dim_4 ON r.d4_id = dim_4.d4_id
     JOIN dim_5 ON r.d5_id = dim_5.d5_id
     JOIN dim_9 ON r.d9_id = dim_9.d9_id
     JOIN dim_6 ON r.d6_id = dim_6.d6_id
     JOIN dim_8 ON r.d8_id = dim_8.d8_id
     JOIN dim_11 ON r.d11_id = dim_11.d11_id;

ALTER TABLE gross_profit_by_time
  OWNER TO canopy_db_admin;
