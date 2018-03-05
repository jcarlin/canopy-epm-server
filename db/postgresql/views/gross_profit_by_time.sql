-- View: elt.gross_profit_by_time

-- DROP VIEW elt.gross_profit_by_time;

CREATE OR REPLACE VIEW elt.gross_profit_by_time AS
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
    a.revenue AS net_rev_trans_exp_balance
   FROM ( SELECT grain_209.goofy_d2_id AS d2_id,
            branch_15.d3_id,
            grain_407.goofy_d4_id AS d4_id,
            grain_516.goofy_d5_id AS d5_id,
            branch_15.d8_id,
            grain_2000.goofy_d9_id AS d9_id,
            grain_2000.goofy_d11_id AS d11_id,
            grain_607.goofy_d6_id AS d6_id,
            sum(branch_15.b15) AS revenue
           FROM branch_15
             JOIN grain_301 USING (epoch_id, d3_id)
             JOIN grain_801 USING (epoch_id, d8_id)
             JOIN grain_209 USING (epoch_id, d2_id)
             JOIN grain_407 USING (epoch_id, d4_id)
             JOIN grain_516 USING (epoch_id, d5_id)
             JOIN grain_2000 USING (epoch_id, d9_id, d11_id)
             JOIN grain_607 USING (epoch_id, d6_id)
          GROUP BY grain_209.goofy_d2_id, branch_15.d3_id, grain_407.goofy_d4_id, grain_516.goofy_d5_id, branch_15.d8_id, grain_2000.goofy_d9_id, grain_2000.goofy_d11_id, grain_607.goofy_d6_id) a
     JOIN dim_2 USING (d2_id)
     JOIN dim_3 USING (d3_id)
     JOIN dim_4 USING (d4_id)
     JOIN dim_5 USING (d5_id)
     JOIN dim_8 USING (d8_id)
     JOIN dim_9 USING (d9_id)
     JOIN dim_11 USING (d11_id)
     JOIN dim_6 USING (d6_id);

ALTER TABLE elt.gross_profit_by_time
    OWNER TO canopy_db_admin;

