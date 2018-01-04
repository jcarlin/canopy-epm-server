--CREATE EXTENSION IF NOT EXISTS hstore SCHEMA pg_catalog;  
--DROP EXTENSION hstore CASCADE; 
SET search_path TO 'elt';
-- DROP FUNCTION elt.cx_upsert(HSTORE) 
CREATE OR REPLACE FUNCTION elt.cx_upsert(hix HSTORE) 
RETURNS hstore AS 
$$

/* --entire process:  01:39:24 hours  index last( ~58:00 (added) | 43:57
Loads data from Foreign data sources
http://

Ver:
  1.0.0  - 18.01.02 - AJD - First!

Notes:
  hstore in: 

SET search_path TO 'elt';
*/ 


DECLARE
  st_fxn TEXT DEFAULT 'cx_upsert'; --Calling fxn
  sb_msg BOOLEAN DEFAULT CASE WHEN coalesce(hix->'p_msg','off') = 'on' THEN true ELSE false END;
  st_msg TEXT DEFAULT st_fxn||': Begin';
  qcx TEXT DEFAULT '--'||st_fxn||E': SQL to create system, dim and origin tables:\n';
  st_make TEXT DEFAULT '--'||st_fxn||E': SQL to make blocks, bricks, root and calc tables:\n';
  st_load TEXT DEFAULT '--'||st_fxn||E': Load commands:\n';
  sd_fxn_start_time TIME ; --SELECT current_time::TIME
  sd_begin TIME; --SELECT current_time::TIME
  si_pad INTEGER DEFAULT 75; --SELECT current_time::TIME
  wt_msg TEXT;
  wt_fact TEXT;
  wt_hier TEXT;
  wt_node TEXT;
  tt_hier TEXT;
  wt_dim TEXT;
  tt_dim TEXT;
  wtx TEXT;
  ttx TEXT;
  hsx HSTORE DEFAULT ''::HSTORE;  --System hstore 
  hx HSTORE DEFAULT ''::HSTORE; 
  htx HSTORE DEFAULT ''::HSTORE; 
  hm HSTORE DEFAULT hstore('m_fxn',st_fxn);
  hox HSTORE DEFAULT ''::HSTORE; 
  qtx TEXT;  --temp SQL
  qwx TEXT;  --working SQL
  qx TEXT;  --SQL
  sx TEXT DEFAULT ''; --indents output to message window
  ix INTEGER; 

  wBx BOOLEAN; 
  wrx RECORD; 
  trx RECORD; 
  trx2 RECORD; 
    
BEGIN
--   RAISE INFO '%',(SELECT current_time)::time;
--   RAISE INFO '%',(SELECT timeofday());
  sd_fxn_start_time:=timeofday()::timestamp::time;
  PERFORM set_config('search_path', 'elt, public', true); --applies to current transaction 

  --******************************
  --Inital Set up
  --******************************
  --Parse the incoming keys
  hx:=hx||hstore('skip_execute',coalesce(hix->'skip_execute','off'));
--   SELECT fact_id FROM s_fact WHERE fact_name = 'bb_nonsal_expense';
  IF hix ? 'root_name' THEN
    execute 'SELECT fact_id FROM s_fact WHERE fact_name = '''||(hix->'root_name')||'''' INTO ttx;
  END IF;
  hix:=hix||hstore('root_id',coalesce(hix->'root_id',ttx));


  --******************************
  --Notes
  --  We are going to build two queries:
  --  1. Inactivate the old value and insert the new value
  --  2. Propagate the change throught the matrix (which is only one table called app_20
  --******************************
  st_msg:=st_fxn||': Here are the parsed parameters, about to create a server to: '||(hx->'host')||'';
  IF sb_msg THEN perform cx_fxn(hm||hx||'m_style=>hstore'||hstore('m_msg', st_msg)); END IF;

/*

SELECT *
FROM (SELECT unnest(fact_key) AS dim_id FROM s_fact WHERE fact_id = 5) a
NATURAL JOIN s_dim ORDER BY dim_byte, dim_id;

select * from s_fact
*/
  -- There should be n number of filters where n is the number of fact_keys in the fact table SELECT * FROM s_fact
  hx:=hx||hstore('where','');  --Stores the where clause to find the key
  hx:=hx||hstore('select','');  --Stores the select/group by clause on the key
  --Loop over this root tables dim key
  qtx:=     E'\n'||'SELECT *';  
  qtx:=qtx||E'\n'||'FROM (SELECT unnest(fact_key) AS dim_id FROM s_fact WHERE fact_id = '||(hix->'root_id')||') a';
  qtx:=qtx||E'\n'||'NATURAL JOIN s_dim ORDER BY dim_byte, dim_id;';
  IF sb_msg THEN PERFORM cx_fxn(hstore('m_msg',E'Loop over this root tables dim key:\n'||qtx)); END IF;
  wbx:=CASE WHEN hix ? 'd2_id' THEN false ELSE true END; --Where member id's passed?
  FOR wrx IN execute qtx LOOP
    -- Build some SQL to be used later
    IF wbx THEN --Only run if id's were not passed
      execute 'SELECT d'||wrx.dim_id||'_id FROM dim_'||wrx.dim_id||' WHERE d'||wrx.dim_id||'_name = '''||(hix->wrx.dim_name)||'''' INTO ttx;
      hix:=hix||hstore('d'||wrx.dim_id||'_id',ttx);  --Stores the where clause to find the key
    END IF;
    hx:=hx||hstore('where',(hx->'where')||' AND d'||wrx.dim_id||'_id = '||(hix->('d'||wrx.dim_id||'_id'))||'');  --Stores the where clause to find the key
    hx:=hx||hstore('select',(hx->'select')||'d'||wrx.dim_id||'_id,');  --Stores the group by clause on the key
  END LOOP;
  hx:=hx||hstore('select',left(hx->'select',-1));  --Drop the last comma

  IF sb_msg THEN perform cx_fxn(hm||hix||hx||'m_style=>hstore'||hstore('m_msg', E'hix and hx\n')); END IF;

/*
WITH
  inactive AS (
    UPDATE root_5 u
    SET active = false
    WHERE active = true AND d2_id = 4 AND d3_id = 5 AND d4_id = 52 AND d5_id = 1559 AND d8_id = 4 AND d9_id = 204 AND d11_id = 7 AND d6_id = 1495
    RETURNING d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id
    )
INSERT INTO root_5 (d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,r5)
SELECT *,'140.651153986187'::DOUBLE PRECISION FROM inactive
;
*/

  --Build Query to 1. Inactivate the old value and insert the new value

  --Build the main query
  qwx:=    E'\n'||'';  
  qwx:=qwx||E'\n'||'WITH';
  --First: make the existing record inactive
  qwx:=qwx||E'\n'||'  inactive AS (';
  qwx:=qwx||E'\n'||'    UPDATE root_'||(hix->'root_id')||' u';
  qwx:=qwx||E'\n'||'    SET active = false';
  qwx:=qwx||E'\n'||'    WHERE active = true'||(hx->'where')||'';
  qwx:=qwx||E'\n'||'    RETURNING '||(hx->'select')||'';
  qwx:=qwx||E'\n'||'    )';
  --Second: Update the new record
  qwx:=qwx||E'\n'||'INSERT INTO root_'||(hix->'root_id')||' ('||(hx->'select')||',r'||(hix->'root_id')||')';
--   qwx:=qwx||E'\n'||'SELECT *,''140.651153986187''::DOUBLE PRECISION FROM inactive';
  qwx:=qwx||E'\n'||'SELECT *,'''||(hix->'value')||'''::DOUBLE PRECISION FROM inactive';
  qwx:=qwx||E'\n'||';';

  IF sb_msg THEN PERFORM cx_fxn(hstore('m_msg',E'1. Inactivate the old value and insert the new value:\n'||qwx)); END IF;
  IF hx->'skip_execute' = 'off' THEN 
    EXECUTE qwx;
  END IF;

/*
WITH update_a20 AS (
  UPDATE app_20 u
  SET a20 = new_a20
  FROM (
      --elt is modified to add the keystone
      WITH keystone AS (
        WITH this_key AS (SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id FROM root_5 WHERE active = true AND d2_id = 4 AND d3_id = 5 AND d4_id = 52 AND d5_id = 1559 AND d8_id = 4 AND d9_id = 204 AND d11_id = 7 AND d6_id = 1495)
        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id
        FROM (  --Shape updated values and grab the goofy value only for grains that have them
          SELECT d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
          FROM this_key
            NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
            NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        ) a
        --The goofy values will naturally join, inversing the grain to show the impacted leaves and this is scoped down against the granite
        NATURAL JOIN granite_x1 --all dims in all roots
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
      --Since the grain could map a leaf to more than one value, we need to make the set unique
      GROUP BY d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id
      ),
      root_1 AS ( 
        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r1) AS r1 
        FROM root_1
        NATURAL JOIN keystone  --all roots need to hit the keystone first
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        WHERE active
        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
      ),
      root_2 AS ( 
        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r2) AS r2
        FROM root_2
        NATURAL JOIN keystone
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        WHERE active
        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
      ),
      root_3 AS ( 
        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r3) AS r3
        FROM root_3
        NATURAL JOIN keystone
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        WHERE active
        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
      ),
      root_4 AS ( 
        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r4) AS r4
        FROM root_4
        NATURAL JOIN keystone
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        WHERE active
        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
      ),
      root_5 AS ( 
        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r5) AS r5
        FROM root_5
        NATURAL JOIN keystone
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        WHERE active
        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
      ),
      root_6 AS ( 
        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r6) AS r6
        FROM root_6
        NATURAL JOIN keystone
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        WHERE active
        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
      ),
      root_7 AS ( 
        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r7) AS r7
        FROM root_7
        NATURAL JOIN keystone
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        WHERE active
        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
      ),
      root_8 AS ( 
        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r8) AS r8
        FROM root_8
        NATURAL JOIN keystone
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        WHERE active
        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
      ),
      root_9 AS ( 
        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r9) AS r9
        FROM root_9
        NATURAL JOIN keystone
        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13
        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy
        WHERE active
        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id
      )
      SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,coalesce(r1,0) + coalesce(r2,0) + coalesce(r3,0) + coalesce(r4,0) - coalesce(r5,0) - coalesce(r6,0) - coalesce(r7,0) - coalesce(r8,0) + coalesce(r9,0) AS new_a20
      FROM root_1
      NATURAL FULL OUTER JOIN root_2
      NATURAL FULL OUTER JOIN root_3
      NATURAL FULL OUTER JOIN root_4
      NATURAL FULL OUTER JOIN root_5
      NATURAL FULL OUTER JOIN root_6
      NATURAL FULL OUTER JOIN root_7
      NATURAL FULL OUTER JOIN root_8
      NATURAL FULL OUTER JOIN root_9
   ) a
  WHERE u.d2_id = a.d2_id
    AND u.d3_id = a.d3_id
    AND u.d4_id = a.d4_id
    AND u.d5_id = a.d5_id
    AND u.d8_id = a.d8_id
    AND u.d9_id = a.d9_id
    AND u.d11_id = a.d11_id
    AND u.d6_id = a.d6_id
  RETURNING u.*
  )
SELECT dim_6.d6_name AS "Org Node", dim_2.d2_name AS "Award", dim_5.d5_name AS "Object Code", d5_desc AS Description, to_char(trunc(rev),  'L999,999,999,999') AS rev
FROM (SELECT min(gr515_oid) AS gr515_oid, goofy_d5_id AS d5_id FROM grain_515 GROUP BY goofy_d5_id) grain_515
NATURAL JOIN dim_5
NATURAL JOIN (
  SELECT d2_id,d3_id,d4_id,goofy_d5_id AS d5_id,d8_id,d9_id,d11_id,d6_id,sum(a20) AS rev
  FROM update_a20 -- app_20
  NATURAL JOIN grain_515 --5 replicates object code to report layout
  GROUP BY d2_id,d3_id,d4_id,goofy_d5_id,d8_id,d9_id,d11_id,d6_id
) a  
NATURAL JOIN dim_6
NATURAL JOIN dim_2
ORDER BY gr515_oid, d2h7_oid
;

*/
  --Build Query to 2. Propagate the change throught the matrix (which is only one table called app_20 (The result of this could feed the status window)

  --Build the main query
  qwx:=    E'\n'||'';  
  qwx:=qwx||E'\n'||'WITH update_a20 AS (';
  qwx:=qwx||E'\n'||'  UPDATE app_20 u';
  qwx:=qwx||E'\n'||'  SET a20 = new_a20';
  qwx:=qwx||E'\n'||'  FROM (';
  qwx:=qwx||E'\n'||'      --elt is modified to add the keystone';
  qwx:=qwx||E'\n'||'      WITH keystone AS (';

  --We don't use root_5's key, we use the key for app_20 and that is hardcoded for now.  
  qwx:=qwx||E'\n'||'        WITH this_key AS (SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id FROM root_'||(hix->'root_id')||' WHERE active = true'||(hx->'where')||')';
  qwx:=qwx||E'\n'||'        SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'        FROM (  --Shape updated values and grab the goofy value only for grains that have them';
  qwx:=qwx||E'\n'||'          SELECT d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'          FROM this_key';
  qwx:=qwx||E'\n'||'            NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'            NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'        ) a';
  qwx:=qwx||E'\n'||'        --The goofy values will naturally join, inversing the grain to show the impacted leaves and this is scoped down against the granite';
  qwx:=qwx||E'\n'||'        NATURAL JOIN granite_x1 --all dims in all roots';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'      --Since the grain could map a leaf to more than one value, we need to make the set unique';
  qwx:=qwx||E'\n'||'      GROUP BY d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'      ),';
  qwx:=qwx||E'\n'||'      root_1 AS ( ';
  qwx:=qwx||E'\n'||'        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r1) AS r1 ';
  qwx:=qwx||E'\n'||'        FROM root_1';
  qwx:=qwx||E'\n'||'        NATURAL JOIN keystone  --all roots need to hit the keystone first';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'        WHERE active';
  qwx:=qwx||E'\n'||'        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'      ),';
  qwx:=qwx||E'\n'||'      root_2 AS ( ';
  qwx:=qwx||E'\n'||'        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r2) AS r2';
  qwx:=qwx||E'\n'||'        FROM root_2';
  qwx:=qwx||E'\n'||'        NATURAL JOIN keystone';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'        WHERE active';
  qwx:=qwx||E'\n'||'        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'      ),';
  qwx:=qwx||E'\n'||'      root_3 AS ( ';
  qwx:=qwx||E'\n'||'        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r3) AS r3';
  qwx:=qwx||E'\n'||'        FROM root_3';
  qwx:=qwx||E'\n'||'        NATURAL JOIN keystone';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'        WHERE active';
  qwx:=qwx||E'\n'||'        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'      ),';
  qwx:=qwx||E'\n'||'      root_4 AS ( ';
  qwx:=qwx||E'\n'||'        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r4) AS r4';
  qwx:=qwx||E'\n'||'        FROM root_4';
  qwx:=qwx||E'\n'||'        NATURAL JOIN keystone';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'        WHERE active';
  qwx:=qwx||E'\n'||'        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'      ),';
  qwx:=qwx||E'\n'||'      root_5 AS ( ';
  qwx:=qwx||E'\n'||'        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r5) AS r5';
  qwx:=qwx||E'\n'||'        FROM root_5';
  qwx:=qwx||E'\n'||'        NATURAL JOIN keystone';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'        WHERE active';
  qwx:=qwx||E'\n'||'        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'      ),';
  qwx:=qwx||E'\n'||'      root_6 AS ( ';
  qwx:=qwx||E'\n'||'        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r6) AS r6';
  qwx:=qwx||E'\n'||'        FROM root_6';
  qwx:=qwx||E'\n'||'        NATURAL JOIN keystone';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'        WHERE active';
  qwx:=qwx||E'\n'||'        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'      ),';
  qwx:=qwx||E'\n'||'      root_7 AS ( ';
  qwx:=qwx||E'\n'||'        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r7) AS r7';
  qwx:=qwx||E'\n'||'        FROM root_7';
  qwx:=qwx||E'\n'||'        NATURAL JOIN keystone';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'        WHERE active';
  qwx:=qwx||E'\n'||'        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'      ),';
  qwx:=qwx||E'\n'||'      root_8 AS ( ';
  qwx:=qwx||E'\n'||'        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r8) AS r8';
  qwx:=qwx||E'\n'||'        FROM root_8';
  qwx:=qwx||E'\n'||'        NATURAL JOIN keystone';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'        WHERE active';
  qwx:=qwx||E'\n'||'        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'      ),';
  qwx:=qwx||E'\n'||'      root_9 AS ( ';
  qwx:=qwx||E'\n'||'        SELECT d2_id,d3_id,goofy_d4_id AS d4_id,d5_id,d8_id,goofy_d9_id AS d9_id,d11_id,d6_id,sum(r9) AS r9';
  qwx:=qwx||E'\n'||'        FROM root_9';
  qwx:=qwx||E'\n'||'        NATURAL JOIN keystone';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_405 --d4 dats_shaper --assigned 13';
  qwx:=qwx||E'\n'||'        NATURAL JOIN grain_908 --d9 time_shaper -- 174;2016_fy -- 192;2017_fy -- 209;2018_fy';
  qwx:=qwx||E'\n'||'        WHERE active';
  qwx:=qwx||E'\n'||'        GROUP BY d2_id,d3_id,goofy_d4_id,d5_id,d8_id,goofy_d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||'      )';
  qwx:=qwx||E'\n'||'      SELECT d2_id,d3_id,d4_id,d5_id,d8_id,d9_id,d11_id,d6_id,coalesce(r1,0) + coalesce(r2,0) + coalesce(r3,0) + coalesce(r4,0) - coalesce(r5,0) - coalesce(r6,0) - coalesce(r7,0) - coalesce(r8,0) + coalesce(r9,0) AS new_a20';
  qwx:=qwx||E'\n'||'      FROM root_1';
  qwx:=qwx||E'\n'||'      NATURAL FULL OUTER JOIN root_2';
  qwx:=qwx||E'\n'||'      NATURAL FULL OUTER JOIN root_3';
  qwx:=qwx||E'\n'||'      NATURAL FULL OUTER JOIN root_4';
  qwx:=qwx||E'\n'||'      NATURAL FULL OUTER JOIN root_5';
  qwx:=qwx||E'\n'||'      NATURAL FULL OUTER JOIN root_6';
  qwx:=qwx||E'\n'||'      NATURAL FULL OUTER JOIN root_7';
  qwx:=qwx||E'\n'||'      NATURAL FULL OUTER JOIN root_8';
  qwx:=qwx||E'\n'||'      NATURAL FULL OUTER JOIN root_9';
  qwx:=qwx||E'\n'||'   ) a';
  qwx:=qwx||E'\n'||'  WHERE u.d2_id = a.d2_id';
  qwx:=qwx||E'\n'||'    AND u.d3_id = a.d3_id';
  qwx:=qwx||E'\n'||'    AND u.d4_id = a.d4_id';
  qwx:=qwx||E'\n'||'    AND u.d5_id = a.d5_id';
  qwx:=qwx||E'\n'||'    AND u.d8_id = a.d8_id';
  qwx:=qwx||E'\n'||'    AND u.d9_id = a.d9_id';
  qwx:=qwx||E'\n'||'    AND u.d11_id = a.d11_id';
  qwx:=qwx||E'\n'||'    AND u.d6_id = a.d6_id';
  qwx:=qwx||E'\n'||'  RETURNING u.*';
  qwx:=qwx||E'\n'||'  )';
  qwx:=qwx||E'\n'||'SELECT dim_6.d6_name AS "Org Node", dim_2.d2_name AS "Award", dim_5.d5_name AS "Object Code", d5_desc AS Description, to_char(trunc(rev),  ''L999,999,999,999'') AS rev';
  qwx:=qwx||E'\n'||'FROM (SELECT min(gr515_oid) AS gr515_oid, goofy_d5_id AS d5_id FROM grain_515 GROUP BY goofy_d5_id) grain_515';
  qwx:=qwx||E'\n'||'NATURAL JOIN dim_5';
  qwx:=qwx||E'\n'||'NATURAL JOIN (';
  qwx:=qwx||E'\n'||'  SELECT d2_id,d3_id,d4_id,goofy_d5_id AS d5_id,d8_id,d9_id,d11_id,d6_id,sum(a20) AS rev';
  qwx:=qwx||E'\n'||'  FROM update_a20 -- app_20';
  qwx:=qwx||E'\n'||'  NATURAL JOIN grain_515 --5 replicates object code to report layout';
  qwx:=qwx||E'\n'||'  GROUP BY d2_id,d3_id,d4_id,goofy_d5_id,d8_id,d9_id,d11_id,d6_id';
  qwx:=qwx||E'\n'||') a  ';
  qwx:=qwx||E'\n'||'NATURAL JOIN dim_6';
  qwx:=qwx||E'\n'||'NATURAL JOIN dim_2';
  qwx:=qwx||E'\n'||'ORDER BY gr515_oid, d2h7_oid';
  qwx:=qwx||E'\n'||';';

--   qwx:=    E'\n'||'';  
--   qwx:=qwx||E'\n'||'WITH';
--   qwx:=qwx||E'\n'||';';

  IF sb_msg THEN PERFORM cx_fxn(hstore('m_msg',E'2. Propagate the change throught the matrix (which is only one table called app_20:\n'||qwx)); END IF;
  IF hx->'skip_execute' = 'off' THEN 
    EXECUTE qwx;
  END IF;



--   perform cx_fxn(hm||hx||'m_style=>hstore');




  hox:=hix;


  RETURN(hox);

END;
$$ 
LANGUAGE plpgsql;


