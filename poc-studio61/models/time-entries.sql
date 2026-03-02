-- time entries   
SELECT
    CAST(id AS INT64)                                 AS interval_id
  , wid                                               AS team_id 
  , user.id                                           AS user_id  
  , user.username                                     AS user_name
  , task_location.space_id                            AS space_id
  , task.id                                           AS task_id 
  , start                                             AS interval_start_ms    
  , DATETIME(TIMESTAMP_MILLIS(CAST(start AS INT64)))  AS interval_start_iso
  , `end`                                             AS interval_end_ms 
  , DATETIME(TIMESTAMP_MILLIS(CAST(`end` AS INT64)))  AS interval_end_iso 
  -- , AS interval_date_added_ms
  -- , AS interval_date_added_iso 
FROM 
  stech_solucoes_tecnologicas_bronze.time_entries
-- WHERE 
--   CAST(id AS INT64) = 4968592578683065325