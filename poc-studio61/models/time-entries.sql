-- time entries   
SELECT
      CAST(id AS INT64)                                                 AS interval_id
    , wid                                                               AS team_id 
    , user.id                                                           AS user_id  
    , user.username                                                     AS user_name
    , task_location.space_id                                            AS space_id
    , task.id                                                           AS task_id 
    , start                                                             AS interval_date_start_ms    
    , COALESCE(DATETIME(TIMESTAMP_MILLIS(CAST(start AS INT64))), NULL)  AS interval_date_start_iso
    , COALESCE(`end`                                           , NULL)  AS interval_date_end_ms 
    , COALESCE(DATETIME(TIMESTAMP_MILLIS(CAST(`end` AS INT64))), NULL)  AS interval_date_end_iso 
    , COALESCE(`at`                                            , NULL)  AS interval_date_added_ms
    , COALESCE(DATETIME(TIMESTAMP_MILLIS(CAST(`at` AS INT64))), NULL)   AS interval_date_added_iso 
FROM 
    stech_solucoes_tecnologicas_bronze.time_entries
WHERE 
    task.id = '86afp1jfh'