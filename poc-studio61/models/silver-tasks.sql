-- tasks
SELECT
      id                                                                        AS task_id
    , team_id                                                                   AS team_id
    , space.id                                                                  AS space_id
    , project.id                                                                AS project_id
    , folder.id                                                                 AS folder_id
    , list_id                                                                   AS list_id
    , status.status                                                             AS task_status
    , COALESCE(DATETIME(TIMESTAMP_MILLIS(CAST(date_created AS INT64))), NULL)   AS task_date_created_iso
    , COALESCE(DATETIME(TIMESTAMP_MILLIS(CAST(date_updated AS INT64))), NULL)   AS task_date_updated_iso
    , COALESCE(DATETIME(TIMESTAMP_MILLIS(CAST(date_done AS INT64))), NULL)      AS task_date_done_iso
    , COALESCE(DATETIME(TIMESTAMP_MILLIS(CAST(date_closed AS INT64))), NULL)    AS task_date_closed_iso
    , COALESCE(DATETIME(TIMESTAMP_MILLIS(CAST(due_date AS INT64))), NULL)       AS task_due_date_iso
    , parent                                                                    AS task_parent_id
    , top_level_parent                                                          AS task_top_parent_id 
    , time_estimate                                                             AS task_time_estimate_ms
    , time_spent                                                                AS task_time_spent_ms
FROM 
    stech_solucoes_tecnologicas_bronze.tasks
-- WHERE 
--     id = '86afp1jfh'