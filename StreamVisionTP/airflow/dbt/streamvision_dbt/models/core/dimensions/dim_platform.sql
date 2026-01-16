/*
  Dimension: platform
*/
select distinct
    platform as platform_id,
    platform as platform_name
from {{ ref('stg_viewing_sessions') }}
