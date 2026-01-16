/*
  Fact: ratings
*/
select
    r.rating_id,
    r.user_id,
    r.content_id,
    date_trunc('day', r.rated_at)::date as rating_date,
    r.rating_value
from {{ ref('stg_ratings') }} r
