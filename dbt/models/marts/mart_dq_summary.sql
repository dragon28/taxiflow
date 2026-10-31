select source_file, coalesce(dq_reason, 'valid') as outcome, count(*) as rows
from {{ ref('stg_yellow_trips') }}
group by all
order by source_file, rows desc
