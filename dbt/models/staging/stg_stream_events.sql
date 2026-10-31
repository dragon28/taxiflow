-- Events persisted by the stream consumer (Parquet, partitioned by event_date).
-- The consumer is at-least-once; duplicates across restarts are removed here.
select * exclude (rn)
from (
    select
        *,
        row_number() over (partition by event_id order by source_offset) as rn
    from read_parquet('{{ var("data_dir") }}/stream/events/**/*.parquet', hive_partitioning = true)
)
where rn = 1
