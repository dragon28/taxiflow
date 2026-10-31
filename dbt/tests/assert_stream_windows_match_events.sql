-- Windows must account for every deduplicated on-time event.
select 1
having (select coalesce(sum(trips), 0) from {{ ref('mart_stream_windows') }})
    <> (select count(*) from {{ ref('stg_stream_events') }})
