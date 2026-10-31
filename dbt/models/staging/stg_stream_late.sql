select *
from read_parquet('{{ var("data_dir") }}/stream/late/*.parquet', union_by_name = true)
