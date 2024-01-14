docker run -v "$PWD:/parquet_parser" thrift thrift -o /parquet_parser/src --gen py /parquet_parser/thrift/parquet.thrift


