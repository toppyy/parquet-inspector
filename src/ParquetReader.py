import src.gen_py.parquet.ttypes    as ThriftTypes
from thrift.transport               import TTransport
from thrift.protocol                import TCompactProtocol
from src.ByteReader                 import ByteReader

thrift_classes = (
    ThriftTypes.Type,
	ThriftTypes.ConvertedType,
	ThriftTypes.FieldRepetitionType,
	ThriftTypes.Encoding,
	ThriftTypes.CompressionCodec,
	ThriftTypes.PageType,
	ThriftTypes.Statistics,
	ThriftTypes.SchemaElement,
	ThriftTypes.DataPageHeader,
	ThriftTypes.IndexPageHeader,
	ThriftTypes.DictionaryPageHeader,
	ThriftTypes.DataPageHeaderV2,
	ThriftTypes.PageHeader,
	ThriftTypes.KeyValue,
	ThriftTypes.SortingColumn,
	ThriftTypes.PageEncodingStats,
	ThriftTypes.ColumnMetaData,
	ThriftTypes.ColumnChunk,
	ThriftTypes.RowGroup,
	ThriftTypes.FileMetaData
)

class ParquetReader:

    def __init__(self, path) -> None:

        self.path = path
        self.br = ByteReader(path)
        self.read_metadata()

    def read_metadata(self) -> None:

        # Read magic + metadata size from footer
        self.br.move_to_position_from_end(0)
        magic       = self.br.read(-4)
        footer_size = self.br.read_int(-4)
        metadata_pos = self.br.get_size() - (footer_size + 8)
        self.br.move_to_position_from_start(metadata_pos )

        # Deserialize metadata (serialized with Thrift Compact Protocol )
        trans = TTransport.TFileObjectTransport(self.br)
        proto = TCompactProtocol.TCompactProtocol(trans)
        self.metadata =  ThriftTypes.FileMetaData()
        self.metadata.read(proto)


    def print_records(self, n: int) -> None:

        for rowgroup in self.metadata.row_groups:
            for column_chunk in rowgroup.columns:
                hdr =  self.read_page_header(column_chunk.meta_data.data_page_offset)
                values = hdr.data_page_header.num_values
                dtype = column_chunk.meta_data.type

                # Skip repetition and def. level
                # cannot handle nested data
                size = self.br.read_int(4)
                self.br.move_to_position_relative(size)

                col_name = str(column_chunk.meta_data.path_in_schema[0],'utf-8')
                print('\n')
                print(col_name)
                for value in self.value_reader(n,values,dtype):
                    print(value)

        print('\n')

    def get_encoder(self, dtype: int):
        # ONLY PLAIN ENCODING
        if dtype == 1:
            return self.br.read_int32

        if dtype == 2:
            return self.br.read_int64

        if dtype == 4:
            return self.br.read_float4

        if dtype == 5:
            return self.br.read_float8

        if dtype == 6:
            return self.read_bytearray

        raise Exception(f"Unknown type {dtype} to get_reader_per_type")

    def read_bytearray(self):
        length = self.br.read_int(4)
        return self.br.read(length)

    def value_reader(self,n: int ,value_count: int, dtype: int):
        rdr = self.get_encoder(dtype)
        if n is None:
            n = value_count
        while n > 0 and n < value_count:
            yield rdr()
            n -= 1

    def read_page_header(self, page_offset: int) -> ThriftTypes.PageHeader:
        self.br.move_to_position_from_start(page_offset)
        trans = TTransport.TFileObjectTransport(self.br)
        proto = TCompactProtocol.TCompactProtocol(trans)
        hdr = ThriftTypes.PageHeader()
        hdr.read(proto)
        return hdr

    def print_metadata(self):
        # Prints metadata as is
        md = self.metadata.__dict__
        for key,value in md.items():
            print(key,value)
            print('--')

    def indent(self,by: int) -> None:
         print('\t' * by, end = '')

    def print_object(self, obj, key=None, depth = 0):

        self.indent(depth)

        if type(obj) == list:
            if key is not None:
                print(f'{key}')
            for v in obj:
                self.print_object(v, None, depth + 1)
            return

        
        if isinstance(obj,thrift_classes):        
            print(type(obj).__name__)
            md = obj.__dict__
            for key,value in md.items():
                self.print_object(value, key, depth + 1)
            return 
        
        if key is not None:
            print(f'{key}: {str(obj)}')
        else:
            print(f'{str(obj)}')

    def print_info(self):
        self.print_object(self.metadata)
        print('\n')
