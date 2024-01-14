import struct
import os

class ByteReader:
    def __init__(self,path) -> None:
        self.path = path
        self.file = open(self.path, "br")
        self.is_open = True

    def get_file(self):
        return self.file

    def read(self,n: int) -> bytes:
        if n > 0:
            return self.read_normal(n)
        return self.read_backwards(n)

    def read_int(self, n: int, byteorder: str = 'little') -> int:
        return int.from_bytes(self.read(n), byteorder=byteorder)

    def read_int32(self) -> int:
        return self.read_int(4)

    def read_int64(self) -> int:
        return self.read_int(8)

    def read_float4(self) -> float:
        [f] = struct.unpack('f', self.read(4))
        return f

    def read_float8(self) -> float:
        [f] = struct.unpack('f', self.read(8))
        return f
        
    def read_backwards(self, n: int) -> bytes:
        self.file.seek(n, 1)
        bts = self.file.read(n * -1)
        self.file.seek(n, 1)
        return bts

    def read_normal(self, n: int) -> bytes:
        return self.file.read(n)
        
    def move_to_position_from_start(self, pos: int) -> None:
        self.file.seek(pos,0)

    def move_to_position_from_end(self, pos: int) -> None:
        self.file.seek(pos,os.SEEK_END)

    def move_to_position_relative(self, pos: int) -> None:
        self.file.seek(pos,os.SEEK_CUR)

    def close(self) -> None:
        self.is_open = False
        self.file.close()

    def get_size(self) -> int:
        return os.fstat(self.file.fileno()).st_size

    def get_position(self) -> int:
        return self.file.tell()

    ## To pass as a Transport for thrift compact protocol
    ## satisfy the interface
    def isOpen(self):
        return self.is_open

    def open(self):
        pass

    def readAll(self, sz):
        buff = ''
        have = 0
        while (have < sz):
            chunk = self.read(sz - have)
            have += len(chunk)
            buff += chunk

            if len(chunk) == 0:
                raise EOFError()

        return buff

    def write(self, buf):
        pass

    def flush(self):
        pass

