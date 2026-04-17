# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

cdef class FastRespParser:
    cdef bytearray _buf
    cdef Py_ssize_t _pos

    def __cinit__(self):
        self._buf = bytearray()
        self._pos = 0

    def feed(self, bytes data):
        if self._pos > 4096 and self._pos > len(self._buf) // 2:
            del self._buf[: self._pos]
            self._pos = 0
        self._buf.extend(data)

    def get_command(self):
        cdef Py_ssize_t saved
        if self._pos >= len(self._buf):
            return None
        saved = self._pos
        try:
            if self._buf[self._pos:self._pos + 1] == b"*":
                return self._read_multibulk()
            return self._read_inline()
        except ValueError:
            self._pos = saved
            return None

    cdef str _read_line(self):
        cdef Py_ssize_t idx = self._buf.find(b"\r\n", self._pos)
        cdef bytes line_bytes
        if idx == -1:
            raise ValueError("incomplete")
        line_bytes = bytes(self._buf[self._pos:idx])
        self._pos = idx + 2
        return line_bytes.decode("utf-8", "replace")

    cdef list _read_inline(self):
        cdef str line = self._read_line()
        cdef list parts = line.strip().split()
        return parts if parts else []

    cdef list _read_multibulk(self):
        cdef str line = self._read_line()
        cdef int count
        cdef int i
        cdef list result
        if not line.startswith("*"):
            raise ValueError("expected multibulk")
        count = int(line[1:])
        if count <= 0:
            return []
        result = []
        for i in range(count):
            result.append(self._read_bulk_string())
        return result

    cdef object _read_bulk_string(self):
        cdef str line = self._read_line()
        cdef int length
        cdef Py_ssize_t end
        cdef bytes value_bytes
        if not line.startswith("$"):
            raise ValueError("expected bulk string")
        length = int(line[1:])
        if length == -1:
            return None
        end = self._pos + length + 2
        if end > len(self._buf):
            raise ValueError("incomplete")
        value_bytes = bytes(self._buf[self._pos:self._pos + length])
        self._pos = end
        return value_bytes.decode("utf-8", "replace")