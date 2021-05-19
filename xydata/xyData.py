import platform
import ctypes
import os.path

if platform.system() == 'Linux':
    try:
        dll = ctypes.CDLL(os.path.dirname(__file__) + '/XYData.so')
    except:
        dll = ctypes.CDLL('XYData.so')
else:
    try:
        dll = ctypes.WinDLL(os.path.dirname(__file__) + '\\XYData.dll')
    except:
        dll = ctypes.WinDLL('XYData.dll')

dll.XYGetVersion.restype = ctypes.c_char_p
dll.XYNew.restype = ctypes.c_void_p
dll.XYGetData.restype = ctypes.c_void_p
dll.XYGetDataSize.restype = ctypes.c_size_t

EC_OK       = 0
EC_INIT     = 1
EC_DATA     = 2
EC_FATAL    = 3

class InitError(Exception):
    pass

class FatalError(Exception):
    pass

class XYData:
    def __init__(self, path):
        h = dll.XYNew(bytes(path, 'utf-8'))
        self.handle = ctypes.c_void_p(h)
        self.buff = None
        self.size = 0

    def __del__(self):
        dll.XYDelete(self.handle)

    def Version():
        return dll.XYGetVersion().decode('utf-8')

    def Load(self, type, code, date):
        rc = dll.XYRead(
            self.handle,
            bytes(type, 'utf-8'),
            bytes(code, 'utf-8'),
            int(date))
        if rc == EC_OK:
            self.buff = dll.XYGetData(self.handle)
            self.size = dll.XYGetDataSize(self.handle)
            return self.size
        elif rc == EC_INIT:
            raise InitError
        elif rc == EC_DATA:
            self.buff = None
            self.size = 0
            return 0
        else:
            raise FatalError

    def Get(self, begin, end):
        if self.buff == None:
            return None
        if 0 <= begin and begin <= end and end <= self.size:
            return ctypes.string_at(self.buff + begin, end - begin)
        else:
            return None

    def Read(self, type, code, date):
        size = self.Load(type, code, date)
        if size > 0:
            return self.Get(0, size)
        else:
            return None

    def Query(self, type, code, date):
        rc = dll.XYQuery(
            self.handle,
            bytes(type, 'utf-8'),
            bytes(code, 'utf-8'),
            int(date))
        if rc == EC_OK:
            return True
        elif rc == EC_INIT:
            raise InitError
        elif rc == EC_DATA:
            return False
        else:
            raise FatalError
