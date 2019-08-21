#
# Autogenerated by Thrift Compiler (0.12.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py:no_utf8strings,slots
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import logging
from .ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
all_structs = []


class Iface(object):
    def renew_leases(self, to_renew):
        """
        Parameters:
         - to_renew

        """
        pass


class Client(Iface):
    def __init__(self, iprot, oprot=None):
        self._iprot = self._oprot = iprot
        if oprot is not None:
            self._oprot = oprot
        self._seqid = 0

    def renew_leases(self, to_renew):
        """
        Parameters:
         - to_renew

        """
        self.send_renew_leases(to_renew)
        return self.recv_renew_leases()

    def send_renew_leases(self, to_renew):
        self._oprot.writeMessageBegin('renew_leases', TMessageType.CALL, self._seqid)
        args = renew_leases_args()
        args.to_renew = to_renew
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_renew_leases(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = renew_leases_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.success is not None:
            return result.success
        if result.ex is not None:
            raise result.ex
        raise TApplicationException(TApplicationException.MISSING_RESULT, "renew_leases failed: unknown result")


class Processor(Iface, TProcessor):
    def __init__(self, handler):
        self._handler = handler
        self._processMap = {}
        self._processMap["renew_leases"] = Processor.process_renew_leases

    def process(self, iprot, oprot):
        (name, type, seqid) = iprot.readMessageBegin()
        if name not in self._processMap:
            iprot.skip(TType.STRUCT)
            iprot.readMessageEnd()
            x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
            oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
            x.write(oprot)
            oprot.writeMessageEnd()
            oprot.trans.flush()
            return
        else:
            self._processMap[name](self, seqid, iprot, oprot)
        return True

    def process_renew_leases(self, seqid, iprot, oprot):
        args = renew_leases_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = renew_leases_result()
        try:
            result.success = self._handler.renew_leases(args.to_renew)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except lease_service_exception as ex:
            msg_type = TMessageType.REPLY
            result.ex = ex
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("renew_leases", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

# HELPER FUNCTIONS AND STRUCTURES


class renew_leases_args(object):
    """
    Attributes:
     - to_renew

    """

    __slots__ = (
        'to_renew',
    )


    def __init__(self, to_renew=None,):
        self.to_renew = to_renew

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.LIST:
                    self.to_renew = []
                    (_etype3, _size0) = iprot.readListBegin()
                    for _i4 in range(_size0):
                        _elem5 = iprot.readString()
                        self.to_renew.append(_elem5)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('renew_leases_args')
        if self.to_renew is not None:
            oprot.writeFieldBegin('to_renew', TType.LIST, 1)
            oprot.writeListBegin(TType.STRING, len(self.to_renew))
            for iter6 in self.to_renew:
                oprot.writeString(iter6)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, getattr(self, key))
             for key in self.__slots__]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        for attr in self.__slots__:
            my_val = getattr(self, attr)
            other_val = getattr(other, attr)
            if my_val != other_val:
                return False
        return True

    def __ne__(self, other):
        return not (self == other)
all_structs.append(renew_leases_args)
renew_leases_args.thrift_spec = (
    None,  # 0
    (1, TType.LIST, 'to_renew', (TType.STRING, None, False), None, ),  # 1
)


class renew_leases_result(object):
    """
    Attributes:
     - success
     - ex

    """

    __slots__ = (
        'success',
        'ex',
    )


    def __init__(self, success=None, ex=None,):
        self.success = success
        self.ex = ex

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 0:
                if ftype == TType.STRUCT:
                    self.success = rpc_lease_ack()
                    self.success.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 1:
                if ftype == TType.STRUCT:
                    self.ex = lease_service_exception()
                    self.ex.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('renew_leases_result')
        if self.success is not None:
            oprot.writeFieldBegin('success', TType.STRUCT, 0)
            self.success.write(oprot)
            oprot.writeFieldEnd()
        if self.ex is not None:
            oprot.writeFieldBegin('ex', TType.STRUCT, 1)
            self.ex.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, getattr(self, key))
             for key in self.__slots__]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        for attr in self.__slots__:
            my_val = getattr(self, attr)
            other_val = getattr(other, attr)
            if my_val != other_val:
                return False
        return True

    def __ne__(self, other):
        return not (self == other)
all_structs.append(renew_leases_result)
renew_leases_result.thrift_spec = (
    (0, TType.STRUCT, 'success', [rpc_lease_ack, None], None, ),  # 0
    (1, TType.STRUCT, 'ex', [lease_service_exception, None], None, ),  # 1
)
fix_spec(all_structs)
del all_structs

