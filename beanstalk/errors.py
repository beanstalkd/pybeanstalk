class FailureError(Exception): pass
class JobError(FailureError): pass

class BeanStalkError(Exception): pass
class ProtoError(BeanStalkError): pass
class ServerError(BeanStalkError): pass

class OutOfMemory(ServerError): pass
class InternalError(ServerError): pass
class Draining(ServerError): pass

class BadFormat(ProtoError): pass
class UnknownCommand(ProtoError): pass
class ExpectedCrlf(ProtoError): pass
class JobTooBig(ProtoError): pass
class NotFound(ProtoError): pass
class NotIgnored(ProtoError): pass
class DeadlineSoon(ProtoError):pass

class UnexpectedResponse(ProtoError): pass

def checkError(linestr):
    '''Note, this will throw an error internally for every case that is a
    response that is NOT an error response, and that error will be caught,
    and CheckError will return happily.

    In the case that an error was returned by beanstalkd, an appropriate error
    will be raised'''

    try:
        errname = ''.join([x.capitalize() for x in linestr.split('_')])
        err = eval(errname)('Server returned: %s' % (linestr,))
    except Exception, e:
        return
    raise err
