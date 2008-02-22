from nose import with_setup, tools
from beanstalk import protohandler
from beanstalk import errors

proto = protohandler.Proto()

def check_result(result, **kw):
    try:
        tstatus = kw.pop('status')
    except:
        tstatus = 'd'

    tres = {'state':'ok'}
    tres.update(kw)

    # result of handle call comes as (state, resdict)
    status, res = result

    print 'status is %s, tstatus is %s' % (status, tstatus)
    assert status == tstatus
    # dont need the parse_data function other than to know its there
    res.pop('parse_data')
    assert res == tres

def test_put():
    line, handler = proto.process_put('test data',0,0,0)
    t ="put 0 0 0 %s\r\ntest data\r\n" % (len('test data'),)
    assert line == t
    check_result(handler("INSERTED 3"), jid=3)
    check_result(handler("BURIED 3"), jid=3, state='buried')

    #check that the put raises the right error on big jobs...
    tools.assert_raises(errors.JobTooBig, proto.process_put,'a' * (2**16),0,0,0)

def test_reserve():
    line, handler = proto.process_reserve()
    t = 'reserve\r\n'
    assert line == t

    check_result(handler('RESERVED 12 23 88'),
        status='i', jid=12, pri=23, bytes=88)

def test_delete():
    line, handler = proto.process_delete(12)
    t = 'delete 12\r\n'
    assert line == t

    check_result(handler('DELETED'))
    tools.assert_raises(errors.NotFound, handler, 'NOT_FOUND')

def test_release():
    line, handler = proto.process_release(33,22,17)
    t = 'release 33 22 17\r\n'
    assert line == t

    check_result(handler('RELEASED'))
    check_result(handler('BURIED'), state='buried')
    tools.assert_raises(errors.NotFound, handler, 'NOT_FOUND')

def test_bury():
    line, handler = proto.process_bury(29, 21)
    t = 'bury 29 21\r\n'
    assert line == t
    check_result(handler('BURIED'))
    tools.assert_raises(errors.NotFound, handler, 'NOT_FOUND')

def test_peek_a():
    line, handler = proto.process_peek()
    t = 'peek\r\n'
    assert line == t

    # make sure that the line is correct when a jid is added
    line, handler = proto.process_peek(39)
    t = 'peek 39\r\n'
    assert line == t

    check_result(handler("FOUND 39 29999 29990"),
        status='i', jid=39, pri=29999, bytes=29990)
    tools.assert_raises(errors.NotFound, handler, 'NOT_FOUND')

def test_kick():
    line, handler = proto.process_kick(200)
    t = 'kick 200\r\n'
    assert line == t
    check_result(handler("KICKED 59"), count=59)

def test_stats():
    import yaml
    line, handler = proto.process_stats(23)
    t = 'stats 23\r\n'
    assert line == t
    #other way of process_stats happening...
    line, handler = proto.process_stats()
    t = 'stats\r\n'
    assert line == t
    # make sure that the result of a handler call has the right data processer
    assert handler('OK 22')[1]['parse_data'] is yaml.load
    # make sure everything else is right
    check_result(handler('OK 2200'), bytes=2200, status='i')

