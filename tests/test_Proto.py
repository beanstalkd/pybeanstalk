import sys
print >>sys.stderr,sys.version
sys.path.append('..')
from nose import with_setup, tools
from beanstalk import protohandler
from beanstalk import errors


prototest_info = [
    [
        ('process_put', ('test_data', 0, 0, 10)),
        "put 0 0 10 %s\r\ntest_data\r\n" % (len('test_data'),),
        [
            ('INSERTED 3\r\n', {'state':'ok','jid':3}),
            ('BURIED 3\r\n', {'state':'buried','jid':3})
        ]
    ],
    [
        ('process_use', ('bar',)),
        'use bar\r\n',
        [
            ('USING bar\r\n', {'state':'ok', 'tube':'bar'})
        ]
    ],
    [
        ('process_reserve', ()),
        'reserve\r\n',
        [
            ('RESERVED 12 5\r\nabcde\r\n',{'state':'ok', 'bytes': 5, 'jid':12,
                'data':'abcde'})
        ]
    ],
    [
        ('process_reserve_with_timeout', (4,)),
        'reserve-with-timeout 4\r\n',
        [
            ('RESERVED 12 5\r\nabcde\r\n',{'state':'ok', 'bytes': 5, 'jid':12,
                'data':'abcde'}),
            ('TIMED_OUT\r\n', {'state':'timeout'})
        ]
    ],
    [
        ('process_delete', (12,)),
        'delete 12\r\n',
        [
            ('DELETED\r\n',{'state':'ok'})
        ]
    ],
    [
        ('process_touch', (185,)),
        'touch 185\r\n',
        [
            ('TOUCHED\r\n',{'state':'ok'})
        ]
    ],
    [
        ('process_release', (33,22,17)),
        'release 33 22 17\r\n',
        [
            ('RELEASED\r\n',{'state':'ok'}),
            ('BURIED\r\n',{'state':'buried'})
        ]
    ],
    [
        ('process_bury', (29, 21)),
        'bury 29 21\r\n',
        [
            ('BURIED\r\n',{'state':'ok'})
        ]
    ],
    [
        ('process_watch', ('supertube',)),
        'watch supertube\r\n',
        [
            ('WATCHING 5\r\n',{'state':'ok','count': 5})
        ]
    ],
    [
        ('process_ignore', ('supertube',)),
        'ignore supertube\r\n',
        [
            ('WATCHING 3\r\n', {'state':'ok', 'count':3})
            #('NOT_IGNORED',{'state':'buried'})
        ]
    ],
    [
        ('process_peek', (39,)),
        'peek 39\r\n',
        [
            ("FOUND 39 10\r\nabcdefghij\r\n", {'state':'ok', 'jid':39,
                'bytes':10, 'data':'abcdefghij'})
        ]
    ],
    [
        ('process_peek_ready', ()),
        'peek-ready\r\n',
        [
            ("FOUND 9 10\r\nabcdefghij\r\n",{'state':'ok', 'jid':9, 'bytes':10,
                'data':'abcdefghij'})
        ]
    ],
    [
        ('process_peek_delayed', ()),
        'peek-delayed\r\n',
        [
            ("FOUND 9 10\r\nabcdefghij\r\n",{'state':'ok', 'jid':9, 'bytes':10,
                'data':'abcdefghij'})
        ]
    ],
    [
        ('process_peek_buried', ()),
        'peek-buried\r\n',
        [
            ("FOUND 9 10\r\nabcdefghij\r\n",{'state':'ok', 'jid':9, 'bytes':10,
                'data':'abcdefghij'})
        ]
    ],
    [
        ('process_kick', (200,)),
        'kick 200\r\n',
        [
            ("KICKED 59\r\n",{'state':'ok', 'count':59})
        ]
    ],
    [
        ('process_stats', ()),
        'stats\r\n',
        [
            ('OK 15\r\n---\ntest: good\n\r\n', {'state':'ok', 'bytes':15,
                'data':{'test':'good'}})
        ]
    ],
    [
        ('process_stats_tube', ('barbaz',)),
        'stats-tube barbaz\r\n',
        [
            ('OK 15\r\n---\ntest: good\n\r\n',{'state':'ok', 'bytes':15,
                            'data':{'test':'good'}})

        ]
    ],
    [
        ('process_stats_job', (19,)),
        'stats-job 19\r\n',
        [
            ('OK 15\r\n---\ntest: good\n\r\n',{'state':'ok', 'bytes':15,
                'data':{'test':'good'}})

        ]
    ],
    [
        ('process_list_tubes', ()),
        'list-tubes\r\n',
        [
            ('OK 20\r\n---\n- default\n- foo\n\r\n', {'state':'ok', 'bytes':20,
                'data':['default','foo']})
        ]
    ],
    [
        ('process_list_tube_used',()),
        'list-tube-used\r\n',
        [
            ('USING bar\r\n', {'state':'ok', 'tube':'bar'})
        ]
    ],
    [
        ('process_list_tubes_watched', ()),
        'list-tubes-watched\r\n',
        [
            ('OK 20\r\n---\n- default\n- foo\n\r\n',{'state':'ok', 'bytes':20,
                'data':['default','foo']})

        ]
    ]
]




def check_line(l1, l2):
    assert l1 == l2, '%s %s' % (l1, l2)

def check_handler(handler, response, cv):
    l = 0
    while True:
        r = len(response)
        l = handler.remaining if handler.remaining > r else r
        y = handler(response[:l])
        response = response[l:]
        if y:
            assert y == cv
            break
    return

def test_interactions():
    for test in prototest_info:
        callinfo, commandline, responseinfo = test
        func = getattr(protohandler, callinfo[0])
        args = callinfo[1]
        for response, resultcomp in responseinfo:
            line, handler = func(*args)
            yield check_line, line, commandline
            yield check_handler, handler, response, resultcomp

def test_put_extra():
    #check that the put raises the right error on big jobs...
    tools.assert_raises(errors.JobTooBig, protohandler.process_put,'a' * (2**16),0,0,0)
    #check that it handles different job sizes correctly (not just default)
    oldmax = protohandler.MAX_JOB_SIZE
    protohandler.MAX_JOB_SIZE = 10
    tools.assert_raises(errors.JobTooBig, protohandler.process_put,'a' * 11,0,0,0)
    protohandler.MAX_JOB_SIZE = oldmax


def test_tube_name():
    assert(protohandler._namematch.match("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-+/;.$_()"))
