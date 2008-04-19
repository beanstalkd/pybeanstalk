'''
An extremely simple producer. Useful for testing and thats about it.
To use it, fill in the address and port for your beanstalkd. Optionally put
in the time between put commands (0 is "as fast as possible", aka no wait).
'''

import beanstalk
import time

SERVER = '127.0.0.1'
PORT = 11300
W_TIME = 1

x = beanstalk.serverconn.ServerConn(SERVER,PORT)

while True:
    x.put('fooo')
    if W_TIME: time.sleep(W_TIME)
