import asyncio
import sys
import logging
import datetime

logging.getLogger().setLevel(logging.DEBUG)

import aioamqp

from context import ezamqp as ezq

class Functions:
    # you can use coroutines as handlers
    @ezq.endpoint("rpc")
    async def format_kws(self, **kwargs):
        return "\n".join("{}={}".format(*i) for i in kwargs.items())

    # we can have one function listen to multiple routing_keys by using
    # topics and the extended keyword
    # you can use regular functions as handlers
    @ezq.endpoint("rpc", "arithmetic.*", extended = True)
    def operations(self, proc_name, q_name, x, y):
        if proc_name == 'arithmetic.add':
            return x+y
        elif proc_name == 'arithmetic.prod':
            return x*y
        else:
            raise ValueError("operation not supported %s", proc_name)

    #you can do this to avoid duplicate code
    rpc_ = ezq.endpoint("rpc_")

    @rpc_
    def print_date(self):
        print(datetime.datetime.utcnow())

    # create a named queue
    # This queue won't be deleted when the application exits.
    @ezq.endpoint("rpc_", "spooler", "spooler")
    def print_date(self, job):
        print("New job", job)

async def setup(loop):
    transport, protocol = await aioamqp.connect('172.17.0.2',
                                login='guest', password='guest')

    channel = await protocol.channel()

    rpcserver = ezq.RPC(loop, channel, 'amq.topic')

    # If we are only using the server we don't need to start
    # await rpcman.start()
    print("Autoconnecting")
    await rpcserver.autoconnect(Functions())
    print("Connected")

logging.debug("Server Starting")

loop = asyncio.get_event_loop()
loop.run_until_complete(setup(loop))
loop.run_forever()

