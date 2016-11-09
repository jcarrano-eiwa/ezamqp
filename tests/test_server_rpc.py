import asyncio
import sys
import logging

import aioamqp

import ezamqp

import sys

logging.getLogger().setLevel(logging.DEBUG)

# This will raise an error if y=0
async def divider(x, y):
    print("x=", x)
    print("y=", y)

    print("x/y=", x/y)

    return x/y

async def setup(loop):
    transport, protocol = await aioamqp.connect('172.17.0.2',
                                login='guest', password='guest')

    channel = await protocol.channel()

    rpcman = ezamqp.RPC(loop, channel, 'amq.topic')

    await rpcman.start()

    # Lets listen for a topic
    await rpcman.register_rpc('divider', 'divider.*', divider)

loop = asyncio.get_event_loop()
loop.run_until_complete(setup(loop))
loop.run_forever()

