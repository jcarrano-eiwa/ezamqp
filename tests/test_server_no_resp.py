import asyncio
import sys

import aioamqp

from context import ezamqp

import sys


async def adder(x, y):
    print("x=", x)
    print("y=", y)

    print("x+y=", x+y)

    return x+y

async def setup(loop):
    transport, protocol = await aioamqp.connect('172.17.0.2',
                                login='guest', password='guest')

    channel = await protocol.channel()

    rpcman = ezamqp.RPC_(channel, 'amq.topic')

    await rpcman.register_rpc_('adder', 'adder.#', adder)

loop = asyncio.get_event_loop()
loop.run_until_complete(setup(loop))
loop.run_forever()

