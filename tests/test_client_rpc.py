#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import sys

import aioamqp

from context import ezamqp

async def example():
    transport, protocol = await aioamqp.connect('172.17.0.2',
                                login='guest', password='guest')

    channel = await protocol.channel()

    rpcman = ezamqp.RPC(loop, channel, 'amq.topic')
    await rpcman.start_client()

    the_future = await rpcman.rpc("divider.something")(1, 5)

    print(await the_future)

    ugly_error = await rpcman.rpc("divider.something")(3, 0)

    print(await ugly_error)

    transport.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(example())
