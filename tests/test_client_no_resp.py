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

    rpcman = ezamqp.RPC_(channel, 'amq.topic')

    await rpcman.rpc_("adder.x1")(1, 5)

    transport.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(example())

