#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import sys
import logging

logging.getLogger().setLevel(logging.DEBUG)

import aioamqp

from context import ezamqp as ezq

async def example():
    transport, protocol = await aioamqp.connect('172.17.0.2',
                                login='guest', password='guest')

    channel = await protocol.channel()

    rpcman = ezq.RPC(loop, channel, 'amq.topic')
    await rpcman.start_client()

    adder_func = rpcman.rpc("arithmetic.add")
    proc_func = rpcman.rpc("arithmetic.prod")
    bad_func = rpcman.rpc("arithmetic.whatever")

    z = await (await adder_func(5, 6))
    print("z=",z)
    y = await (await proc_func(z, 2.2))
    print("y=",y)

    try:
        w = await (await bad_func(8, 9))
    except ezq.RemoteException as e:
        print("Remote exception:", e)
        print("Remote traceback:")
        for l in e.remote_tb:
            print(l)

    await rpcman.rpc_("print_date")()
    await rpcman.rpc_("spooler")("hello!")

    print(await (await rpcman.rpc("format_kws")(a=7, b="hello", c=[])))

    transport.close()

logging.debug("Client Starting")

loop = asyncio.get_event_loop()
loop.run_until_complete(example())
