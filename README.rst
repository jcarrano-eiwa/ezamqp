=========================================
ezamqp: asyncio and AMQP made even easier
=========================================

¿What is this?
==============

ezamqp is a (very) thin layer over aioamqp. It allows you to

- Easily connect object methods with queue callbacks, creating the
  necessary queues.
- Write callbacks like regular functions. ezamqp takes care of
  serializing arguments, return data and exceptions.
- Forget about ACKs.
- Implement RPC, with a asyncio.Future based interface.
- All of the above while retaining full control over the AMQP primitives.

This is not
===========

- A full featured anything. In fact it is intentionally stripped down as
  much as useful.

Overview
========

The ``Queue`` class
-------------------

This class encapsulates an AMQP channel and adds a default exchange.
It allows you to easily define queues and callbacks.

You must parse the message payload and properties, and ack the message
if necessary.

the ``endpoint(...)`` decorator
-------------------------------

Decorate a class' methods with ``@endpoint`` to make them react to incoming
message. Then call ``Queue.autoconnect(object)`` to create the queues and
register the object's methods as callbacks.

There are different endpoint types. The ``Queue`` class implements the
``queue`` endpoint. ``RPC_`` implements ``rpc_`` and ``RPC`` implements
``rpc``.

The ``RPC_`` class
------------------

The ``RPC_`` class allows you to write your queue consumer callbacks in a
more human-friendly way, and automates the handling of acks and rejects.

The ``RPC`` class
-----------------

The ``RPC`` class allows consumer callbacks to return a response to the
instance that sent the message.

Example
=======

Server
------

.. code:: python

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

Client
------

.. code:: python

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
