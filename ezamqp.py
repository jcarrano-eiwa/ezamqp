"""Easy AMQP Endpoint management"""

import json
import asyncio
import itertools
import functools
import enum
import traceback
import datetime
import collections
from collections import ChainMap
from weakref import WeakValueDictionary
import logging

import aioamqp

__author__ = [  "Juan Carrano <juan@carrano.com.ar>"]
__version__ = "0.1.0"
__license__ = """MIT"""

logger = logging.getLogger(__name__)

@enum.unique
class ACK(enum.Enum):
    """Message acknowledge modes.

    no_ack: No ACK required.
    receive: ACK when the message is received.
    process: ACK when the message is processed.
    success: ACK when the message is processed successfuly, reject if
            it is not (use with caution).
    """
    no_ack = 0
    receive = 1
    process = 2
    success = 3

def _endpoint_as(endpoint_type, topic, queue_name, f,
                                            **queue_declare_kwargs):
    f._queue_topic = topic
    f._queue_name = queue_name
    f._queue_kwargs = queue_declare_kwargs
    f._endp_type = endpoint_type

    return f

def endpoint(endpoint_type, topic = None, queue_name = '',
                                                **queue_declare_kwargs):
    return functools.partial(_endpoint_as, endpoint_type, topic, queue_name,
                            **queue_declare_kwargs)

def _prefix(prefix, string):
    return "{}.{}".format(prefix, string) if prefix else string

def _tstamp():
    """Time as an integer amount of seconds."""
    return round(datetime.datetime.utcnow().timestamp())

if hasattr(asyncio.BaseEventLoop, "create_future"):
    def _create_future(loop):
        return loop.create_future()
else:
    logging.warning("ayncio.BaseEventLoop does not have a create_future method")
    def _create_future(loop):
        return asyncio.Future()

class Queue:
    """Manage AMQP queues.

    This class exposes a simpler interface to send and receive messages
    that do not expect a response.

    The only endpoint type it supports is "queue", with the signature:
        f(channel, body, envelope, properties)
    """

    def __init__(self, channel, exchange_name, **defaults):
        self.channel = channel
        self.exchange_name = exchange_name

        self._register_defaults = defaults

    # TODO: add a "fork" keyword argument to register_queue()
    async def register_queue(self, queue_name, routing_key, function,
                            exclusive = False, no_ack = False, **kwargs):
        """Create a queue named `queue_name`, bind it with `routing_key`
        and start consuming with `function` as the callback.

        If the queue name is empty and kwargs does not indicate
        otherwise, the queue is set to auto-delete.

        Parameters:
            queue_name: Name of the queue (string). Can be empty.
            routing_key: Key or pattern to bind. If the exchange is a
                    topic exchange, the `routing_key` can be a pattern
                    like *.us.stock.#
            function: Coroutine to be used as function. The function
                    must have the the signature:
                        f(channel, body, envelope, properties)
                    Where:
                        channel: channel name as str.
                        body: message payload as bytes.
                        envelope: an aioamqp.envelope.Envelope
                        properties: an aioamqp.properties.Properties
            exclusive, no_ack: Passed to basic_consume.
            **kwargs: Extra arguments for channel.queue_declare
        """
        if "auto_delete" not in kwargs and not queue_name:
            kwargs["auto_delete"] = True

        q = await self.channel.queue_declare(queue_name, **kwargs)
        _queue_name = q['queue']
        logger.debug("Created queue: %s", _queue_name)

        await self.channel.queue_bind(_queue_name, self.exchange_name,
                                                        routing_key)
        await self.channel.basic_consume(function, _queue_name,
                                    exclusive=exclusive, no_ack=no_ack)

    def publish(self, data, routing_key, **kwargs):
        """Shorthand for publishing to self.exchange_name."""
        return self.channel.publish(data, self.exchange_name, routing_key,
                                    **kwargs)

    def register_x(self, endp_type, queue_name, queue_topic, f,
                                                            **kwargs):
        registerer_name = "register_{}".format(endp_type)
        # prevent runaway recursion
        if registerer_name == "register_x":
            raise ValueError("'x' is not allowed as endpoint type")

        try:
            reg = getattr(self, registerer_name)
        except AttributeError:
            raise ValueError("Unknown endpoint type %s"%endp_type)

        kwargs = ChainMap(kwargs, self._register_defaults)

        return reg(queue_name, queue_topic, f, **kwargs)

    def autoregister(self, f, prefix = ""):
        """Register a function that was decorated with @endpoint.
        """
        queue_name = f._queue_name and _prefix(prefix, f._queue_name)
        queue_topic = _prefix(prefix, f._queue_topic or f.__name__)

        return self.register_x(f._endp_type, queue_name, queue_topic,
                                                f, **f._queue_kwargs)

    async def autoconnect(self, obj, prefix = ""):
        """Inspect all  attributes of object `obj` and register as
        handlers all which were decorated by @endpoint.

        If prefix is given it is prepended to queue and topic strings.
        """
        for attr_name in dir(obj):
            f = getattr(obj, attr_name)
            if hasattr(f, "_endp_type"):
                await self.autoregister(f, prefix = prefix)

class RPC_(Queue):
    """Remote Procedure Calls without replies.

    RPC_ (with trailing underscore) allows you to write your queue
    consumer callbacks in a more human-friendly way.


    It extends Queue with:

    * A wrapper for callbacks that deserializes arguments and calls the
      "real" callback.
    * ACK/Reject handling.
    * A new endpoint type "rpc_".
    * A method for serializing arguments and publishing the message.

    Several signatures for user callbacks are supported. The standard one is::

        f(*args, **kwargs)

    The extended version is::

        f(proc_name, queue_name, *args, **kwargs)

    where proc_name is the name that was called on the client side and
    queue_name is the name of the queue that received the message.
    Usually you will want to use the standard type, but the extended
    format may be useful if you are using wildcard topic routing, for
    example, to have one function process all messages like us.stock.*

    Indicate the type of callback you will be using with the `extended`
    parameter of `RPC_.register_rpc_`

    Setting extended=None disables callback wrapping and the signature becomes
    the sames as for the Queue class::

        f(channel, body, envelope, properties)
    """

    # Server methods

    async def _rpc_noret_receive_wrapper(self, f, channel, body,
                                        envelope, properties,
                                        ack_mode=None, extended=False):
        if ack_mode == ACK.receive:
            await self.channel.basic_client_ack(envelope.delivery_tag)

        try:
            if extended is None:
                coro_or_r = f(channel, body, envelope, properties)
            else:
                d = self.decode_args(body.decode("utf-8"))
                if extended:
                    coro_or_r = f(d['proc_name'], channel, *d['args'], **d['kwargs'])
                else:
                    coro_or_r = f(*d['args'], **d['kwargs'])

            if isinstance(coro_or_r, collections.abc.Coroutine):
                r = await coro_or_r
            else:
                r = coro_or_r
        except Exception as e:
            if ack_mode == ACK.success:
                await self.channel.basic_reject(envelope.delivery_tag)
            logger.exception("Exception in RPC handler: %s", channel)
            raise
        else:
            if ack_mode == ACK.success:
                await self.channel.basic_ack(envelope.delivery_tag)
        finally:
            if ack_mode == ACK.process:
                await self.channel.basic_client_ack(envelope.delivery_tag)

        return r

    def register_rpc_(self, queue_name, routing_key, function,
                    ack_mode = ACK.receive, extended=False, **kwargs):
        """Create a queue named `queue_name`, bind it with `routing_key`
        and start consuming with `function` as the callback.

        See the docs for class RPC_ for a description of the function
        signatures.
        """
        wrapped = functools.partial(self._rpc_noret_receive_wrapper, function,
                                ack_mode=ack_mode, extended=extended)
        return self.register_queue(queue_name, routing_key, wrapped,
                                                            **kwargs)

    # Client methods

    def _call(self, proc_name, publish_method,
              properties, mandatory, immediate, *args, **kwargs):
        data, ctype, cenc = self.format_args(proc_name, *args, **kwargs)

        props = {'content_type': ctype, 'content_encoding': cenc,
                 'timestamp': _tstamp()}
        props.update(properties)

        return publish_method(data, proc_name,
                                properties=props, mandatory=mandatory,
                                immediate=immediate)

    def rpc_(self, proc_name, mandatory = False, immediate = False,
                                                        **properties):
        """Return a callable:
            f(*args, **kwargs)
        that can be used to do remote procedure calls without reply.
        The callable returns an awaitable coroutine.

        Parameters:
            proc_name: The procedure name. This will be used as
                       routing_key and also encoded in the message that
                       will be sent.
            _publish_method: callable to be used for sending the
                             message. self.publish is used by default.
            **kwags: `mandatory` and `immediate` are passed to
                     _publish_method. The rest are interpreted as
                     properties.
        """
        return functools.partial(self._call, proc_name, self.publish,
                                    properties, mandatory, immediate)

    @staticmethod
    def format_args(proc_name, *args, **kwargs):
        """Format arguments for sending over AMQP.

        Returns:
            data, content_type, content_encoding
        """
        d = {'proc_name': proc_name,
             'args': args,
             'kwargs': kwargs}

        return json.dumps(d).encode("utf-8"), "application/json", "utf-8"

    @staticmethod
    def decode_args(body):
        return json.loads(body.decode("utf-8"))


class RemoteException(Exception):
    """This exception is raise when the remote RPC handler raises an
    error.

    Attributes:
        _exc_type: string containing the remote exception type.
        _message: string containing the original exception message
        remote_tb: List of strings containing the remote traceback.
    """
    def __init__(self, message, exc_type, remote_tb):
        super().__init__("{}('{}')".format(exc_type, message))
        self._message = message
        self._exc_type = exc_type
        self.remote_tb = remote_tb

class RPC(RPC_):
    """RPC (without underscore) extends RPC_ with the possibility of
    sending (and receiving) responses.

    RPCs differ from normal messages in that they generate an
    additional "response" message.

    It adds:
    * An additional wrapper (built over the one in RPC_) for callbacks
      that serializes the return (or any exception that is raised) and
      sends it to a "response" queue.
    * A new endpoint type "rpc"
    * A low level publish_rpc method that works similar to Queue.publish
      except that it does the preparations so that a response can be
      received.
    * A high level `rpc` method for calling remote procedures in a
      friendly way.
    * The constructor take and additional "loop" argument.

    The rpc interface is based on Futures. Each calls yields a Future
    object that can be awaited.

    To use the RPC client, you must first call the start_client()
    coroutine-method to create the response queue and register the
    receiver callback.

    Implementation details:
    The Futures for unanswered rpcs are kept in a WeakValueDictionary.
    If the application looses all references for a Future then it is
    deleted and can't be recovered. When the response arrives, it will
    be discarded.
    """
    def __init__(self, loop, *args, **kwargs):
        """loop must be a asycio.BaseEventLoop-derived object.
        The rest of the argument are passed on to the superclass."""
        super().__init__(*args, **kwargs)
        self._loop = loop
        self._correlation_gen = itertools.count()
        self._open_responses = WeakValueDictionary()
        self._client_started = False

    # RPC server: Methods dealing with receiving, processing and
    # responding to RPCs

    async def _rpc_receive_wrapper(self, f, channel, body, envelope, properties,
                                        ack_mode, **kwargs):
        try:
            r = await self._rpc_noret_receive_wrapper(f, channel, body,
                                        envelope, properties,
                                        ack_mode=ack_mode, **kwargs)
        except Exception as e:
            rdata, ctype, cenc = self.format_exception(e)
        else:
            rdata, ctype, cenc = self.format_response(r)

        cid = properties.correlation_id

        new_props = {'timestamp': _tstamp(),
                     'content_type': ctype,
                     'content_encoding': cenc,
                     'correlation_id': cid,
                     'reply_to': properties.reply_to}

        logger.debug("Sending response: %s", cid)

        await self.channel.publish(rdata, '', properties.reply_to,
                                    properties = new_props)

    def register_rpc(self, queue_name, routing_key, function,
                    ack_mode = ACK.receive, extended = False, **kwargs):
        wrapped = functools.partial(self._rpc_receive_wrapper, function,
                            ack_mode=ack_mode, extended=extended)
        return self.register_queue(queue_name, routing_key, wrapped,
                                                    **kwargs)

    @staticmethod
    def format_response(obj):
        data = json.dumps({'return': obj}).encode("utf-8")

        return data, "application/json", "utf-8"

    @staticmethod
    def format_exception(exception):
        """Convert `exception` to a dict and serialize it"""
        err_info = {
                'exc_type': type(exception).__name__,
                'exc_message': str(exception),
                'traceback': traceback.format_tb(exception.__traceback__)
                }

        data = json.dumps(err_info).encode("utf-8")

        return data, "application/json", "utf-8"

    # RPC client: Methods dealing with sending RPC requests and
    #    receiving the results.

    async def start_client(self):
        """Declare the response queue.
        You should call this only once. After the first call, further
        call to start_client() will do nothing.
        """
        if self._client_started:
            return

        r = await self.channel.queue_declare(exclusive = True,
                                             auto_delete = True)
        self.response_queue = r['queue']

        logger.debug("Return queue is: %s", self.response_queue)

        await self.channel.basic_consume(self._response_handler,
                                 self.response_queue,
                                 no_ack=True)

        self._client_started = True

    async def _response_handler(self, channel, body, envelope, properties):
        d = self.decode_response(body)
        cid = properties.correlation_id

        try:
            pending_future = self._open_responses.pop(cid)
        except KeyError:
            logger.debug("Discarded response: %s", cid)
            return

        if "return" in d:
            pending_future.set_result(d["return"])
        else:
            pending_future.set_exception(RemoteException(
                                            d["exc_message"],
                                            d["exc_type"],
                                            d["traceback"]))

    async def publish_rpc(self, data, routing_key, **kwargs):
        """Publish a message along with the necessary steps to allow for
        a reply:

        * Create a future and register it.
        * Set the reply_to field.
        * Generate a new correlation id.

        Returns:
            future: A new asyncio.Future object associated with the
                    reply to this message. The future is registered in
                    self._open_responses.
        """
        # The correlation_id MUST be a string (why????)
        cid = str(next(self._correlation_gen))
        future = _create_future(self._loop)

        self._open_responses[cid] = future

        rpc_properties = {'reply_to': self.response_queue,
                          'correlation_id': cid}
        rpc_properties.update(kwargs.pop("properties", ()))

        await self.publish(data, routing_key, properties = rpc_properties,
                                **kwargs)

        return future

    def rpc(self, proc_name, mandatory = False, immediate = False,
                                                        **properties):
        """Return a callable:
            f(*args, **kwargs)

        that can be used to do remote procedure calls that send a reply.

        The callable returns a coroutine that does the remote call and
        yields an asyncio.Future object.
        The result (or error) of the Future is set when a reply is
        received.

        The RPC object keeps weak references to all Futures created.
        If the Future is destroyed, the reply to that RPC will be
        silently ignored.

        Example:

        Assuming client is a RPC object
        ... f = client.rpc("some_function")
        ... x = await f(1, 2, r = 0)
        ... result = await x
        """

        return functools.partial(self._call, proc_name, self.publish_rpc,
                                properties, mandatory, immediate)

    @staticmethod
    def decode_response(body):
        d = json.loads(body.decode("utf-8"))

        return d
