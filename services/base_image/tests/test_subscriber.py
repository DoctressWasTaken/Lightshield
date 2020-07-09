# flake8: noqa
import os
import asyncio
from unittest.mock import AsyncMock, Mock, patch, PropertyMock
from services.base_image.subscriber import Subscriber
import websockets
from websockets.exceptions import InvalidHandshake

class TestSubscriber:

    subscriber = None

    def setup_method(self):

        os.environ['MAX_TASK_BUFFER'] = "10"
        self.subscriber = Subscriber(service_name='Test')
        self.loop = asyncio.get_event_loop()

    def test_initialization(self):

        assert self.subscriber.max_buffer == 10

    def test_pause_handler_not_stopped(self):
        # Setup
        self.subscriber.max_buffer = 100
        self.subscriber.redisc = Mock()
        type(self.subscriber.redisc).llen = AsyncMock(return_value=20)
        websocket = Mock()
        type(websocket).send = AsyncMock()
        self.loop.run_until_complete(self.subscriber.pause_handler(websocket))
        websocket.send.assert_any_await('PAUSE')
        websocket.send.assert_any_await('UNPAUSE')

    def test_pause_handler_stopped(self):
        # Setup
        self.subscriber.max_buffer = 100
        self.subscriber.redisc = Mock()
        self.subscriber.stopped = True
        type(self.subscriber.redisc).llen = AsyncMock(return_value=100)
        websocket = Mock()
        type(websocket).send = AsyncMock()
        self.loop.run_until_complete(self.subscriber.pause_handler(websocket))
        websocket.send.assert_awaited_once_with('PAUSE')

    def test_pause_handler_cycle(self):
        # Setup
        self.subscriber.max_buffer = 100
        self.subscriber.redisc = Mock()
        type(self.subscriber.redisc).llen = AsyncMock(side_effect=[100, 20])
        websocket = Mock()
        type(websocket).send = AsyncMock()
        self.loop.run_until_complete(self.subscriber.pause_handler(websocket))
        websocket.send.assert_any_await('PAUSE')
        websocket.send.assert_any_await('UNPAUSE')

    def test_init(self):
        with patch('aioredis.create_redis_pool', new_callable=AsyncMock) as mock:
            mock.return_value = 'NotARedisConnection'
            self.loop.run_until_complete(self.subscriber.init())
            assert self.subscriber.redisc == 'NotARedisConnection'

    def test_async_run_stopped(self):
        self.subscriber.init = AsyncMock()
        self.subscriber.stopped = True
        self.loop.run_until_complete(self.subscriber.async_run())
        self.subscriber.init.assert_awaited_once()


"""    def test_async_run_connection_failed(self):
        self.subscriber.init = AsyncMock()
        self.subscriber.uri = "http://uri.test"
        type(self.subscriber).stopped = PropertyMock(side_effect=[False, False, True])

        with patch('services.base_image.subscriber.websockets', new_callable=Mock) as websockets:
            websocket = Mock()
            type(websocket).send = AsyncMock(side_effect=ConnectionError)
            type(websockets).connect = AsyncMock(return_value=websocket)
            type(websockets).__aexit__ = AsyncMock(return_value=websocket)

            self.subscriber.logging.info = Mock()
            self.loop.run_until_complete(self.subscriber.async_run())
            print(self.subscriber.logging.info.call_args)
            websockets.assert_awaited_with(self.subscriber.uri)
            print(self.subscriber.logging.info.call_args)
            self.subscriber.logging\
                .info.assert_any_call('Could not establish connection, handshake failed.')
"""