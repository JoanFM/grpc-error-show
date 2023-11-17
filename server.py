import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from jina.proto import jina_pb2, jina_pb2_grpc

import asyncio
import signal
import threading
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:  # pragma: no cover
    import multiprocessing

HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
    signal.SIGSEGV,
)


class RuntimeSupport:
    def __init__(
        self,
        port: int,
        signal_handlers_installed_event: Optional[
            Union['asyncio.Event', 'multiprocessing.Event', 'threading.Event']
        ] = None,
        **kwargs,
    ):
        self.port = port
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self.is_cancel = asyncio.Event()
        self.is_signal_handlers_installed = (
            signal_handlers_installed_event or asyncio.Event()
        )

        def _cancel(sig):
            def _inner_cancel(*args, **kwargs):
                self.is_cancel.set(),

            return _inner_cancel

        for sig in HANDLED_SIGNALS:
            self._loop.add_signal_handler(sig, _cancel(sig), sig, None)

        self.is_signal_handlers_installed.set()
        self._loop.run_until_complete(self.async_setup())

    async def async_setup(self):
        self.server = GRPCServerWrapper(port=self.port)
        await self.server.setup_server()

    async def _wait_for_cancel(self):
        while not self.is_cancel.is_set():
            await asyncio.sleep(0.1)

        await self.async_teardown()

    def run_forever(self):
        async def async_run_forever():
            await self.server.run_server()
            self.is_cancel.set()

        async def _loop_body():
            try:
                await asyncio.gather(async_run_forever(), self._wait_for_cancel())
            except asyncio.CancelledError:
                print('received terminate ctrl message from main process')

        self._loop.run_until_complete(_loop_body())

    def teardown(self):
        self._loop.run_until_complete(self.async_teardown())
        self._loop.stop()
        self._loop.close()

    async def async_teardown(self):
        await self.server.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.teardown()


class Handler:
    async def Call(self, request_iterator, context=None, *args, **kwargs):
        print(f'Received call in Server!')
        await asyncio.sleep(0.5)
        raise Exception('Causing exception')


class GRPCServerWrapper:
    def __init__(
        self,
        port: int,
        **kwargs,
    ):
        self._port = port
        self.health_servicer = health.aio.HealthServicer()
        self._handler = Handler()
        self.stopped = False

    async def setup_server(self):
        self.server = grpc.aio.server()

        jina_pb2_grpc.add_JinaRPCServicer_to_server(self._handler, self.server)

        service_names = (
            jina_pb2.DESCRIPTOR.services_by_name['JinaRPC'].full_name,
            reflection.SERVICE_NAME,
        )
        # Mark all services as healthy.
        health_pb2_grpc.add_HealthServicer_to_server(self.health_servicer, self.server)

        reflection.enable_server_reflection(service_names, self.server)

        bind_addr = f'0.0.0.0:{self._port}'
        self.server.add_insecure_port(bind_addr)
        await self.server.start()
        for service in service_names:
            await self.health_servicer.set(
                service, health_pb2.HealthCheckResponse.SERVING
            )
        print(f'Server is setup!!')

    async def shutdown(self):
        """Free other resources allocated with the server, e.g, gateway object, ..."""
        if not self.stopped:
            print(f'SHUTDOWN START')
            await self.health_servicer.enter_graceful_shutdown()
            await self.server.stop(1.0)
            print(f'SHUTDOWN FINISH')
        self.stopped = True

    async def run_server(self):
        """Run GRPC server forever"""
        await self.server.wait_for_termination()


def run(
    port: int,
    is_shutdown: Union['multiprocessing.Event', 'threading.Event'],
    is_ready: Union['multiprocessing.Event', 'threading.Event'],
    is_signal_handlers_installed: Union['multiprocessing.Event', 'threading.Event'],
):
    try:
        runtime = RuntimeSupport(
            port=port, signal_handlers_installed_event=is_signal_handlers_installed
        )
    except Exception as ex:
        print(f'Exception {ex}')
    else:
        with runtime:
            # here the ready event is being set
            is_ready.set()
            print(f'Lets run forever!')
            runtime.run_forever()
    finally:
        is_shutdown.set()
