import multiprocessing
from server import run
from client import call
import time
import asyncio

if __name__ == '__main__':
    ports = [8080, 8081, 8082, 8083]
    for port in ports:
        is_shutdown = multiprocessing.Event()
        is_ready = multiprocessing.Event()
        is_signal_handlers_installed = multiprocessing.Event()
        p = multiprocessing.Process(
            target=run, args=(port, is_shutdown, is_ready, is_signal_handlers_installed)
        )
        p.start()
        while not is_ready.is_set():
            time.sleep(5)
        asyncio.run(call(port))
        p.terminate()
        p.join()
