import grpc
from jina.proto import jina_pb2, jina_pb2_grpc


async def call(port):
    async with grpc.aio.insecure_channel(f'localhost:{port}') as channel:
        stub = jina_pb2_grpc.JinaRPCStub(channel)

        # Unary RPC method call
        def generator():
            reqs = [jina_pb2.DataRequestProto(), jina_pb2.DataRequestProto()]
            for req in reqs:
                yield req

        req_iter = generator()

        try:
            print(f'Lets call the server')
            async for resp in stub.Call(req_iter):
                print("Response from server:", resp)
        except Exception as e:
            print(f"Error from server: {e}")
