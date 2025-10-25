from concurrent import futures
import grpc
import protos.index_pb2 as index_pb2
import protos.index_pb2_grpc as index_pb2_grpc
from google.protobuf import empty_pb2


class IndexServicer(index_pb2_grpc.IndexServicer):
    def __init__(self):
        self.urlsToIndex = []
        self.indexedItems = []
        # TODO: This approach needs to become interactive. Use input() to create a rudimentary user interface to:
        # 1. Add urls for indexing
        # 2. search indexed urls
        self.urlsToIndex.append("https://git-scm.com/")

    def putNew(self, request, context):
        self.urlsToIndex.append(request.url)
        print(f"putNew() called with URL: {request.url}")
        return empty_pb2.Empty()

    def takeNext(self, request, context):
        print("takeNext() called sending an URL")
        return index_pb2.TakeNextResponse(url=self.urlsToIndex[0])



def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    index_pb2_grpc.add_IndexServicer_to_server(IndexServicer(), server)
    server_port = 8183
    server.add_insecure_port("0.0.0.0:{}".format(server_port))
    # server.add_insecure_port("[::]:{}".format(server_port))
    server.start()
    print("Server started on port {}".format(server_port))
    
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
