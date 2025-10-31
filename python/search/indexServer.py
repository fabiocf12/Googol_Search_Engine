from concurrent import futures
import grpc
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
from google.protobuf import empty_pb2


class IndexServicer(index_pb2_grpc.IndexServicer):
    def __init__(self):
        self.indexedItems = {}

    def addToIndex(self, request, context):
        url = request.url
        words = request.words
        for word in words:
            if word not in self.indexedItems:
                self.indexedItems[word] = []
            self.indexedItems[word].append(url)
            print(f"added url {url} to word {word}")
        return empty_pb2.Empty()

    

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    index_pb2_grpc.add_IndexServicer_to_server(IndexServicer(), server)
    server_port = 8186
    server.add_insecure_port("0.0.0.0:{}".format(server_port))
    # server.add_insecure_port("[::]:{}".format(server_port))
    server.start()
    print("Server started on port {}".format(server_port))
    
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
