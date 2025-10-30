from concurrent import futures
import grpc
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
from google.protobuf import empty_pb2
import queue
from bloom_filter2 import BloomFilter


class GatewayServicer(index_pb2_grpc.GatewayServicer):
    def __init__(self):
        self.urlsToIndex = queue.Queue()
        self.urlsseen = BloomFilter(max_elements=50_000_000, error_rate=0.01) #bloom filter instead of set
        
        #should we use a lock to access it via threads? 
        
        self.urlsToIndex.put("https://git-scm.com/")
        self.urlsseen.add("https://git-scm.com/")



    def takeNext(self, request, context):
        print("takeNext() called sending an URL")
        
        try:
            url = self.urlsToIndex.get_nowait()     #nowait or just get?
            
        except queue.Empty:
            print("URL Queue empty - nothing to send!")
            return index_pb2.TakeNextResponse(url="")
        
        print(f"[GATEWAY] Sending URL : {url}")  
        return index_pb2.TakeNextResponse(url=url)

    def putNew(self, request, context):
        
        url = request.url
        
        if url in self.urlsseen:
            print("URL already added\n")
        else:
            self.urlsseen.add(url)    
            self.urlsToIndex.put(url)
            
            print(f"putNew() called with URL: {url}")
            
        return empty_pb2.Empty()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    index_pb2_grpc.add_GatewayServicer_to_server(GatewayServicer(), server)
    server_port = 8185
    server.add_insecure_port("0.0.0.0:{}".format(server_port))
    # server.add_insecure_port("[::]:{}".format(server_port))
    server.start()
    print("Server started on port {}".format(server_port))
    
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
