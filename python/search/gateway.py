from concurrent import futures
import grpc
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
from google.protobuf import empty_pb2
import queue
from bloom_filter2 import BloomFilter
import threading


class GatewayServicer(index_pb2_grpc.GatewayServicer):
    def __init__(self):
        self.urlsToIndex = queue.Queue()
        self.urlsseen = BloomFilter(max_elements=50_000_000, error_rate=0.01) #bloom filter instead of set
        self.lock = threading.Lock()
        
        
        #self.urlsToIndex.put("https://example.com") # https://git-scm.com/
        #self.urlsseen.add("https://example.com")

        ports = [8081, 8082, 8083]
        self.barrels = []
        for port in ports:
            channel_barrel = grpc.insecure_channel('localhost:{port}'.format(port=port))
            stub_barrel = index_pb2_grpc.IndexStub(channel_barrel)
            self.barrels.append(stub_barrel)

        self.round_robin_counter = 0


    def takeNext(self, request, context):
        #print("takeNext() called sending an URL")
        
        try:
            url = self.urlsToIndex.get_nowait()     #nowait or just get?
            
        except queue.Empty:
            #print("URL Queue empty - nothing to send!")
            return index_pb2.TakeNextResponse(url="")
        
        print(f"[GATEWAY] Sending URL : {url}")  
        return index_pb2.TakeNextResponse(url=url)

    def putNew(self, request, context):
        
        url = request.url
        
        with self.lock:
            
            if url not in self.urlsseen:
                
                self.urlsseen.add(url)    
                self.urlsToIndex.put(url)  
                
        print(f"putNew() called with URL: {url}")
            
        return empty_pb2.Empty()

    def searchWord(self, request, context):

        words = request.words
        try:
            print(f"sent WORDS: {words} to storage barrel")
            result = self.barrels[self.round_robin_counter].searchWord(index_pb2.SearchWordRequest(words=words))
            self.round_robin_counter = (self.round_robin_counter + 1) % len(self.barrels)
            
        except Exception as e:
            print(e)

        return result
    
    def searchPage(self, request, context):

        url = request.url
        try:
            print(f"sent URL: {url} to storage barrel")
            result = self.barrels[self.round_robin_counter].searchPage(index_pb2.SearchPageRequest(url=url))
            self.round_robin_counter = (self.round_robin_counter + 1) % len(self.barrels)

        except Exception as e:
            print(e)

        return result

def serve():
    print("I am the gateway")

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
