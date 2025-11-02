from concurrent import futures
import grpc
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
from google.protobuf import empty_pb2
import queue
from bloom_filter2 import BloomFilter
import threading
import time
from collections import Counter

class GatewayServicer(index_pb2_grpc.GatewayServicer):
    def __init__(self):
        self.urlsToIndex = queue.Queue()
        self.urlsseen = BloomFilter(max_elements=50_000_000, error_rate=0.01) #bloom filter instead of set
        self.lock = threading.Lock()
        self.stats = {}
        self.popular_searches = Counter()
        
        self.urlsToIndex.put("https://www.python.org/") # https://git-scm.com/
        self.urlsseen.add("https://www.python.org/")

        self.ports = [8081, 8082, 8083]
        self.barrels = []
        for port in self.ports:
            channel_barrel = grpc.insecure_channel('localhost:{port}'.format(port=port))
            stub_barrel = index_pb2_grpc.IndexStub(channel_barrel)
            self.barrels.append(stub_barrel)
            
            self.stats[port] = {}
            self.stats[port]["times"] = []
            self.stats[port]["num_entries"] = 0

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
            else:   
                return empty_pb2.Empty()
                
        self.urlsToIndex.put(url)  
                
        print(f"putNew() called with URL: {url}")
            
        return empty_pb2.Empty()

    def searchWord(self, request, context):

        words = request.words
        words = request.words
        query_search = " ".join(words).strip()
        self.popular_searches[query_search] += 1
        
        start = time.time()
        try:
            print(f"sent WORDS: {words} to storage barrel")
            
            result = self.barrels[self.round_robin_counter].searchWord(index_pb2.SearchWordRequest(words=words))
            elapsed = time.time() - start
            port = self.ports[0] + (self.round_robin_counter % len(self.barrels))
            self.stats[port]["times"].append(elapsed)
            
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
    
    def getSystemStats(self, request, context):
        
        response = index_pb2.SystemStatsResponse()
        
        for port, data in self.stats.items():
            
            avg_time = sum(data['times']) / len(data['times']) if data['times'] else 0
            
            try:
                channel = grpc.insecure_channel(f'localhost:{port}')
                stub = index_pb2_grpc.IndexStub(channel)
                stat = stub.getStats(empty_pb2.Empty())
                num_entries = stat.num_entries
                
            except Exception as e:
                num_entries = -1  # offline
                
            response.barrels.append(
                index_pb2.BarrelStats(port=str(port), num_entries=num_entries, avg_search_time=avg_time)
            )
            
            
        for (query, count) in self.popular_searches.most_common(10): 
            response.top_searches.append(query)
            
        return response

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
