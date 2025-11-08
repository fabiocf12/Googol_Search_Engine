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
import json


class GatewayServicer(index_pb2_grpc.GatewayServicer):
    
    def __init__(self):
        self.urlsToIndex = queue.Queue()
        self.urlsseen = BloomFilter(max_elements=50_000_000, error_rate=0.01) #bloom filter instead of set
        self.lock = threading.Lock()
        self.popular_searches = Counter()
        self.stats = {}
        
        #To begin with
        self.urlsToIndex.put("https://eden.dei.uc.pt/~rbarbosa")
        self.urlsseen.add("https://eden.dei.uc.pt/~rbarbosa")
        
        with open("config.json") as f:
            config = json.load(f)

        self.gateway_host = config["gateway"]["host"]
        self.gateway_port = config["gateway"]["port"]
        self.barrel_configs = config["barrels"]

        self.barrels = []  #stubs list
        self.barrel_info = []  #info for round-robin and stats
        
        for barrel in self.barrel_configs:
            host = barrel["host"]
            port = barrel["port"]
            barrel_id = f"{host}:{port}"
            
            #creat channel and client
            channel_barrel = grpc.insecure_channel(f"{host}:{port}")
            stub_barrel = index_pb2_grpc.IndexStub(channel_barrel)
            print(f"connected {port}")
            
            self.barrels.append(stub_barrel)
            self.barrel_info.append({"host": host, "port": port, "id": barrel_id})
            self.stats[barrel_id] = {"times": [], "num_entries": 0}
            

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

    def searchWord(self, request, context,attempts=0):

        words = request.words
        words = request.words
        query_search = " ".join(words).strip()
        self.popular_searches[query_search] += 1
        
        start = time.time()
        idx = self.round_robin_counter
        info = self.barrel_info[idx]
        stub = self.barrels[idx]
        barrel_id = info["id"]

        try:
            print(f"sent WORDS: {words} to storage barrel {barrel_id}")
            
            result = stub.searchWord(index_pb2.SearchWordRequest(words=words))
            elapsed = time.time() - start
            self.stats[barrel_id]["times"].append(elapsed)
            
        except Exception as e:
            print(f"Error contacting {barrel_id}")
            attempts += 1
            
            if attempts >= len(self.barrels):
                print("Too many attemps. Aborting!")
                return index_pb2.SearchWordResponse(results=[])
            
            #tries on next barrel
            self.round_robin_counter = (self.round_robin_counter + 1) % len(self.barrels)
            return self.searchWord(request, context,attempts=attempts)

        else: # if everything good, move robin
            self.round_robin_counter = (self.round_robin_counter + 1) % len(self.barrels)   
            return result
    
    def searchPage(self, request, context,attempts=0):

        url = request.url
        idx = self.round_robin_counter
        info = self.barrel_info[idx]
        stub = self.barrels[idx]
        barrel_id = info["id"]
        
        try:
            print(f"sent URL: {url} to storage barrel")
            result = stub.searchPage(index_pb2.SearchPageRequest(url=url))

        except Exception as e:
            print(f"Error contacting {barrel_id}")
            attempts += 1
            
            if attempts >= len(self.barrels):
                print("Too many attemps. Aborting!")
                return index_pb2.SearchPageResponse(urls=[])   
            
            #tries on next barrel
            self.round_robin_counter = (self.round_robin_counter + 1) % len(self.barrels)
            return self.searchPage(request, context,attempts=attempts)
        
        else:# if everything good, move robin
            self.round_robin_counter = (self.round_robin_counter + 1) % len(self.barrels)
            return result
    
    def getSystemStats(self, request, context):
        
        response = index_pb2.SystemStatsResponse()
        
        for info in self.barrel_info:
            barrel_id = info["id"]
            
            avg_time = sum(self.stats[barrel_id]["times"]) / len(self.stats[barrel_id]["times"]) if self.stats[barrel_id]["times"] else 0
                
            try: #connection with barrel
                channel = grpc.insecure_channel(f'{info["host"]}:{info["port"]}')
                stub = index_pb2_grpc.IndexStub(channel)
                stat = stub.getStats(empty_pb2.Empty())
                num_entries = stat.num_entries
                
            except Exception as e:
                num_entries = -1  # offline
                
            response.barrels.append(
                index_pb2.BarrelStats(port=barrel_id, num_entries=num_entries, avg_search_time=avg_time)
            )
            
            
        for (query, count) in self.popular_searches.most_common(10): 
            response.top_searches.append(query)
            
        return response

def serve():
    print("I am the gateway")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    index_pb2_grpc.add_GatewayServicer_to_server(GatewayServicer(), server)
    
    with open("config.json") as f:
        config = json.load(f)

    gateway_host = config["gateway"]["host"]
    gateway_port = config["gateway"]["port"]

    server.add_insecure_port(f"{gateway_host}:{gateway_port}")
    server.start()
    print(f"Server started on {gateway_host}:{gateway_port}")
    
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
