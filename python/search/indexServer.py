from concurrent import futures
import grpc
import sys
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
from google.protobuf import empty_pb2
import argparse
import time
import random
import json
import pickle
import threading

class IndexServicer(index_pb2_grpc.IndexServicer):
    
    def __init__(self, barrel_id):
        self.indexedItems = {}
        self.pointedToBy = {} # page : list of pages that point to it
        self.pagesInfo = {} 
        self.barrel_id = barrel_id
        self.lock = threading.Lock()
        self.last_step = 0

        # reload save
        try:
            with open(f"file1_barrel.pkl", "rb") as f:
                obj = pickle.load(f)
        except:
            obj = 0
        
        if obj:
            self.indexedItems = obj
        
        try:
            with open(f"file2_barrel.pkl", "rb") as f:
                obj = pickle.load(f)
        except:
            obj = 0
        
        if obj:
            self.pointedToBy = obj
        try:
            with open(f"file3_barrel.pkl", "rb") as f:
                obj = pickle.load(f)
        except:
            obj = 0
        
        if obj:
            self.pagesInfo = obj


    def addToIndex(self, request, context):
        
        if len(self.indexedItems) > self.last_step + 100:
            try:
                with self.lock:
                    with open(f"file1_barrel.pkl", "wb") as f:
                        pickle.dump(self.indexedItems, f)
                    
                    with open(f"file3_barrel.pkl", "wb") as f:
                        pickle.dump(self.pagesInfo, f)
            except Exception as e:
                print(e)
        
            
        url = request.url
        words = request.words
        title = request.title
        snippet = request.snippet
        
        self.pagesInfo[url] = {"title": title, "snippet": snippet}
        
        for word in words:
            if word not in self.indexedItems:
                self.indexedItems[word] = []
            self.indexedItems[word].append(url)
            
        return empty_pb2.Empty()
    
    def addToIndexPage(self, request, context):
        url_pointed = request.url_pointed
        url_that_points = request.url_that_points

        if url_pointed not in self.pointedToBy:
            self.pointedToBy[url_pointed] = []
        if url_that_points not in self.pointedToBy[url_pointed]:
            self.pointedToBy[url_pointed].append(url_that_points)
        #print(f"added url {url_that_points} that points to {url_pointed}")

        return empty_pb2.Empty()
    
    def searchWord(self, request, context):

        print("I was just used to search for a word")

        words = request.words
        
        sets = []
        for w in words:
            if w in self.indexedItems.keys():
                sets.append(set(self.indexedItems[w]))
            else:
                return index_pb2.SearchWordResponse(results=[])
    
        common_urls = set.intersection(*sets)
        common_urls = list(common_urls)
        common_urls.sort(key=lambda x: len(self.pointedToBy[x]), reverse=True)

        for url in common_urls:
            print(f"{url} pointed by {len(self.pointedToBy[url])}")
            print(f"POINTERS: {self.pointedToBy[url]}")

        results = []
        for url in common_urls:
            info = self.pagesInfo.get(url, {})
            results.append(index_pb2.SearchResult(
                url=url,
                title=info.get("title", "No title"),
                snippet=info.get("snippet", "")
            ))
            
        return index_pb2.SearchWordResponse(results=results)
    
    def searchPage(self, request, context):
        url = request.url
        urls_that_point = self.pointedToBy.get(url, []) # gets list, on key error will default to []
        
        return index_pb2.SearchPageResponse(urls=list(urls_that_point))

    def getStats(self, request, context):
        
        try:
        
            return index_pb2.BarrelStats(
                num_entries=len(self.indexedItems),
            )
        except Exception as e:
            print("error in getstats")
        
def serve(barrel_id):
    
    print("I am an indexServer / storage barrel")
    
    with open("config.json") as f:
        config = json.load(f)

    barrels = config["barrels"]

    host = barrels[barrel_id]["host"]
    port = barrels[barrel_id]["port"]
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = IndexServicer(barrel_id)
    servicer.port = port  # necessário para getStats()
    index_pb2_grpc.add_IndexServicer_to_server(servicer, server)

    server.add_insecure_port(f"{host}:{port}")
    server.start()
    print(f" Barrel {barrel_id} started on {host}:{port}")

    server.wait_for_termination()


if __name__ == "__main__":
    barrel_id = int(sys.argv[1])
    serve(barrel_id)
