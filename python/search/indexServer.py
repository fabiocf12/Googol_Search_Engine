from concurrent import futures
import grpc
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
from google.protobuf import empty_pb2
import argparse


class IndexServicer(index_pb2_grpc.IndexServicer):
    def __init__(self):
        self.indexedItems = {}
        self.pointedToBy = {} # page : list of pages that point to it

    def addToIndex(self, request, context):
        url = request.url
        words = request.words
        for word in words:
            if word not in self.indexedItems:
                self.indexedItems[word] = []
            self.indexedItems[word].append(url)
            #print(f"added url {url} to word {word}")
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

        words = request.words
        
        sets = []
        for w in words:
            if w in self.indexedItems.keys():
                sets.append(set(self.indexedItems[w]))

        if not sets:  # no known words
            print("#### no matches found")
            return index_pb2.SearchWordResponse(urls=[])
    
        common_urls = set.intersection(*sets)
        print("####", common_urls)

        common_urls = list(common_urls)

        common_urls.sort(key=lambda x: len(self.pointedToBy[x]), reverse=True)

        for url in common_urls:
            print(url, len(self.pointedToBy[url]))
            print(self.pointedToBy[url])

        return index_pb2.SearchWordResponse(urls=common_urls)
    
    def searchPage(self, request, context):
        url = request.url
        urls_that_point = self.pointedToBy.get(url, []) # gets list, on key error will default to []
        
        return index_pb2.SearchPageResponse(urls=list(urls_that_point))

def serve():
    print("I am an indexServer / storage barrel")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    index_pb2_grpc.add_IndexServicer_to_server(IndexServicer(), server)

    parser = argparse.ArgumentParser(description="description")
    parser.add_argument('--port', type=int, default=8080, help='Port to run the server on')
    args = parser.parse_args()

    server_port = args.port
    server.add_insecure_port("0.0.0.0:{}".format(server_port))
    # server.add_insecure_port("[::]:{}".format(server_port))
    server.start()
    print("Server started on port {}".format(server_port))
    
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
