import grpc
from google.protobuf import empty_pb2
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
import requests

def run():
    # Create a gRPC channel
    channel = grpc.insecure_channel('localhost:8185')
    
    # Create a stub (client)
    stub = index_pb2_grpc.GatewayStub(channel)

    try:
        while True:
            next_command = input("L - link to index\nW - words to search\nQ - quit ")
            
            if next_command == 'q':
                break
            
            elif next_command == "l": # submit a link to index
                link_to_idx = input("link: ")
                stub.putNew(index_pb2.PutNewRequest(url=link_to_idx))
                print(f"Submitted URL: {link_to_idx} to Gateway")
                
            elif next_command == "w": # submit words to search
                
                words = input("words: ")
                words_list = words.split(" ")
                
                try:
                    result = stub.searchWord(index_pb2.SearchWordRequest(words=words_list))
                    #print(f"Submitted WORDS: {words} to Gateway")
                    print(f"got result: {result}")
                    
                except Exception as e:
                    print(e)
                    
            elif next_command == "p": # submit a page to get pages that points to it
                url = input("page url: ")
                
                try:
                    result = stub.searchPage(index_pb2.SearchPageRequest(url=url))
                    print(f"got result: {result}")
                except Exception as e:
                    print(e)
                
    except KeyboardInterrupt:
        print("\nStopping the robot...")


if __name__ == '__main__':
    run()
