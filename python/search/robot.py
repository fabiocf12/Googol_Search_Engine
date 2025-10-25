import grpc
from google.protobuf import empty_pb2
import protos.index_pb2 as index_pb2
import protos.index_pb2_grpc as index_pb2_grpc
import requests
from bs4 import BeautifulSoup as jsoup

from urllib.parse import urljoin


def run():
    # Create a gRPC channel
    channel = grpc.insecure_channel('localhost:8183')
    
    # Create a stub (client)
    stub = index_pb2_grpc.IndexStub(channel)
    
    try:
        try:
            response = stub.takeNext(empty_pb2.Empty())
            print(response.url)
            url = response.url
            print(f"Received URL: {url}")
            
            try:
                # Fetch webpage using requests and parse with BeautifulSoup
                response = requests.get(url)
                response.raise_for_status()  # Raise an exception for bad status codes
                soup = jsoup(response.text, 'html.parser')

                link_list = soup.select("a[href]")
                abs_links = []
                for link in link_list:
                    link = urljoin(url, link["href"])
                    print("LINK:", link)
                    abs_links.append(link)

                page_text = soup.get_text()
                page_text_tokenized = page_text.split()
                print("tokenized:", page_text_tokenized)

                for link in abs_links:
                    stub.putNew(index_pb2.PutNewRequest(url=link))

                # TODO: Get all text and tokenize. 
                # TODO: find new URls and submit to queue

            except requests.RequestException as e:
                print(f"Error fetching webpage: {e}")
            
        except grpc.RpcError as e:
            print(f"RPC failed: {e.code()}")
            print(f"RPC error details: {e.details()}")
            raise
                
    except KeyboardInterrupt:
        print("\nStopping the robot...")
        
if __name__ == '__main__':
    run()
