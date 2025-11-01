import grpc
from google.protobuf import empty_pb2
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
import requests
from bs4 import BeautifulSoup as jsoup

from urllib.parse import urljoin, urlparse
import os

def run():
    # Create a gRPC channel
    channel_gateway = grpc.insecure_channel('localhost:8185')
    # Create a stub (client)
    stub_gateway = index_pb2_grpc.GatewayStub(channel_gateway)

    # and one for communicating with the barrels
    channel_barrel = grpc.insecure_channel('localhost:8186')
    stub_barrel = index_pb2_grpc.IndexStub(channel_barrel)

    while True:
        try:
            try:
                response = stub_gateway.takeNext(empty_pb2.Empty())
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

                        path = urlparse(link).path.lower()
                        if os.path.splitext(path)[1] in {".zip", ".tar", ".gz", ".xz", ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".mp4", ".exe", ".iso", ".sign", ".bz2"}:
                            print("Skipping file type:", link)
                            continue
                        stub_barrel.addToIndexPage(index_pb2.AddToIndexRequestPage(url_pointed=link, url_that_points=url))

                    page_text = soup.get_text()
                    page_text_tokenized = page_text.split()
                    #print("tokenized:", page_text_tokenized)

                    for link in abs_links:
                        stub_gateway.putNew(index_pb2.PutNewRequest(url=link))

                    stub_barrel.addToIndex(index_pb2.AddToIndexRequest(url=url, words=page_text_tokenized))

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
            break
        
if __name__ == '__main__':
    run()
