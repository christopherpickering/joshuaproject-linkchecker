import requests
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from functools import partial
import csv
import concurrent.futures
import threading
import time


from dotenv import load_dotenv
load_dotenv()

thread_local = threading.local()

CHUNK=int(os.environ.get("CHUNK",'10'))
CHUNK_SIZE=int(os.environ.get("CHUNK_SIZE",'10'))

API_KEY = os.environ.get("API_KEY")

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'}


def get_urls():

    page_size=int(os.environ.get("CHUNK_SIZE",'100'))
    page = CHUNK-(CHUNK_SIZE-1) # pages start at 1

    end_page = CHUNK

    while True:
        URLS=[]
        p = requests.get(f'https://api.joshuaproject.net/v1/people_groups.json?api_key={API_KEY}&page={page}&limit={page_size}')

        try:
            data=p.json()

            for group in data:
                if "Resources" not in group:
                    continue
                for resource in group["Resources"]:
                    if "URL" in resource:
                        URLS.append(resource["URL"])

            yield page, [*set(URLS)]

        except BaseException as e:
            print(f"Error with https://api.joshuaproject.net/v1/people_groups.json?api_key=...&page={page}&limit={page_size}'")
            print(e)

        # if page size is < 100 then we reached the end.
        if len(data) < CHUNK_SIZE or page == CHUNK:
            break

        page += 1

        # if page > 0:
        #     break

def get_headers(url):

    ok_codes = [
        200, # good
        301, # moved and redirected
        302 # found, no change
        307 # found and redirected
        303 # redirected
        ]
    try:
        p = requests.head(url, allow_redirects=False, headers=HEADERS)

        if p.status_code not in ok_codes:

            # retry and real get request.
            # some servers block head requests.
            p = requests.get(url, allow_redirects=False, headers=HEADERS)
            if p.status_code not in ok_codes:
                return [url,p.status_code, '']

    except BaseException as e:
        return [url,999,e]

def runner():
    url_errors =[]

    for page, urls in get_urls():
        print(f"Processing {len(urls)} urls from page {page}...",sep='',end="\r",flush=True)
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
            url_errors.extend(executor.map(get_headers, urls))
        duration = round(time.time() - start_time,0)
        print(f"Processed {len(urls)} urls from page {page} in {duration} seconds", flush=True)
    total = len(url_errors)
    url_errors=[x for x in url_errors if x]

    with open("errors.csv", "w") as file:

        writer = csv.writer(file)
        for row in url_errors:
            writer.writerow(row)

    return total

if __name__ == '__main__':
    start_time = time.time()
    total = runner()

    duration = time.time() - start_time
    print(f"Processed {total} urls in {duration} seconds.")







