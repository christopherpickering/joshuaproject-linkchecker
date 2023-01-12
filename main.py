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


API_KEY = os.environ.get("API_KEY")

START=0
END=99999999999
if os.environ.get("CHUNK"):
    END=int(os.environ.get("CHUNK"))
    START=END-50



def get_urls():

    page_size=100
    page = START

    end_page = END

    URLS=[]

    while True:
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
        if len(data) < 100 or page == end_page:
            break

        page += 1

        # if page > 0:
        #     break

def get_headers(url):
    try:
        p = requests.head(url, allow_redirects=False)

        if p.status_code not in [200,301]:
            return [url,p.status_code, '']

    except BaseException as e:
        return [url,999,e]

def progress_indicator(char):
    print(char, end='', flush=True)

def runner():
    url_errors =[]
    pages = 0
    for page, urls in get_urls():
        pages = page
        print(f"Processing {len(urls)} urls...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            url_errors.extend(executor.map(get_headers, urls))

    total = len(url_errors)
    url_errors=[x for x in url_errors if x]

    with open("errors.csv", "w") as file:

        writer = csv.writer(file)
        for row in url_errors:
            writer.writerow(row)

    return pages, total

if __name__ == '__main__':
    start_time = time.time()
    pages, total = runner()

    duration = time.time() - start_time
    print(f"Processed {total} urls in {duration} seconds from {int(pages)} API requests")







