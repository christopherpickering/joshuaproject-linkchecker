import requests
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from functools import partial
import csv

from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ.get("API_KEY")

START=0
END=99999999999
if os.environ.get("CHUNK"):
    END=int(os.environ.get("CHUNK"))
    START=END-3000



def get_urls():

    page_size=100
    page = (START/page_size) + 1

    end_page = (END/page_size)

    URLS=[]

    while True:
        p = requests.get(f'https://api.joshuaproject.net/v1/people_groups.json?api_key={API_KEY}&page={page}&limit={page_size}')

        data=p.json()

        for group in data:
            if "Resources" not in group:
                continue
            for resource in group["Resources"]:
                if "URL" in resource:
                    URLS.append(resource["URL"])

        yield [*set(URLS)]


        # if page size is < 100 then we reached the end.
        if len(data) < 100 or page == end_page:
            break

        page += 1

        # break

def process(url_batch):
    errors = []
    for url in url_batch:
        try:
            p = requests.head(url, allow_redirects=True)

            if p.status_code != 200:
                errors.append([url,p.status_code, ''])

            if p.history:
                errors.append([url,301, p.url])
        except BaseException as e:
            errors.append([url,999,e])
    return errors


def progress_indicator(char):
    print(char, end='', flush=True)

def runner():
    worker_count = int(round((os.cpu_count() or 1) / 2))

    if sys.platform == "win32":
        # Work around https://bugs.python.org/issue26903
        worker_count = min(worker_count, 60)

    with ProcessPoolExecutor(max_workers=worker_count) as exe:
        url_errors = []

        func = partial(process)
        futures = {exe.submit(func, url_batch): url_batch for url_batch in get_urls()}

        for future in as_completed(futures):
            r = future.result()
            if r:
                # if running "at home" we can turn on a progress bar to show failed groups
                # progress_indicator('x')
                url_errors.extend(r)
            # else:
            #     progress_indicator('.')


    # if running "at home" we can print errors.
    # print("\n\n")
    # print(len(url_errors), " errors")
    with open("errors.csv", "w") as file:

        writer = csv.writer(file)
        for row in url_errors:
            writer.writerow(row)

    # if there were any 999 or 400 or 406 return an error.
    # get all status codes
    codes = [x for x in url_errors if x[1] in [999,400,406]]

    if codes:
        return 1
    return 0

if __name__ == '__main__':
    print(runner())