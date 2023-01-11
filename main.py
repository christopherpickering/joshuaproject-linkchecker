import requests
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from functools import partial
import csv

from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ.get("API_KEY")


def get_urls():
    page=1
    page_size=100

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

        page += 1

        if len(data) < 100:
            break
        

        yield [*set(URLS)]

        break


# for x in get_urls():
#     print(x)


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
                # progress_indicator('x')
                # print(r)
                url_errors.extend(r)
            # else:
            #     progress_indicator('.')


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