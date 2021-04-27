import requests
from requests.api import get
import database as db
from os import system
from bs4 import BeautifulSoup
import re
import json
import requests
import concurrent.futures

initiation = db.initiate()


choices = ["Bogotá", "Medellín"]


def choose(choice):
    start_scraping(choice)


def add_new(data):
    return db.add_new(data)


def get_full_data(pid):

    url = f"https://www.fincaraiz.com.co/detail.aspx?a={pid}"

    r = requests.get(url=url)

    data = r.text

    soup = BeautifulSoup(data, "lxml")

    scripts = soup.find_all("script")

    for script in scripts:

        sc = re.findall(r"var sfAdvert = (.*);", str(script))
        if len(sc) > 0:
            datajson = json.loads(sc[0])
            return (
                pid,
                datajson["Category1Id"],
                datajson["Location1Id"],
                datajson["Location2Id"],
                datajson["AgeId"],
                datajson["Price"],
                datajson["Surface"],
                datajson["Area"],
                datajson["Rooms"],
                datajson["Baths"],
                datajson["Stratum"],
                datajson["Garages"],
                datajson["Latitude"],
                datajson["Longitude"],
            )

from concurrent.futures import ThreadPoolExecutor, as_completed



def start_scraping(choice):
    added = 0
    discarded = 0

    if choice == 1:

        for contcity in range(2):
            print(f"Starting with city {choices[contcity]}")
            for i in range(0, 100):
                print(f"{i}/?")

                choice_url = [
                    f"https://www.fincaraiz.com.co/AdvertsPoint.ashx?page={i}&find=ad=30|1||||1||8,9|||67|3630001,6700003,6700004,6700016||||||||||||||||1|0||||||||-1||",
                    f"https://www.fincaraiz.com.co/AdvertsPoint.ashx?page={i}&find=ad=30|1||||1||8,9|||55|5500006,5500001,5500016,5500004,5500005,5500002,5500003||||||||||||||||1|0||||||||-1||",
                ]
                url = choice_url[contcity]

                r = requests.get(url=url)

                data = r.json()

                # response length
                reslength = len(data["response"]["docs"])

                if reslength > 0:
                    # for each element in docs

                    links_to_call = []
                    for p in data["response"]["docs"]:
                        # for each id

                        add = db.is_in_db(p[0])

                        if add:
                            discarded = discarded + 1
                        else:
                            links_to_call.append(p[0])


                            # db.add_new(new_data)



                        # if result:
                        #     added = added + 1
                        # else:
                        #     discarded = discarded + 1
                        # print(f"New added: {added}, Discarded: {discarded}")
                    
                        
                    threads = []
                    i = 0
                    with ThreadPoolExecutor(max_workers=8) as executor:
                        for url in links_to_call:
                            threads.append(executor.submit(get_full_data, url))
                            
                        for task in as_completed(threads):
                            added = added + 1
                            print(f"New added: {added}, Discarded: {discarded}")
                            add_new(task.result())
                        
                else:
                    print("No more properties")
                    break
