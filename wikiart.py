import requests
from typing import Optional
from config import *
import re
BASE_URL = "https://www.wikiart.org/en/api/2/"
BASE_URL_WIKIDATA = "https://www.wikidata.org/w/rest.php/wikibase/v1"
import pandas as pd
import pywikibot
from pywikibot import pagegenerators
def fetch_painting_JSON(
    id
) -> dict:
    """
    Fetches a  list of paintings from the WikiArt API, with optional filters and query parameters.

    Parameters
    ----------
    id: int
        ID of the painting (on WikiArt)

    Returns
    -------
    dict
        A JSON-parsed dictionary containing the results.

    Raises
    ------
    ValueError 
        If id is Invalid
    requests.HTTPError
        If the API request fails.
    """
    if id==-1:
        raise ValueError(f"invalid id , got {id}")
    
        
    params ={"id":id}
        
    
    response = requests.get(BASE_URL + "Painting", params=params,  timeout=10)
    response.raise_for_status()
    return response.json()

    
def login(wikiart_api_access_key, wikiart_api_secret_key):
    params = {
        "accessCode":wikiart_api_access_key,
        "secretCode":wikiart_api_secret_key
    }
    response = requests.get(BASE_URL+"login", params=params)
    response.raise_for_status()
    return response.json()["SessionKey"]

def get_gallery(painting:dict):
    return painting.get("galleries")

def get_painting_id_from_wikidata_id(id:str):
    artist = id.split("/")[0]
    title = id.split("/")[1]
    title2 = title.rstrip('0123456789')
    params ={"term": artist.replace("-", " ") + " "+ title2.replace("-", " ").removesuffix(" ") }
    print(params)
    response = requests.get(BASE_URL + "PaintingSearch", params=params,  timeout=10)
    response.raise_for_status()
    canditates = response.json()
    for paint in canditates.get("data"):
        
        if (artist+paint.get("image").split(artist)[1]).removesuffix(".jpg!Large.jpg")==id:
            return paint.get("id")
    return -1
#print(fetch_painting_JSON(get_painting_id_from_wikidata_id("vincent-van-gogh/the-starry-night-1889")))
"""
response = requests.get(
    BASE_URL_WIKIDATA,
    params={
        'action': 'query',
        "list":"Q3305213",
        'limit': 1,
    },
    headers={
        'Authorization': f'Bearer {wikidata_access_token}',
    }
)
"""
def get_wikidata_items():
    QUERY = """
    SELECT DISTINCT ?item WHERE {
    ?item p:P31 ?statement0.
    ?statement0 (ps:P31/(wdt:P279*)) wd:Q3305213.
    ?item p:P6002 ?statement1.
    ?statement1 (ps:P6002) _:anyValueP6002.
    }
    """
    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    generator = pagegenerators.PreloadingEntityGenerator(pagegenerators.WikidataSPARQLPageGenerator(QUERY,site=repo))
    l = {}
    i=0
    for item in generator:
       
       l.update( {item.getID():item.get()["claims"]["P6002"][0].getTarget()})
       
       i+=1
    
    df = pd.DataFrame(l.items(), columns=["wikidata_id", "wikiart_id"])
    df.to_csv("ids.csv")
get_wikidata_items()   