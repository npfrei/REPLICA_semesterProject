"""
Function used to filter which painting from wikidata to keep 
"""


import requests
from config import *
import pandas as pd

BASE_URL_WIKIDATA = "https://www.wikidata.org/w/rest.php/wikibase/v1/"






"""
Used to get the "location" field for paintings on Wikidata (i.e. the gallery where they are stored)
"""
def lambda_func_gallery_wikidata(x):
    params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/" + x.split("/")[-1], params=params, auth=BearerAuth(wikidata_access_token))
    response.raise_for_status()
    return response.json()["statements"].get("P276",  [{"value": {"content": -1 }}])[0].get("value", {"content": -1}).get("content")

"""
    Used to get the "location" field for paintings on Wikidata (i.e. the coordinates where they are stored)
"""
def lambda_func_location(x):
    
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/"+x, auth=BearerAuth(wikidata_access_token))

    response.raise_for_status()
    content= response.json()["statements"].get("P625", [{"value": {"content": {'latitude': 0, 'longitude': 0}} }])[0].get("value", {"content": {'latitude': 0, 'longitude': 0}}).get("content")

    return (content.get("latitude", 0), content.get("longitude", 0)) if content is not None else (0,0)
        
        
"""
Used to authenticate into Wikidata's API
"""
class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


"""
Get the geolocation of each gallery
"""
def process_gallery_location():
    df = pd.read_csv("galleries.csv")
    df["geo_location"] = df["id"].apply(lambda x: lambda_func_location(x) if x !="-1" else (0,0))
    df.to_csv("galleries2.csv")
"""
Get the gallery where each painting is stored
"""
def process_paintings_gallery_wikidata():
    df = pd.read_csv("galleries_wikidata2.csv", dtype={"gallery": str})
    df2 = df.copy()
    df2 = df2[df2["gallery"]=="-10"] #keep only paintings for which we don't have the gallery information yet
    count = 0
    
    try:
        
        for i in range(count, len(df2), 100):

                df2["gallery"][i:i+100] = df2["item"][i:i+100].apply(lambda x: lambda_func_gallery_wikidata(x) )
                index = df2[i:i+100].index
                df["gallery"].iloc[index] = df2["gallery"][i:i+100]
                count = i
                df.to_csv("galleries_wikidata2.csv",index=False) 
    except Exception as e:
        print(f"Error occurred while processing row {i}: {e}")
        #recursive call to the function in case of error to keep processing the rest of the paintins 
        process_paintings_gallery_wikidata()
 
"""
Get the entire metadata for a Wikidata item in json format
"""
def get_metadata_from_wikidata_id(id:str):
    params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/" + id.split("/")[-1], params=params, auth=BearerAuth(wikidata_access_token))
    response.raise_for_status()
    return response.json()


