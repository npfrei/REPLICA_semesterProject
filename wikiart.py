import requests
from typing import Optional
from config import *
import re

import pandas as pd
import pywikibot
from pywikibot import pagegenerators
import os
import json
from geopy import geocoders  
BASE_URL_WIKIDATA = "https://www.wikidata.org/w/rest.php/wikibase/v1/"



    



def get_wikidata_items_with_img(query:str):
    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()
    
    generator = pagegenerators.PreloadingEntityGenerator(pagegenerators.WikidataSPARQLPageGenerator(query,site=repo))
           
    l = []
    i=0
    for item in generator:
       
        l.append(item.getID())
        i+=1
        print(i)
       
    df = pd.DataFrame(l, columns=["wikidata_id"])
    df.to_csv("images.csv")



def lambda_func_gallery_wikidata(x):
    params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/" + x.split("/")[-1], params=params, auth=BearerAuth(wikidata_access_token))
    response.raise_for_status()
    return response.json()["statements"].get("P276",  [{"value": {"content": -1 }}])[0].get("value", {"content": -1}).get("content")
def lambda_func_location(x):
    #params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/"+x, auth=BearerAuth(wikidata_access_token))
    
    response.raise_for_status()
    content= response.json()["statements"].get("P625", [{"value": {"content": {'latitude': 0, 'longitude': 0}} }])[0].get("value", {"content": {'latitude': 0, 'longitude': 0}}).get("content")
    print(content)
    return (content.get("latitude", 0), content.get("longitude", 0)) if content is not None else (0,0)

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


    
def process_gallery_location():
    df = pd.read_csv("galleries2.csv")

    df["geo_location"] = df["id"].apply(lambda x: lambda_func_location(x) if x !="-1" else (0,0))
    df.to_csv("galleries2.csv")
def process_paintings_gallery_wikidata():
    df = pd.read_csv("galleries_wikidata2.csv", dtype={"gallery": str})
    df2 = df.copy()
    df2 = df2[df2["gallery"]=="-10"]
    
    count = 0
    print(len(df))
    try:
        
        for i in range(count, len(df2), 100):
            
                
                df2["gallery"][i:i+100] = df2["item"][i:i+100].apply(lambda x: lambda_func_gallery_wikidata(x) )
                index = df2[i:i+100].index
                df["gallery"].iloc[index] = df2["gallery"][i:i+100]
                print("----", i, "----")
                count = i
                
                df.to_csv("galleries_wikidata2.csv",index=False) 
    except Exception as e:
        print(f"Error occurred while processing row {i}: {e}")
        process_paintings_gallery_wikidata()
def lambda_func_owned_by(x):
    params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/" + x.split("/")[-1], params=params, auth=BearerAuth(wikidata_access_token))
    response.raise_for_status()
    
    return response.json().get("statements", {"P127": [-2]}).get("P127",  [-1])[0]
    #return response.json()["statements"].get("P127",  [{"value": {"content": [-1] }}])[0].get("value", {"content": -1}).get("content")
def process_gallery_location_wikidata():
    df = pd.read_csv("galleries_wikidata.csv")
    df2 = pd.read_csv("galleries.csv")
    df = pd.DataFrame(df["gallery"].drop_duplicates(), columns=["gallery"])
    print(len(df))
    df = df[~df["gallery"].isin(df2["id"])]
    print(len(df))
    df["geo_location"] = df["gallery"].apply(lambda x: lambda_func_location(x) if isinstance(x, str) and x !="-1" and x!="-10" else (0,0))
    
    df.to_csv("galleries3.csv", index=False)
    
def process_owned_by_wikidata():
    df = pd.read_csv("images_with_owner2.csv", dtype={"owner": str})
    
    df2 = df.copy()
    df2 = df2[df2["owner"]=="-10"]
    
    count = 0
    print(len(df2))
    
    try:
        
        for i in range(count, len(df2), 100):
                df2["owner"][i:i+100] = df2["item"][i:i+100].apply(lambda x: lambda_func_owned_by(x) if isinstance(x, str)  else -1 )
                print("----", i, "----")
                df2.to_csv("images_with_owner2.csv", index=False)
                count = i
    except Exception as e:
        print(f"Error occurred while processing row {i}: {e}")
        process_owned_by_wikidata()
def lambda_func_owned_by(x):
    params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/" + x.split("/")[-1], params=params, auth=BearerAuth(wikidata_access_token))
    response.raise_for_status()
    return response.json().get("statements", {"P127": [-2]}).get("P127",  [-1])[0]
def lambda_func_img(x):
    params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/" + x.split("/")[-1], params=params, auth=BearerAuth(wikidata_access_token))
    response.raise_for_status()
    return response.json().get("statements", {"P18": [-2]}).get("P18",  [-1])[0]
def process_image_link_wikidata():
    df = pd.read_csv("images_with_owner2.csv", dtype={"img": str})
    df2 = df.copy()
    df2 = df2[df2["img"]=="-10"]
    count = 0
    print(len(df2))
    try:
        
        for i in range(count, len(df2), 100):
                df2["img"][i:i+100] = df2["item"][i:i+100].apply(lambda x: lambda_func_img(x) if isinstance(x, str)  else -1 )
                print("----", i, "----")
                df2.to_csv("images_with_owner2.csv", index=False)
                count = i
    except Exception as e:
        print(f"Error occurred while processing row {i}: {e}")
        process_image_link_wikidata()     
    
    
#lg.login()
#QUERY1 = 
"""
    SELECT DISTINCT ?item WHERE {
  ?item p:P31 ?statement0.
  ?statement0 (ps:P31/(wdt:P279*)) wd:Q3305213.
  ?item p:P6002 ?statement1.
  ?statement1 (ps:P6002) _:anyValueP6002.
    }
 #   """
#QUERY2 = 
"""
    SELECT DISTINCT ?item WHERE {
  ?item p:P31 ?statement0.
  ?statement0 (ps:P31/(wdt:P279*)) wd:Q3305213.
  ?item p:P18 ?statement1.
  ?statement1 (ps:P18) _:anyValueP18.
  {
    ?item p:P276 ?statement2.
    ?statement2 (ps:P276/(wdt:P279*)) wd:Q33506.
  }
  UNION
  {
    ?item p:P127 ?statement3.
    ?statement3 (ps:P127/(wdt:P279*)) wd:Q33506.
  }
}
LIMIT 10   
    """
#get_wikidata_items_with_img(QUERY2)   

#get_paintings_from_json()
#process_paintings_gallery()
#process_gallery_location()

#process_paintings_gallery_wikidata()
"""
df=pd.read_csv("galleries.csv")
df3=pd.read_csv("galleries2.csv")
df = df.append(df3)
print(len(df["geo_location"]))
df2=pd.read_csv("images.csv")
print((df2["gallery"].value_counts()))

df2 = df2.merge(df[["gallery", "geo_location"]], on="gallery", how="left")
df2 = df2[df2["geo_location"]!="(0, 0)"]


print(df2["completitionYear"].mean())
print(df2["completitionYear"].std())
print(df2["completitionYear"].min())
print(df2["completitionYear"].max())
print(df2["completitionYear"].quantile(0.25))
print(df2["completitionYear"].quantile(0.75))
import matplotlib.pyplot as plt
df2.hist("completitionYear", bins=range(0, int(df2["completitionYear"].max())+1, 20), log=True)
plt.show()
df2.hist("completitionYear", bins=range(1500, int(df2["completitionYear"].max())+1, 10), log=True)

plt.show()
df2.to_csv("images_with_location.csv", index=False)
"""
#process_gallery_location_wikidata()
#lambda_func_owned_by("Q45585")
"""
df = pd.read_csv("galleries_wikidata.csv", dtype={"gallery": str})
df2 = pd.read_csv("galleries3.csv", dtype={"gallery": str})
df3 = pd.read_csv("galleries2.csv", dtype={"gallery": str})
df4 = pd.read_csv("galleries.csv", dtype={"gallery": str})
df4 = df4[["id", "geo_location"]]
df4.rename(columns={"id": "gallery"}, inplace=True)
df2 = pd.concat([df2, df3, df4], ignore_index=True)
df2 = df2.drop_duplicates(subset=["gallery"], keep="first").reset_index(drop=True)
df = df.merge(df2[["gallery", "geo_location"]], on="gallery", how="left")
df = df.drop_duplicates(subset=["item"], keep="first").reset_index(drop=True)
df = df[df["geo_location"]!="(0, 0)"]
df = df[df["geo_location"].notna()]
print(len(df["item"].unique()))
df.to_csv("images_with_owner3.csv", index=False)
"""
"""

df = pd.read_csv("images_with_owner.csv", dtype={"owner": str})
df3 = pd.read_csv("images_with_owner2.csv", dtype={"owner": str})
df3 = df3[~df3["item"].isin(df["item"])]
df3["owner"] = "-10"
df3.to_csv("images_with_owner2.csv", index=False)
"""
#process_owned_by_wikidata()
#df = pd.read_csv("images_with_owner.csv")
#print(len(df[df["owner"]!="-1"]))

#process_image_link_wikidata()
def get_metadata_from_wikidata_id(id:str):
    params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/" + id.split("/")[-1], params=params, auth=BearerAuth(wikidata_access_token))
    response.raise_for_status()
    return response.json()
def lambda_func_artist(x):
    params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/" + x, params=params, auth=BearerAuth(wikidata_access_token))
    response.raise_for_status()
    print(response.json()["statements"].get("P170"))
    return response.json()["statements"].get("P170",  [{"value": {"content": -2 }}])[0].get("value", {"content": -2}).get("content")

def process_paintings_artist_wikidata():
    df = pd.read_csv("wikidata_to_process_artist.csv", dtype={"artist": str})
    df2 = df.copy()
    df2 = df2[df2["artist"]=="-1"]
    
    count = 0
    print(len(df))
    try:
        
        for i in range(count, len(df2), 100):
            
                
                df2["artist"][i:i+100] = df2["id"][i:i+100].apply(lambda x: lambda_func_artist(x) )
                index = df2[i:i+100].index
                df["artist"].iloc[index] = df2["artist"][i:i+100]
                print("----", i, "----")
                count = i
                
                df.to_csv("wikidata_to_process_artist.csv",index=False) 
    except Exception as e:
        print(f"Error occurred while processing row {i}: {e}")
        process_paintings_artist_wikidata()


"""
df = pd.read_csv("wikiart_images_location.csv")
artists = df["artist"].unique()
print(len(artists))
df2  = pd.read_csv("artist_names.csv")
df2["names"] = df2["names"].apply(lambda x : x.lower())
artists_match = {}
for artist in artists:
    
    matches = 0
    for ind, row in df2.iterrows():
        if str.__contains__(row["names"], '\''+artist +'\''):
            artists_match.update({artist:row["artist"]})
            matches+=1
    if matches>1:
        artists_match.update({artist:">1" })
        print(artist)
    if matches == 0:
        for ind, row in df2.iterrows():
            if str.__contains__(row["names"], artist ):
                artists_match.update({artist:row["artist"]})
                matches+=1
        if matches>1:
            artists_match.update({artist:">1" })
            print(artist)
        if matches == 0:
            artists_match.update({artist:artist })
df["wikidata_artist_id"] = df["artist"].map(artists_match)
df.to_csv("wikiart_images_location2.csv")
"""