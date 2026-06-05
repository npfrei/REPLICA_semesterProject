
import numpy as np
import scipy as sp
import matplotlib.pyplot as plt
import pandas as pd
import os
from sklearn.cluster import DBSCAN
from PIL import Image
from wikiart import get_metadata_from_wikidata_id
import json
import jsondiff as jd
from jsondiff import diff, symbols
from collections import defaultdict


IMAGES_DIR_WIKIDATA = "../../data/images/" #path where images and embeddings are stored


"""
Get files and embeddings from the directory, if a dataframe is provided, only get the files that are in the dataframe.
"""
def get_files(df = None):
    
    files_wd = os.listdir(IMAGES_DIR_WIKIDATA)
    if df is not None:
        files_wd = [f for f in files_wd if f in df["label"].values]
    f_wd = np.array([f for f in files_wd if f.endswith(".npy")])
    i_wd = np.array([f for f in files_wd if f.endswith(".jpg")])
    embeddings_wd = np.array([np.load(IMAGES_DIR_WIKIDATA + f, allow_pickle=True) for f in f_wd])
    return f_wd, embeddings_wd, i_wd

"""
Cluster the embeddings with DBSCAN and save the results in a csv file. If display is True, display the clusters 

"""
def dbscan_clustering(embeddings, filename, eps,display=False):
    
    for i in range(1,len(eps)):
        
        dist = str(eps[i]/10)
        print(i)
        #dist2 = str(eps[i+1]/10)
        
        if display: #creates images for each cluster if display is True, limited to 200 images per cluster to avoid memory issues
            for id_ in df["cluster_"+ dist2].unique():
                if id_!=-1 and len(df[df["cluster_"+ dist2]==id_])<200 :
                    if id_ in df["cluster_"+ dist]:
                        files = df[df["cluster_"+ dist]==id_]
                        files2 = df[df["cluster_"+ dist2]==id_]
                        print(len(files),len(files2))
                        if len(files)!=len(files2):
                            display_cluster(df, id_, eps[i+1]/10)
                    else:
                        display_cluster(df, id_, eps[i+1]/10)
            
        
        clusters = DBSCAN(eps=eps[i]/10, min_samples=2).fit_predict(embeddings)
        df = pd.read_csv(filename)
        df["cluster_" + dist] = clusters
            
        df2 = df[df["cluster_" + dist]!=-1]
        df3 = df2[df2["cluster_" + dist]!=df2["cluster_" + str(eps[i-1]/10)]]
        st = df3.groupby(["cluster_" + dist, "cluster_" + str(eps[i-1]/10)]).count().reset_index()
        cluster_labels = {-1:-1}
        max_ = max(df["cluster_" + str(eps[i-1]/10)])
        for ind in st.index:
            
            
            old = st["cluster_" + str(eps[i-1]/10)][ind]
            new = st["cluster_" + dist][ind]
            if old!=-1:
                
                cluster_labels.update({new:old})
            elif new not in cluster_labels.keys():
                
                max_ +=1
                cluster_labels.update({new:max_})
            
        df2 = df.replace({"cluster_" + dist: cluster_labels})      
        df2.to_csv(filename, index=False)
        print(df2["cluster_" + dist].nunique())
        if df2["cluster_" + dist].nunique()<=1:
            break
    


"""
Returns the number of statements and labels that changed between two jsons that contain the metadata of two Wikidata items.
"""
def compare_jsons(j1, j2):
    
    statements_changes = {}
    labels_changes = {}
    r = diff(j1, j2, syntax="explicit")
    updates = r.get(symbols.update)
    if updates is not None:
        statements = updates.get("statements", {}).get(symbols.update)
        if statements is not None:
            for s, v in statements.items():
                if v.get(0, {}).get(symbols.update, {}).get("value") is not None:
                    for s1, v1 in v[0][symbols.update]["value"].items():
                        if s1==symbols.update:
                            keys = v1.keys()
                            
                            old = v1.get("content")
                            if isinstance(old, dict) and old.get(symbols.update) is not None:
                                old = old.get(symbols.update)
                            
                            new = j1["statements"][s][0]["value"].get("content") 
                            
                            
                            if ((isinstance(old, dict) and not symbols.insert in old.keys()) or not isinstance(old, dict)) and ((isinstance(old, dict) and not symbols.insert in new.keys()) or not isinstance(new, dict)):
                                statements_changes[s] = (old, new)
        
        labels = updates.get("labels", {}).get(symbols.update, {})
        if labels is not None:
            
            
            #print(labels.keys())
            old = labels.values()
            new = [j1["labels"][s2] for s2 in labels.keys()]
            labels_changes = dict(zip(labels.keys(), zip(old, new)))
        
                    
    
    if statements_changes!={}:  #only return something if there is a change in statements             
        
        return (1,statements_changes, labels_changes)
    return (0,{}, {})        
    
"""
For all clusters, get the size, at what eps they appear, what are their subclusters and members and save the results in a csv file.
"""
def get_clusters_description():
    
    
    df = pd.read_csv("dbscan_labels10.csv", index_col=0)
    
    cols = [c for c in df.columns if c.startswith("cluster_")]
    
    seen_clusters = set()
    sizes, first_appearance = {}, {}
    members, subclusters = defaultdict(set), defaultdict(set)
    
    
    
    
    for i in range(len(cols)):
        col = cols[i]
    
        for cluster in df[col].unique():
            if cluster!=-1 :
                
                seen_clusters.add(cluster)
                df_cluster = df[df[col]==cluster][col]
                mb  = set(df_cluster.index)
                
                if i>0:
                    for emb in mb:
                        
                        
                        if df[cols[i-1]][emb]!=-1 and df[cols[i-1]][emb] != cluster:
                            
                            subclusters[cluster].add(df[cols[i-1]][emb])
                
                members[cluster] = mb
                sizes[cluster] = len(df_cluster)
                if cluster not in first_appearance.keys():
                    first_appearance[cluster] = float(cols[i].split("_")[-1])
    out = pd.DataFrame({"cluster": sorted(list(seen_clusters)), "size": [sizes[c] for c in seen_clusters], "first_appearance": [first_appearance[c] for c in seen_clusters], "subclusters": [";".join(str(c) for c in subclusters[m]) for m in seen_clusters ], "members": [";".join(members[c]) for c in seen_clusters]})
    
    out.to_csv("clusters_description.csv", index=False)
    
"""
Used to correct the clusters obtained with DBSCAN, if a cluster appears at a certain eps but not at a smaller one, we check if the members of this cluster belong to the same cluster at the smaller eps, if not we give them a new cluster id.
""" 
def corr_dbscan():
    df = pd.read_csv("dbscan_labels9.csv", index_col=0)
    cols = [c for c in df.columns if c.startswith("cluster_")]
    all_clusters = set()
    for col in cols:
        all_clusters = all_clusters.union(set(df[col].unique()))
    
    seen_clusters = set()
    prev_size = {}
    for i in range(len(cols)):
        col = cols[i]
        
        for cluster in df[col].unique():
            
            if cluster!=-1 :
                seen_clusters.add(cluster)
                df_cluster = df[df[col]==cluster][col]
                mb = set(df_cluster.index)
                if cluster in prev_size.keys() and prev_size[cluster] != len(mb):
                    prev_size[cluster] = len(mb)
                    k = cluster + 1
                    while k in all_clusters:
                        k+=1
                    
                    all_clusters.add(k)
                    for emb in mb:
                        for j in range(i, len(cols)):
                            if df[cols[j]][emb]==cluster:
                                df.loc[df.index==emb, cols[j]] = k
                else:
                    prev_size[cluster] = len(mb)
                            
    
    for col in cols:
        all_clusters = all_clusters.union(set(df[col].unique()))    
    
    df.to_csv("dbscan_labels10.csv", index=True)               
f, e, i = get_files()
print(len(f), len(e))
f = set(f_.removesuffix(".npy") for f_ in f)    
m = set(m_.removesuffix(".json") for m_ in os.listdir("../../data/metadata_same/"))
diff = m.difference(f)
print(len(diff))

for file in diff:
    os.replace(f"../../data/metadata_same/{file}.json", f"../../data/metadata2/{file}.json")

 


    

