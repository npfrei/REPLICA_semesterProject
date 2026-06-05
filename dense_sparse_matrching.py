
from vismatch import get_matcher
from vismatch.viz import  plot_matches
import itertools
import pandas as pd

from skimage.metrics import mean_squared_error, structural_similarity, peak_signal_noise_ratio
import cv2
import numpy as np
import os
from clusering import get_path_wa, IMAGES_DIR_WIKIDATA
import requests
from PIL import Image

img_size = 512  # optional

def compute_image_matching():
    matcher = get_matcher("superpoint-lightglue", device="cuda")
    
    df = pd.read_csv("dbscan_labels6.csv")

    d = pd.read_csv("similarity7.csv")
    d = d.set_index("Unnamed: 0")
   

    d = dict(zip(d.index, d.values))
    df_wa = pd.read_csv("images_with_location.csv")
    df_wa["title"] = df_wa["title"].apply(lambda x : x.lower().replace(" ", "-"))
    
    for col in df.columns:
        if col.startswith("clu"):
        
            col_fil = df[df[col]!=-1]
            g = col_fil.groupby(col)["label"].apply(lambda x: x)
            l = []
            #calculate pairwise similarity for each image pair in the same cluster using superpoint-lightglue matcher and store results in dictionary
            for i,j in g.index:
                if i in l:
                    continue
                l.append(i)
                images_loc = [(IMAGES_DIR_WIKIDATA+f).removesuffix("npy")+"jpg" for f in g[i].values]
                print(i)
                if len(images_loc) < 2 or len(images_loc) > 50:
                    print(f"Skipping cluster {i} with {len(images_loc)} images.")
                    continue
                for idx1 in range(len(images_loc)):
                    try:
                        img0 = matcher.load_image(images_loc[idx1], resize=img_size)
                    except Exception as e:
                        print(e)
                        continue
                        
                                
                    for idx2 in range(idx1 + 1, len(images_loc)):
                        if "(\'"+images_loc[idx1] + "\', \'" + images_loc[idx2]+"\')" in d.keys() or "(\'"+images_loc[idx2] + "\', \'" + images_loc[idx1]+"\')" in d.keys():
                            print("S")
                            continue
                        try:
                            img1 = matcher.load_image(images_loc[idx2], resize=img_size)
                        except Exception as e:
                            print(e)
                            continue
                    
                        #match points using superpoint-lightglue
                        result = matcher(img0, img1)
                        #calulate similarity ratio based on number of inliers and total macthed points
                        ratio =result["num_inliers"] / max(1, result["all_kpts0"].shape[0])
                        #store similarity and other relevant information in dictionary for later analysis for each image pair
                        d["(\'"+images_loc[idx1] + "\', \'" + images_loc[idx2]+"\')"] = [ratio, result["num_inliers"], result["H"], result["all_kpts0"].shape[0], result["all_kpts1"].shape[0], result["all_desc0"].shape[0], result["all_desc1"].shape[0], result["matched_kpts0"].shape[0], result["matched_kpts1"].shape[0], result["inlier_kpts0"].shape[0], result["inlier_kpts1"].shape[0]]
            
            pd.DataFrame.from_dict(d, orient="index",   columns=[ "similarity", "num_inliers", "H", "all_kpts0", "all_kpts1", "all_desc0", "all_desc1", "matched_kpts0", "matched_kpts1", "inlier_kpts0", "inlier_kpts1"]).to_csv("similarity8.csv")




def get_same_images(df_name, threshold=0.97, min_matches=100):
    
    df = pd.read_csv(df_name)

    df["inlier_ratio"] = df["inlier_kpts0"] / df["matched_kpts0"]
    df["same"] = (df["inlier_ratio"] >= threshold) &(df["matched_kpts0"] > min_matches) 
    df["close"] = ((df["inlier_ratio"] >= 0.8) & (df["matched_kpts0"] > 50)) & ~(df["same"])
    df["close_same_matches"] = (df["matched_kpts0"] > 300) & (df["inlier_ratio"] < 0.96) & df["same"]
    df["close_ratio"] = (df["inlier_ratio"] >= 0.88) & (df["matched_kpts0"] <= 300) & (df["inlier_ratio"] < 0.96) & ~df["close"]


    print(df["same"].value_counts())
    print(df["close"].value_counts())
    print(df["close_same_matches"].value_counts())
    print(df["close_ratio"].value_counts())
    print((df["inlier_ratio"]).describe())
    df.to_csv(df_name.replace(".csv", "_filtered.csv"), index=False)
    


def main():
    """
    compute_image_matching()
    get_same_images("similarity8.csv", threshold=0.97, min_matches=100)
    """
    df = pd.read_csv("similarity9_filtered.csv")
    df = df[df["same"]==True]
    df2 = pd.read_csv("duplicates.csv")
    df["item1"] = df["Unnamed: 0"].apply(lambda x : x.split(",")[0].replace("(", "").replace("\'", "").strip())
    df["item2"] = df["Unnamed: 0"].apply(lambda x : x.split(",")[1].replace(")", "").replace("\'", "").strip())
    for idx, row in df.iterrows():
        item1 = row["item1"]
        item2 = row["item2"]
        if item1 not in df2["item1"].values and item1 not in df2["item2"].values and item2 not in df2["item1"].values and item2 not in df2["item2"].values:
            df2 = pd.concat([df2, pd.DataFrame({"item1": [item1], "item2": [item2], "label":"same"})], ignore_index=True)
    print(len(df2))
main()