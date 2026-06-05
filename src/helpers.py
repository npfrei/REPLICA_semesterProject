"""
Mostly code used for visualization and debugging purposes.
"""
from PIL import Image
import matplotlib.pyplot as plt
import numpy as np
import os   
from src.clusering import IMAGES_DIR_WIKIDATA
import pandas as pd

"""
Display two images next to each other
"""
def display_pair(im1, im2, save_path, title1, title2, title_main):
    f, axarr = plt.subplots(1, 2, figsize=(16, 8))
    image1 = Image.open(im1)
    axarr[0].imshow(image1)
    axarr[0].set_title(title1 + " - " + im1.split("/")[-1])
    axarr[0].set_axis_off()

    image2 = Image.open(im2)
    axarr[1].imshow(image2)
    axarr[1].set_title(title2 + " - " + im2.split("/")[-1])
    axarr[1].set_axis_off()

    f.suptitle(title_main)
    f.savefig(save_path+im1.split("/")[-1].removesuffix(".jpg") + "_" + im2.split("/")[-1].removesuffix(".jpg") + ".jpg")
    plt.close()
"""
Display all images in a given cluster
"""  
def display_cluster(df, id_, eps, title=""):
    files = df[df["cluster_"+ str(eps)]==id_]["label"]
    
    
    images_loc = [(IMAGES_DIR_WIKIDATA+f).removesuffix("npy")+".jpg" if f.startswith("Q") else get_path_wa(f) for f in files ]
    print(images_loc)
    x, y= int(np.ceil(np.sqrt(len(images_loc)))), int(np.ceil(np.sqrt(len(images_loc) ))) if np.sqrt(len(images_loc))%1 >= 0.5 else int(np.floor(np.sqrt(len(images_loc) )))
    if x>0 and y>0:
        f, axarr = plt.subplots(x,y, figsize=(10*x, 10*y))
        k=0
        
        for i in range(x):
            for j in range(y):
                if y>1:
                    if(i+j*y<len(images_loc)):
                        im = images_loc[i+j*y]
                        if im!="" and im.split("/")[-1] in os.listdir("/".join(im.split("/")[:-1])) :
                        
                            image = Image.open(im)
                            axarr[i][j].imshow(image)
                            axarr[i][j].set_title(im.split("/")[-1])
                            axarr[i][j].set_axis_off()
                            k+=1
                else:
                    im = images_loc[i]
                    if im!="" and im.split("/")[-1] in os.listdir("/".join(im.split("/")[:-1]))  :
                        image = Image.open(im)
                        axarr[i].imshow(image)
                        axarr[i].set_title(im.split("/")[-1])
                        axarr[i].set_axis_off()
                        k+=1
        print(k, len(images_loc))             
        if(k>1):
            f.suptitle(title)
            f.savefig(  "../../data/interesting_clusters/"+str(id_)+  "_cluster" + "_"+str(eps) + ".jpg")
            
        plt.close()

"""
Plot different stats about clusters in general
"""
def plot_clusters():
    df_clusters = pd.Dataframe(columns= ["clusters", "images"])
    df = pd.read_csv("dbscan_labels.csv")
    df = df.set_index("label")
    l=[]
    unique_c = set()
    l2=[]
    l3=[]
    l4=[]
    for col in df.columns:
        if col.startswith("clu"):
            print(col)
            l.append(df[col].nunique())
            
            col_fil = df[df[col]!=-1][col]
            unique_c.update(col_fil.unique())
            clusters_sizes = []
            for v in col_fil.unique():
                clusters_sizes.append(len(df[df[col]==v][col]))
            print(min(clusters_sizes))
            l2.append(np.array(clusters_sizes).mean())
            l3.append(max(clusters_sizes))
            l4.append(col_fil.nunique())
        





    print(len(unique_c))
    f, ax = plt.subplots(1,1)
    ax.plot(np.arange(1.0,12.0, step=1), l2)

    plt.title("mean size of clusters")
    plt.xlabel("eps")
    plt.ylabel("mean size of clusters")
    f.savefig("n_clusters_mean.jpg")
    
    f , ax = plt.subplots(1,1)
    ax.plot(np.arange(1.0,12.0, step=1), l3)
    
    plt.title("max size of clusters")
    plt.xlabel("eps")
    plt.ylabel("max size of clusters")
    plt.yscale("log")
    f.savefig("n_clusters_max.jpg")
    f , ax = plt.subplots(1,1)
    ax.plot(np.arange(1.0,12.0, step=1), l4)
    
    plt.title("number of clusters")
    plt.xlabel("eps")
    plt.ylabel("number of clusters")
    
    f.savefig("n_clusters.jpg")

"""
Plot all clusters for a given wikidata id
"""
def plot_clusters_id(id_):
    df = pd.read_csv("dbscan_labels9.csv")
    df.set_index("label", inplace=True)
    curr_cluster = -1
    for col in df.columns:
        if col.startswith("cluster_"):
            
            if curr_cluster!=df[col][id_]:
                curr_cluster =  df[col][id_]
                
                if curr_cluster!=-1:
                    
                    df2 = df[df[col]==curr_cluster]
                    rows = len(df2)//10
                    if rows>1:
                        images = [Image.open(IMAGES_DIR_WIKIDATA +x+".jpg") for x in df2.index[:10*rows] ]
                        max_width = 0
                        new_images = []
                        for i in range(rows):
                            widths, heights = zip(*(i.size for i in images[i*10:(i+1)*10]))
                            
                            total_width = sum(widths)
                            max_width = max(max_width, total_width)
                            mean_height = int(np.mean(heights))
                            mid = mean_height//2
                            new_im = Image.new('RGB', (total_width, mean_height), color=(255, 255, 255))
                            
                            x_offset = 0
                            for im in images[i*10:(i+1)*10]:
                                im2 = im.thumbnail((im.size[0]*mean_height//im.size[1], mean_height))
                                new_im.paste(im, (x_offset,mid - im.size[1]//2))
                                x_offset += im.size[0]
                            new_images.append(new_im)
                        total_height = sum(im.size[1] for im in new_images)
                        final_im = Image.new('RGB', (max_width, total_height), color=(255, 255, 255))
                        y_offset = 0
                        for im in new_images:
                            final_im.paste(im, (0, y_offset))
                            y_offset += im.size[1]
                        final_im.save('test' + col + '.jpg')
                        
                    else:
                        images = [Image.open(IMAGES_DIR_WIKIDATA +x+".jpg") for x in df2.index[:10] ]
                        widths, heights = zip(*(i.size for i in images))

                        total_width = sum(widths)
                        mean_height = int(np.mean(heights))
                        mid = mean_height//2
                        new_im = Image.new('RGB', (total_width, mean_height), color=(255, 255, 255))
                        images = images[:10]
                        x_offset = 0
                        for im in images:
                            im2 = im.thumbnail((im.size[0]*mean_height//im.size[1], mean_height))
                            new_im.paste(im, (x_offset,mid - im.size[1]//2))
                            x_offset += im.size[0]

                        new_im.save('test' + col + id_ + '.jpg')