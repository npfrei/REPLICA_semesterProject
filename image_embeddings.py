import requests
from io import BytesIO
from PIL import Image
import torch
import hashlib
from wikidata_api import BASE_URL_WIKIDATA, BearerAuth
#wikidata access token and headers for API requests, stored in config.py for security and modularity, need to be requested from https://meta.wikimedia.org/wiki/Special:OAuthConsumerRegistration/propose/oauth2
from config import wikidata_access_token, headers
from torchvision.transforms import v2
import pandas as pd
import dask.dataframe as dd
from dask import delayed
import os
from time import sleep
import numpy as np
import json
MODEL_NAME = "dinov3_vitl16"
LOCAL_REPO_PATH = "../../data/dinov3"
WEIGHT_PATH = r"../../data/dinov3_vitl16_pretrain_lvd1689m-8aa4cbdd.pth" # Needs to be requested from Meta (no automatic download)

"""
Fonction used to load the DINOV3 model from local weights
Returns : model loaded on CUDA
"""
def load_model():


    model = torch.hub.load(
        repo_or_dir=LOCAL_REPO_PATH,
        model=MODEL_NAME,
        source="local",
        weights=WEIGHT_PATH
    )
    model.cuda()

    return model
"""
Function to create the necessary transformations for the input images before feeding them to the model. The transformations include resizing, normalization, and conversion to tensor format. 
Returns : a composed transformation that can be applied to the input images.
"""
def make_transform(resize_size: int = 256):
    to_tensor = v2.ToImage()
    resize = v2.Resize((resize_size, resize_size), antialias=True)
    to_float = v2.ToDtype(torch.float32, scale=True)
    normalize = v2.Normalize(
        mean=(0.485, 0.456, 0.406),
        std=(0.229, 0.224, 0.225),
    )
    return v2.Compose([to_tensor, resize, to_float, normalize])

#url to wikimedia commons images to fetch images
base_image_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/"


#link to the wikidata api to fetch the image link for a given item
api = "https://www.wikidata.org/w/rest.php/wikibase/v1/file/"
"""
Function to get an image from Wikimedia Commons based on the image name
Returns : PIL Image object
"""
def get_image(image_name):
    #computes hashes for the image location on wikimedia commons based on the image name
    hsh = hashlib.md5(image_name.encode()).hexdigest()
    a,ab = hsh[0], hsh[:2]
    #compute the url for the image based on the naming convention of wikimedia commons, with a specific handling for tiff images which have a different url structure and are more likely to cause issues with fetching and processing
    if image_name.endswith(".tif") or image_name.endswith(".tiff"):
        image_url =  base_image_url + a + "/" + ab + "/" + image_name +"/lossy-page1-500px-" + image_name +".jpg"
    else:
        image_url = base_image_url + a + "/" + ab + "/" + image_name +"/512px-" + image_name
    try:
        #fetch the image from the computed url using requests, with error handling for potential issues such as network errors or issues with the image format. The headers are included to respect the API usage policies and avoid potential blocking.
        response = requests.get(image_url, headers=headers)
        response.raise_for_status() # Raise an exception for HTTP errors
        image = Image.open(BytesIO(response.content))
        print("Image loaded successfully using PIL.")
        sleep(0.8) # Sleep to respect rate limits
    except requests.exceptions.RequestException as e:
        
        try:
            #Try fetching image again replacing 512px by 500px as certain images (mostly thumbnails) have different allowed sizes
            response = requests.get(image_url.replace("512px", "500px"), headers=headers)
            response.raise_for_status() # Raise an exception for HTTP errors
            image = Image.open(BytesIO(response.content))
            print("Image loaded successfully using PIL.")
            sleep(0.8)
        except Exception as e:
            print(f"Error fetching image from URL: {e}")
            # Fallback or exit if image cannot be fetched
            raise
    except Image.UnidentifiedImageError as e:
        print(f"PIL could not identify the image format: {e}")
        # Fallback or exit if image format is not recognized
        raise
    return image
def get_embeddings(id_series, model_arg): # Rename model to model_arg to avoid conflict with outer scope 'model'
    embeddings_list = []
    # Ensure model is on CUDA (it should be from load_model, but good to ensure for map_partitions context)
    model_arg.eval() # Set model to evaluation mode
    model_arg.cuda() # Ensure model is on CUDA for each worker

    for item_url in id_series:
        image_id_suffix = item_url.split("/")[-1] # Extract suffix for each item
        if image_id_suffix + ".jpg" not in os.listdir("../../data/images/"): # Check if image already exists locally
            try:
                image = get_image(get_image_name(image_id_suffix))
                image = image.convert("RGB") # Ensure image is in RGB format
                image.save( f"../../data/images/{image_id_suffix}.jpg") # Save image locally for reference
            except Exception as e:
                print(f"Error fetching image: {e}")
                sleep(2.0) # Sleep to respect rate limits before next attempt
                continue
            
        else:
            image = Image.open(f"../../data/images/{image_id_suffix}.jpg") # Load image from local storages
        if image_id_suffix + ".npy" not in os.listdir("../../data/images/"):
            
            with torch.inference_mode():
                with torch.autocast('cuda', dtype=torch.bfloat16):
                    batch_img = make_transform()(image)[None]
                    batch_img = batch_img.to('cuda').to(dtype=torch.bfloat16) # Move to CUDA and convert to bfloat16
                    outputs = model_arg(batch_img)
                    # For DINOv3, the model likely returns the pooled output directly
                    np.save(f"../../data/images/{image_id_suffix}.npy", outputs.cpu().numpy().flatten()) # Save embedding to text file for future use
                    embeddings_list.append((image_id_suffix,outputs.cpu().numpy().flatten())) # Flatten and append
        else:
            embeddings_list.append((image_id_suffix, np.load(f"../../data/images/{image_id_suffix}.npy"))) # Load embedding from numpy file if it exists        
    return pd.Series(embeddings_list, index=id_series.index)
"""
Function to get the image name from Wikidata's API based on the item ID 
Returns : link to the image on Wikimedia Commons
"""
def get_image_name(id):
    
    params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/" + id, params=params, auth=BearerAuth(wikidata_access_token))
    response.raise_for_status()
    with open(f"../../data/metadata_same/{id}.json", "w") as f:
        json.dump(response.json(), f)
    image_name = response.json().get("statements", {"P18": [-2]}).get("P18",  [-1])[0].get("value", {"content":""}).get("content", "").replace(" ", "_")
    return image_name





def get_embeddings_single(image, model_arg):
        with torch.inference_mode():
            with torch.autocast('cuda', dtype=torch.bfloat16):
                batch_img = make_transform()(image)[None]
                batch_img = batch_img.to('cuda').to(dtype=torch.bfloat16) # Move to CUDA and convert to bfloat16
                outputs = model_arg(batch_img)
                return outputs.cpu().numpy().flatten() # Flatten and return embedding
    
def main():
    files = os.listdir("../../data/images/")
    files = [f.removesuffix(".jpg") for f in files if f.endswith(".jpg")]
    model = load_model()

    df = pd.read_csv("images.csv") #dataframe with the ID of each Wikidata item

    df = df[int(len(df)/2):]

    dfd = dd.from_pandas(df, npartitions=2) #split dataframe into 2 partitions to parallelize the embedding computation
    # The meta for map_partitions should describe the output of the function, which is a pandas Series of numpy arrays.
    # Need to create a dummy input to infer the shape of the embedding array.
    with torch.inference_mode():
        with torch.autocast('cuda', dtype=torch.bfloat16):
            # Create a dummy image, transform it, and pass it through the model to get output shape
            dummy_image = Image.new('RGB', (256, 256)) # Example size
            dummy_batch_img = make_transform()(dummy_image)[None].to('cuda').to(dtype=torch.bfloat16)
            dummy_output = model(dummy_batch_img)
            dummy_embedding_shape = dummy_output.cpu().numpy().flatten().shape
    # Create a meta Series containing an array of the correct shape and type
    meta = pd.Series([np.zeros(dummy_embedding_shape, dtype=np.float32)], dtype=object)
    emb = dfd.item.map_partitions(get_embeddings, model_arg=delayed(load_model)(), meta=meta)
    emb.to_csv("../../data/images_with_embeddings.csv", index=False)