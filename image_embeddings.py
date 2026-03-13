import pipreqs
import requests
from io import BytesIO
from PIL import Image
import torch
import hashlib
from wikiart import BASE_URL_WIKIDATA, BearerAuth, wikidata_access_token
from torchvision.transforms import v2
import pandas as pd
import dask.dataframe as dd
from dask import delayed
import dask
from time import sleep
import numpy as np
MODEL_NAME = "dinov3_vitl16"
LOCAL_REPO_PATH = "..\..\data\dinov3"
WEIGHT_PATH = r"..\..\data\dinov3_vitl16_pretrain_lvd1689m-8aa4cbdd.pth" # Needs to be requested from Meta (no automatic download)


def load_model():


    model = torch.hub.load(
        repo_or_dir=LOCAL_REPO_PATH,
        model=MODEL_NAME,
        source="local",
        weights=WEIGHT_PATH
    )
    model.cuda()

    return model
def make_transform(resize_size: int = 256):
    to_tensor = v2.ToImage()
    resize = v2.Resize((resize_size, resize_size), antialias=True)
    to_float = v2.ToDtype(torch.float32, scale=True)
    normalize = v2.Normalize(
        mean=(0.485, 0.456, 0.406),
        std=(0.229, 0.224, 0.225),
    )
    return v2.Compose([to_tensor, resize, to_float, normalize])


base_image_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/"
headers = {'User-Agent': 'Néhémie_Frei/0.0 (nehemie.frei@epfl.ch)'}
api = "https://www.wikidata.org/w/rest.php/wikibase/v1/file/"

def get_image(image_name):
    hsh = hashlib.md5(image_name.encode()).hexdigest()
    a,ab = hsh[0], hsh[:2]
    image_url = base_image_url + a + "/" + ab + "/" + image_name +"/512px-" + image_name
    try:
        response = requests.get(image_url, headers=headers)
        response.raise_for_status() # Raise an exception for HTTP errors
        image = Image.open(BytesIO(response.content))
        print("Image loaded successfully using PIL.")
        sleep(1.4) # Sleep to respect rate limits
    except requests.exceptions.RequestException as e:
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
        image = get_image(get_image_name(image_id_suffix))

        with torch.inference_mode():
            with torch.autocast('cuda', dtype=torch.bfloat16):
                batch_img = make_transform()(image)[None]
                batch_img = batch_img.to('cuda').to(dtype=torch.bfloat16) # Move to CUDA and convert to bfloat16
                outputs = model_arg(batch_img)
                # For DINOv3, the model likely returns the pooled output directly
                embeddings_list.append((image_id_suffix,outputs.cpu().numpy().flatten())) # Flatten and append
    return pd.Series(embeddings_list, index=id_series.index)
def get_image_name(id):
    params ={ "format":"json"}
    response = requests.get(BASE_URL_WIKIDATA + "entities/items/" + id, params=params, auth=BearerAuth(wikidata_access_token))
    response.raise_for_status()
    return response.json().get("statements", {"P18": [-2]}).get("P18",  [-1])[0].get("value", {"content":""}).get("content", "").replace(" ", "_")

model = load_model()
df = pd.read_csv("images_with_owner3.csv")[:100]
dfd = dd.from_pandas(df, npartitions=2)
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

