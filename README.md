# Clustering and duplicates identification in Artworks on Wikidata

This github hosts the code used to generate the dataset at 10.5281/zenodo.20270374, to identify duplicate items and potential errors in Wikidata paintings, by using clustering and point matching on the images of the paintings.

### Files

All python files used in this project are located in the src folder

*wikidata_api.py :* Used to get images with a precise geolocation using Wikidata's API. A config.py with authentication tokens (which need to be requested [here](https://www.wikidata.org/wiki/Wikidata:REST_API/Authentication)) from Wikidata are needed to run it.

*image_embeddings.py :* Used to compute the embedding for each image using DINOV3

*clusering.py* : Used to calculate the clusters for the embeddings

*dense_sparse_matching.py :* Used to calculate point matching to find duplicate images

*helpers.py* : Helper functions used mainly to vizualize clusters automatically

Requirements for a python enviroment can be found in *requirements.txt*
