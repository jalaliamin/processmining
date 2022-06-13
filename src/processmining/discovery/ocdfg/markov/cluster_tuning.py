from processmining.discovery.ocdfg.markov import constants as ocdfgmarkov_const
import pandas as pd

def similarity_tuning(sim_matrix, dir=ocdfgmarkov_const.lbl_out, threshold=None, clusters=None):
    from processmining.discovery.ocdfg.markov import discover as disc
    if clusters==None:
        clusters={}
        clusters[0] = disc.discover_clusters(sim_matrix, 0, dir)
        clusters[1] = disc.discover_clusters(sim_matrix, 1, dir)
        clusters = similarity_tuning(sim_matrix, threshold=0.5, clusters=clusters)
    elif threshold in clusters.keys():
        return clusters
    else:    
        c = disc.discover_clusters(sim_matrix, threshold, dir)
        clusters[threshold] = c

        upper = min([k for k,v in clusters.items() if k > threshold])
        lower = max([k for k,v in clusters.items() if k < threshold])

        if len(clusters[threshold])!=len(clusters[upper]):
            clusters = similarity_tuning(sim_matrix, threshold=round((threshold+upper)/2,2), clusters=clusters)

        if len(clusters[threshold])!=len(clusters[lower]):
            clusters = similarity_tuning(sim_matrix, threshold=round((threshold+lower)/2,2), clusters=clusters)

    return clusters

def get_optimised_similarity_tuning(sim_matrix, dir=ocdfgmarkov_const.lbl_out):
    tunned_similarity_clusters = similarity_tuning(sim_matrix, dir)
    return {k[0]:tunned_similarity_clusters[k[0]] for v,k in pd.DataFrame({(k,len(v)) for k,v in tunned_similarity_clusters.items()},index=None).groupby(by=[1]).min(0).reset_index().iterrows()}
    