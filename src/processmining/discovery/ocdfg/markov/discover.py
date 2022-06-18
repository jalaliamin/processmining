from processmining.discovery.ocdfg.markov import constants as ocdfgmarkov_const
from processmining.discovery.ocdfg import constants as ocdfg_const

def get_weights(ocdfg):
    oc_weighted_edges = {}
    oc_weighted_edges_outputs = {}
    oc_weighted_edges_input = {}

    for obj in ocdfg[ocdfg_const.lbl_object_types]:
        oc_weighted_edges[obj] = {}
        oc_weighted_edges_outputs[obj] = {}
        oc_weighted_edges_input[obj] = {}
        if obj in ocdfg[ocdfg_const.lbl_edges][ocdfg_const.lbl_event_couples].keys():
            for (e1,e2) in ocdfg[ocdfg_const.lbl_edges][ocdfg_const.lbl_event_couples][obj]:
                ev_list = ocdfg[ocdfg_const.lbl_edges][ocdfg_const.lbl_event_couples][obj][(e1,e2)]
                oc_weighted_edges[obj][(e1,e2)] = len(ev_list)

                if e1 in oc_weighted_edges_outputs[obj].keys():
                    oc_weighted_edges_outputs[obj][e1] += len(ev_list)
                else:
                    oc_weighted_edges_outputs[obj][e1] = len(ev_list)

                if e2 in oc_weighted_edges_input[obj].keys():
                    oc_weighted_edges_input[obj][e2] += len(ev_list)
                else:
                    oc_weighted_edges_input[obj][e2] = len(ev_list)

    return (oc_weighted_edges_input, oc_weighted_edges_outputs, oc_weighted_edges)

def discover_ocdfg_markov(ocdfg):
    (oc_weighted_edges_input, oc_weighted_edges_outputs, oc_weighted_edges) = get_weights(ocdfg)
    oc_weighted_edges_markov = {}

    for obj in oc_weighted_edges.keys():
        oc_weighted_edges_markov[obj] = {}
        for (e1,e2) in oc_weighted_edges[obj]:
            oc_weighted_edges_markov[obj][(e1,e2, ocdfgmarkov_const.lbl_in)] = oc_weighted_edges[obj][(e1,e2)]     /    oc_weighted_edges_input[obj][(e2)]
            oc_weighted_edges_markov[obj][(e1,e2, ocdfgmarkov_const.lbl_out)] = oc_weighted_edges[obj][(e1,e2)]    /    oc_weighted_edges_outputs[obj][(e1)]
            oc_weighted_edges_markov[obj][(e1,e2, ocdfgmarkov_const.lbl_inout)] = oc_weighted_edges[obj][(e1,e2)] / (oc_weighted_edges_input[obj][(e2)] + oc_weighted_edges_outputs[obj][(e1)] )
            
    ocdfg[ocdfgmarkov_const.lbl_approach] = oc_weighted_edges_markov
    return ocdfg
    

def get_probability(ocdfgmkv, obj, a1, a2, dir):
    if not (a1,a2,dir) in ocdfgmkv[ocdfgmarkov_const.lbl_approach][obj].keys():
        return 0
    else:
        return ocdfgmkv[ocdfgmarkov_const.lbl_approach][obj][(a1,a2, dir)]
        
def discover_similarity_matrix(ocdfgmkv, precision_round=2):
    import math
    diff_matrix = {}
    root_matrix = {}
    for dir in [ocdfgmarkov_const.lbl_in, ocdfgmarkov_const.lbl_out, ocdfgmarkov_const.lbl_inout]:
        diff_matrix[dir]={}
        root_matrix[dir]={}
        for ot1 in ocdfgmkv[ocdfgmarkov_const.lbl_approach].keys():
            if not ot1 in root_matrix[dir].keys():
                root_matrix[dir][ot1] = 0
            for ot2 in ocdfgmkv[ocdfgmarkov_const.lbl_approach].keys():
                if not ot2 in root_matrix[dir].keys():
                    root_matrix[dir][ot2] = 0
                diff = 0
                for a1 in ocdfgmkv[ocdfg_const.lbl_activities]:
                    for a2 in ocdfgmkv[ocdfg_const.lbl_activities]:
                        a = get_probability(ocdfgmkv, ot1, a1, a2, dir)
                        b = get_probability(ocdfgmkv, ot2, a1, a2, dir)
                        if ot1==ot2:
                            root_matrix[dir][ot1] += (a**2)
                        diff += (a * b)
                diff_matrix[dir][(ot1,ot2)] = diff

    sim_matrix = {}
    for dir in diff_matrix.keys():
        for (i1,i2) in diff_matrix[dir].keys():
            root = (root_matrix[dir][i1]+root_matrix[dir][i2])/2
            # There might be no event for some object types, so this part will handle such situations
            if root!=0:
                sim_matrix[(i1,i2, dir)]=round((diff_matrix[dir][(i1,i2)]/root),precision_round)
            else:
                if i1==i2:
                    sim_matrix[(i1,i2, dir)] = 1
                else:
                    sim_matrix[(i1,i2, dir)] = 0

    return sim_matrix
    
def filter_matrix(sim_matrix, threshold, dir=ocdfgmarkov_const.lbl_out):
    return {(a,b, dir):v for ((a,b,c),v) in sim_matrix.items() if c==dir and v>=threshold} 
    
    
def discover_clusters(sim_matrix, threshold, dir=ocdfgmarkov_const.lbl_out):
  
  def update_clusters(cluster, index, a, b):    
    if not index in cluster:
      cluster[index] = set()

    cluster[index].add(a)
    cluster[index].add(b)
    return cluster

  def merge_clusters(cluster, index1, index2, a, b):    
    c = len(cluster)
    cluster[c] = set()
    for a in cluster[index1]:
      cluster[c].add(a)
    for a in cluster[index2]:
      cluster[c].add(a)
    cluster.pop(index1)
    cluster.pop(index2)    
    cluster = update_clusters(cluster, c, a, b)
    c = {}
    for i in cluster:
      c[len(c)] = cluster[i]
    return c

  def get_current_clusters_index(cluster, a, b):
    result = set()
    for i in cluster.keys():
      if (a in cluster[i]) or (b in cluster[i]):
        result.add(i)
    return list(result)
    


  matrix = filter_matrix(sim_matrix, threshold, dir)
  cluster = {}

  for ((o1, o2, d), v) in matrix.items():

    indxs = get_current_clusters_index(cluster, o1, o2)

    if indxs==[]: # none of them founded
      cluster = update_clusters(cluster, len(cluster), o1, o2)
    elif len(indxs)==1: # one cluster was founded
      cluster = update_clusters(cluster, indxs[0], o1, o2)
    else: ## two cluster is founded
      cluster = merge_clusters(cluster, indxs[0], indxs[1], o1, o2)
    
  return cluster
  