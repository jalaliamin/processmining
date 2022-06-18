from processmining.discovery.ocdfg.markov import constants as ocdfgmarkov_const
import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd 
    
def get_similarity_plot(sim_matrix, dir=ocdfgmarkov_const.lbl_out, annotation=False, chart_style='default', size=(8,6)):    
    import seaborn as sns
    style.use(chart_style)

    df_similarity_metrics = pd.Series(sim_matrix).reset_index()
    df_similarity_metrics.columns = ['Object Type', 'Object Type ', 'Direction', 'Similarity']        

    fig, axs = plt.subplots(ncols=1, gridspec_kw=dict(width_ratios=[1]), figsize=size)
    
    df_in = df_similarity_metrics[df_similarity_metrics['Direction']==dir].pivot_table(index='Object Type',columns='Object Type ', values='Similarity', aggfunc='sum')

    
    cmap = sns.cm.rocket_r
    svm = sns.heatmap(df_in, annot=annotation, fmt=".2f", linewidths=.5 , ax=axs, vmin = 0.0, vmax = 1.0, cmap = cmap)
    svm.figure.tight_layout()

    svm.set(xlabel='', ylabel='')

    
    return svm.get_figure() 
    #axs.set_title('Similarity based on ' + dir + ' ')
        
    #return 

def get_similarity_tuning_plot(tunned_similarity_clusters, chart_style='default', size=(6, 5)):

    style.use(chart_style)
    df_viz = pd.DataFrame([(k, len(v)) for k,v in tunned_similarity_clusters.items()]).rename(columns={0: "threshold", 1: "NumberOfCluster"}).sort_values(by=['threshold']).reset_index(drop=True)

    threshold = df_viz.threshold
    NumberOfCluster = df_viz.NumberOfCluster
    fig = plt.figure(figsize = size)
    plt.clf()

    plt.plot(threshold, NumberOfCluster,'b-')

    previous_y=0
    for x,y in zip(threshold, NumberOfCluster):

        label = "{:.2f}".format(x)

        if ((previous_y!=y)):
            plt.annotate(label, (x,y), textcoords="offset points", xytext=(0,3), ha='center') 
        previous_y = y

    plt.xlabel("Threshold")
    plt.ylabel("Number Of Cluster")
    plt.title("Cluster tuning based on threshold")
    #plt.show()
    return fig