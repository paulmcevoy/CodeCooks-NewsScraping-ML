import pandas as pd
from get_table_data import get_table_data
from get_table_data import get_table_data
#from create_xls_csv import create_xls_csv
from collections import Counter
df_art_table, df_ent_table = get_table_data()

df_art_table["publishedat"] = [d.to_pydatetime().date() for d in df_art_table["publishedat"]]
df_art_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_art_table["publishedat"]]

df_ent_table["publishedat"] = [d.to_pydatetime().date() for d in df_ent_table["publishedat"]]
df_ent_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_ent_table["publishedat"]]

df_art_table["length"] = [len(text) for text in df_art_table["text"]]
df_ent_table["length"] = [len(text) for text in df_ent_table["text"]]
print("Text cleaned")

df_art_snip = df_art_table[:50]
df_ent_snip = df_ent_table[:50]

#df_ents_cut = df_table[['article', 'name', 'url', 'publishedat', 'length', 'source']]
df_ent_table_date = df_ent_table[df_ent_table.publishedat == '07_07_2017']


df_ents_full = df_ent_table_date

from collections import  defaultdict
df_ents_current_dict = defaultdict(list)
df_ents_sim_dict = defaultdict(dict)
df_ents_sim = pd.DataFrame([])
df_ents_data_set = set(df_ents_full['publishedat'])

for date in df_ents_data_set:
    top_ents = []
    df_ents_current = df_ents_full[df_ents_full.publishedat == date]
    print("Date: {} Entities: {}".format(date, len(df_ents_current)))
    counts = Counter(df_ents_current.name)
    for letter, count in counts.most_common(20):
        top_ents.append(letter)
        #print ("%S: %7d" % (letter, count))
    for index, row in df_ents_current.iterrows():
        df_ents_current_dict[row['article']].append(row['name'])
    len_list = []
    for key1, value1 in df_ents_current_dict.items():
        for key2, value2 in df_ents_current_dict.items():
            sim_num = len(set(value1) & set(value2))
            sim_vals = set(value1) & set(value2)
            if sim_num >= 3 and key1 != key2:
                #print("{} {} {} {}\n".format(key1, key2, sim_num, sim_vals)   )
                df_ents_sim_dict[key1][key2] = sim_num
                df_ents_sim = df_ents_sim.append(pd.DataFrame({'article1': key1, 'article2': key2, 'sim_count': sim_num, 'entities': sim_vals}, index=[0]), ignore_index=True)
    article_bin_table = pd.DataFrame(False, index=df_ents_current_dict.keys(), columns=top_ents)
    for ent in top_ents:
        for article in df_ents_current_dict.keys():
            #ent in df_ents_current_dict[article].values()
            if ent in df_ents_current_dict[article]:
                print("Found match!\n UniqueID {}\n Ent {}\n List {}\n".format(article, ent, df_ents_current_dict[article]))
                article_bin_table.loc[article][ent] = True
                
    df_ents_sim.to_csv('df_ents_sim_%s.csv' % date)
    
import numpy as np
from kmodes import kmodes
from sklearn.metrics import jaccard_similarity_score
from sklearn.metrics.pairwise import pairwise_distances
sim_table = (1 - pairwise_distances(article_bin_table, metric = "hamming"))
sim_table_df = pd.DataFrame(sim_table, index=df_ents_current_dict.keys(), columns=df_ents_current_dict.keys())

sim_table_df.to_csv('sim_table_df%s.csv' % date)
np.savetxt("sim_table.csv", sim_table, delimiter=",")


# random categorical data
data = np.random.choice(20, (100, 10))

km = kmodes.KModes(n_clusters=2, init='Huang', n_init=5, verbose=1)

clusters = km.fit_predict(article_bin_table)

# Print the cluster centroids
print(km.cluster_centroids_)
article_bin_table['clusters'] = clusters


import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
pca = PCA(2)

# Turn the dummified df into two columns with PCA
plot_columns = pca.fit_transform(article_bin_table.ix[:,0:19])

# Plot based on the two dimensions, and shade by cluster label
plt.scatter(x=plot_columns[:,1], y=plot_columns[:,0], c=article_bin_table["clusters"], s=30)
#plt.scatter(article_bin_table.col1, df.col2, s=df.col3)

plt.show()


#dummy
#dummy2
#dummy3