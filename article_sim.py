import pandas as pd
from get_clean_table_data import get_clean_table_data
#from create_xls_csv import create_xls_csv
from collections import Counter
df_art_table, df_ent_table = get_clean_table_data()

df_art_snip = df_art_table[:50]
df_ent_snip = df_ent_table[:50]

#df_ents_cut = df_table[['article', 'name', 'url', 'publishedat', 'length', 'source']]
df_ent_table_date = df_ent_table[(df_ent_table.publishedat == '15_07_2017') | (df_ent_table.publishedat == '14_07_2017') | (df_ent_table.publishedat == '13_07_2017') | (df_ent_table.publishedat == '12_07_2017') | (df_ent_table.publishedat == '11_07_2017')]
df_art_table_date = df_art_table[df_art_table.publishedat == '05_07_2017']

df_ents_full = df_ent_table_date

from collections import  defaultdict
df_ents_current_dict = defaultdict(list)
df_ents_sim_dict = defaultdict(dict)
df_ents_sim = pd.DataFrame([])
top_ents = pd.DataFrame([])
top_ents_df = pd.DataFrame([])
ent_slice = pd.DataFrame([])
ent_slice_score_df = pd.DataFrame([])
ent_slice_score = defaultdict(dict)
top_ents_dict = defaultdict(dict)
df_ents_current_slice = pd.DataFrame([])
df_ents_current_test = defaultdict(list)

df_ents_data_set = set(df_ents_full['publishedat'])

for date in df_ents_data_set:
    
    top_ents = []
    top_counts = []
    ent_slice_score_lst = []
    df_ents_current = df_ents_full[df_ents_full.publishedat == date]
    print("Date: {} Entities: {}".format(date, len(df_ents_current)))
    counts = Counter(df_ents_current.name)
    loop_count = 0
    for letter, count in counts.most_common(50):
        top_ents.append(letter)
        top_counts.append(count)
        #print ("%S: %7d" % (letter, count))
        #top_ents_df = top_ents_df.append(pd.DataFrame({'ent': letter, 'count': count}, index=[0]), ignore_index=True)
        top_ents_dict[letter]= count
    top_ents_df = pd.DataFrame({'ents': top_ents,'counts': top_counts})
    for ent in top_ents:
        ent_slice = df_ents_current[df_ents_current.name == ent]
        ent_slice_score[ent] = ent_slice['score'].sum()
        ent_slice_score_lst.append(ent_slice['score'].sum())

        #ent_slice_score_df = ent_slice_score_df.append(pd.DataFrame({'ent': ent, 'sum': ent_slice['score'].sum()}, index=[0]), ignore_index=True)
    ent_slice_score_df = pd.DataFrame({'ents': top_ents,'adj_counts': ent_slice_score_lst})
    top_ents_adj = ent_slice_score_df.sort_values('adj_counts', ascending=False)
    top_ents_adj = top_ents_adj[:30]

    for i, j in zip(df_ents_current.article,df_ents_current.name):
        #print("i: {} j: {}".format(i,j))
        df_ents_current_dict[i].append(j)

    #for index, row in df_ents_current.iterrows():
    #    df_ents_current_dict[row['article']].append(row['name'])
    len_list = []
    for key1, value1 in df_ents_current_dict.items():
        for key2, value2 in df_ents_current_dict.items():
            sim_num = len(set(value1) & set(value2))
            sim_vals = set(value1) & set(value2)
            if sim_num >= 3 and key1 != key2:
                #print("{} {} {} {}\n".format(key1, key2, sim_num, sim_vals)   )
                df_ents_sim_dict[key1][key2] = sim_num
                df_ents_sim = df_ents_sim.append(pd.DataFrame({'article1': key1, 'article2': key2, 'sim_count': sim_num, 'entities': sim_vals}, index=[0]), ignore_index=True)
    article_bin_table = pd.DataFrame(False, index=df_ents_current_dict.keys(), columns=top_ents_adj.ents)
    for ent in top_ents_adj.ents:
        for article in df_ents_current_dict.keys():
            #ent in df_ents_current_dict[article].values()
            if ent in df_ents_current_dict[article]:
                #print("Found match!\n UniqueID {}\n Ent {}\n List {}\n".format(article, ent, df_ents_current_dict[article]))
                article_bin_table.loc[article][ent] = True
                
    df_ents_sim.to_csv('df_ents_sim_%s.csv' % date)
    top_ents_adj.to_csv('top_ents%s.csv' % date)
    #ent_slice_score_df.to_csv('ent_slice_score_df%s.csv' % date)
    article_bin_table_keep = article_bin_table.loc[(article_bin_table != False).any(axis=1),:]
    article_bin_table_keep['sum'] = article_bin_table.sum(axis=1)
    
    for index, row in article_bin_table_keep.iterrows():
        if row['sum'] < 3:
            article_bin_table_keep.drop(index, inplace=True)

    import unicodecsv as csv
    with open('df_ents_current_dict%s.csv'  % date, 'wb') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in df_ents_current_dict.items():
            writer.writerow([key, value])

    import numpy as np
    from kmodes import kmodes
    from sklearn.metrics import jaccard_similarity_score
    from sklearn.metrics.pairwise import pairwise_distances
    sim_table = (1 - pairwise_distances(article_bin_table_keep, metric = "hamming"))
    #sim_table_df = pd.DataFrame(sim_table, index=df_ents_current_dict.keys(), columns=df_ents_current_dict.keys())
    
    km = kmodes.KModes(n_clusters=5, init='Huang', n_init=5, verbose=0)
    clusters = km.fit_predict(article_bin_table_keep)
    print(km.cluster_centroids_)
    article_bin_table_keep['clusters'] = clusters
    
    import matplotlib.pyplot as plt
    from sklearn.decomposition import PCA
    pca = PCA(2)
    plot_columns = pca.fit_transform(article_bin_table_keep.ix[:,0:29])
    plt.scatter(x=plot_columns[:,1], y=plot_columns[:,0], c=article_bin_table_keep["clusters"], s=30)
    plt.savefig('sim_scatter%s.png' %date)

    plt.show()
#dummy
#dummy2
#dummy3