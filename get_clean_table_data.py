from get_table_data import get_table_data
import pandas as pd
def get_clean_table_data():

    df_art_table, df_ent_table, df_ent_table_norm, df_url_table = get_table_data()
    
    df_art_table["publishedat"] = [d.to_pydatetime().date() for d in df_art_table["publishedat"]]
    df_art_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_art_table["publishedat"]]
    
    df_ent_table["publishedat"] = [d.to_pydatetime().date() for d in df_ent_table["publishedat"]]
    df_ent_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_ent_table["publishedat"]]

    df_ent_table_norm["publishedat"] = [d.to_pydatetime().date() for d in df_ent_table_norm["publishedat"]]
    df_ent_table_norm["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_ent_table_norm["publishedat"]]

    
    df_url_table["publishedat"] = [d.to_pydatetime().date() for d in df_url_table["publishedat"]]
    df_url_table["publishedat"] =  [d.strftime('%d_%m_%Y') if not pd.isnull(d) else '' for d in df_url_table["publishedat"]]
    
    df_art_table["length"] = [len(text) for text in df_art_table["text"]]
    df_ent_table["length"] = [len(text) for text in df_ent_table["text"]]
    df_ent_table_norm["length"] = [len(text) for text in df_ent_table_norm["text"]]

    print("Text cleaned")
    
    return(df_art_table, df_ent_table, df_ent_table_norm, df_url_table)
