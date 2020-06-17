import pandas as pd


df_a = pd.read_csv('./data_source/a_lvr_land_a.csv',skiprows=[1])
df_b = pd.read_csv('./data_source/b_lvr_land_a.csv',skiprows=[1])
df_e = pd.read_csv('./data_source/e_lvr_land_a.csv',skiprows=[1])
df_f = pd.read_csv('./data_source/f_lvr_land_a.csv',skiprows=[1])
df_h = pd.read_csv('./data_source/h_lvr_land_a.csv',skiprows=[1])


df_all = pd.concat([df_a,df_b,df_e,df_f,df_h], axis=0, ignore_index=True)
df_filter_a = df_all.loc[(df_all['主要用途'] == '住家用') & (df_all['建物型態'].str.contains('住宅大樓')) & (df_all['總樓層數'].str.len() > 2) & (df_all['總樓層數'].str.contains('十一層|十二層') == False),:]
df_filter_a.to_csv('./filter_a.csv', index = False)


df_all['車位數'] = df_all['交易筆棟數'].apply(lambda x: int(x[-1]))
df_filter_b = pd.DataFrame(index=range(0),columns=['總件數','總車位數','平均總價元','平均車位總價元'])
df_filter_b.loc[0,'總件數'] = len(df_all)
df_filter_b.loc[0,'總車位數'] = df_all['車位數'].sum()
df_filter_b.loc[0,'平均總價元'] = df_all['總價元'].mean()
df_filter_b.loc[0,'平均車位總價元'] = df_all['車位總價元'].mean()
df_filter_b.to_csv('./filter_b.csv', index = False)