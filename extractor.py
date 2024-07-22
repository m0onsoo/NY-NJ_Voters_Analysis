import pandas as pd


# Change the path to suit your environment.

klast = pd.read_csv("../Data/name/KSURE_LAST_NAME.csv")
kfirst = pd.read_csv("../Data/name/KLIKE_FIRST_NAME.csv")

klasts = list(klast['LAST_NAME'])
kfirsts = list(kfirst[~kfirst['Real_Name'].str.contains("\*")]['Real_Name'])

num_files = 8
for i in range(0, num_files):
    # 1. 작업할 파일을 부르고 각종 처리

    data = pd.read_csv(f"../Data/org/NYCvoters_{i}.csv") 
    data = data.iloc[:, 1:]

    # Remove rows where firstname does not exist
    data.dropna(subset=['FIRSTNAME', 'STATUS'], inplace=True)

    # Only active people are needed
    data = data[data['STATUS'].str.contains('A')]

    # Eliminate possible Chinese names in advance
    data.drop(data[data['FIRSTNAME'].str.contains("XU|XI|MEI|JIE")].index, inplace = True)

    # data['MIDDLENAME'].fillna('', inplace=True)
    data['F_MODIFIED'] = data['FIRSTNAME']
    data['F_MODIFIED'] = data['F_MODIFIED'].str.replace('-', '', regex=True)
    data['F_MODIFIED'] = data['F_MODIFIED'].str.replace(' ', '', regex=True)

    # Need to indicate by what logic the row was extracted
    # Default initialized to 0
    # When extracted using LAST, FIRST, MIDDLE NAME, 1 is given, and when extracted using address information, 2 is given.
    print(f"File_{i} Processing")
    data['LABEL'] = 0 

    df_else = data.copy()

    # Extracting Koreans based on name
    df_sure = data[data['LASTNAME'].isin(klasts) | data['F_MODIFIED'].isin(kfirsts) | data['MIDDLENAME'].isin(kfirsts)]
    df_sure.loc[:,'LABEL'] = 1 # last & first & middle name
    print(f"File_{i} Last_Frist_Middle Completed")

    # People residing at the same address are also identified as Korean
    # Merge two data frames (inner join)
    df_else = data.drop(df_sure.index, axis=0)
    merged_data = pd.merge(df_sure, df_else, on=['RADDNUMBER','RSTREETNAME', 'RAPARTMENT', 'RCITY', 'RZIP5'], how='inner')
    merged_data = merged_data.dropna(subset=['RADDNUMBER', 'RAPARTMENT'])   # 널값이 있는 행 제외

    addr_ids = list(merged_data['SBOEID_y'])
    df_addr = df_else[df_else['SBOEID'].isin(addr_ids)]  
    df_addr.loc[:,'LABEL'] = 2                         # 2 == address

    # Just combine them and done
    result = pd.concat([df_sure, df_addr], ignore_index=True)

    result.to_csv(f"NY_Koreans_{i}.csv")         
    print(f"File_{i} Extraction Completed")
    print(f"Number of Koreans in {i}th file =", len(result))