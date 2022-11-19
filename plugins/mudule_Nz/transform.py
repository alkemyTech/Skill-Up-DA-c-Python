import pandas as pd
from datetime import datetime, date 
from pathlib import Path
from typing import TypeVar, List

PathLike = TypeVar("PathLike", str, Path, None)

def std_transform(
    dfPath: PathLike = None,
    bornColname: str = 'birth',
    dateBornSchema: str = None,
    inscriptionDateSchema: str = None,
    minAge: int = 17,
    maxAge: int = 82,
    strCols: List[str]=['last_name','email','university','career','location'],
    pathPostalCode: PathLike = None,
    yyBornDate: bool = False,
    pCode: str = 'codigo_postal',
    lCode: str = 'localidad',
    target_file: str = ''
    ):
    ############################load Dataframe############################################
    df = pd.read_csv(dfPath,encoding="utf-8")
    
    ############################set date in model yy/.. example 65/3/27###################
    if yyBornDate:
        df[bornColname] = df[bornColname].map(lambda x: '19'+ x if int(x[0:2])>20  else '20'+ x)
    
    ############################age calculus##############################################
    
    def age(born): 
        born = datetime.strptime(born,dateBornSchema).date() 
        today = date.today() 
        return today.year - born.year - ((today.month,today.day) < (born.month,born.day)) 
    df['age'] = df[bornColname].apply(age)
    
    df = df.drop([bornColname], axis=1)
    
    ############################drop Unnamed: 0 if exist###################################
    
    if 'Unnamed: 0' in df.columns:
        df = df.drop(['Unnamed: 0'], axis=1)
    
    ############################normalize inscription_date##################################
    
    df = df[df['age'] >= minAge]
    df = df[df['age'] <= maxAge]
    
    ############################normalize inscription_date##################################
    
    df['inscription_date'] = pd.to_datetime(df['inscription_date'],format=inscriptionDateSchema)
    
    ############################normalize string column#####################################
    
    for i in strCols:
        if i in df.columns:
            df[i] = df[i].apply(lambda x : ' '.join(x.lower().replace('-', ' ').split()))
    
    ############################join csv-sql and csv-postal_code############################
    
    cp = pd.read_csv(pathPostalCode)
    cp[lCode] = cp[lCode].apply(lambda x : ' '.join(x.lower().replace('-', '').split()))

    if 'location' not in df.columns and 'postal_code' in df.columns:
        
        df = pd.merge(left=df,right=cp, left_on='postal_code', right_on=pCode).rename(columns={'localidad':'location'})
        df['location'] = df['location'].apply(lambda x : ' '.join(x.lower().replace('-', '').split()))
        df = df.drop([pCode], axis=1)
        
    if 'location' in df.columns and 'postal_code' not in df.columns:
        
        df = pd.merge(left=df,right=cp, left_on='location', right_on=lCode).rename(columns={'codigo_postal':'postal_code'})
        df = df.drop([lCode], axis=1)
        
    ############################categorizing gender#########################################
    
    df['gender'] = df['gender'].map(lambda x : 'female' if x == 'F' else 'male').astype("category") 
    
    df.to_csv(target_file, header=True, index=None, sep='\t', mode='a')
    
    return df