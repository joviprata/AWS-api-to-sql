#Importar Bibliotecas:
#AWS SDKs:
import logging
import boto3
from botocore.exceptions import ClientError

#HTTP requests:
import requests
import json

#Definir data:
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import calendar

#Salvar arquivos CSV:
import csv
from pathlib import Path
import io

#Carregar variáveis de ambiente:
import os
from dotenv import load_dotenv

#Mesclar tabelas:
import pandas as pd

#Pegar dados paralelamente:
import aiohttp
import asyncio
import nest_asyncio


#Carregar credenciais:
load_dotenv()
long_lived_user_access_token = os.getenv('long_lived_user_access_token')
account_id = os.getenv('account_id')
account_username = os.getenv('account_username')
endpoint_base = os.getenv('graph_domain') + os.getenv('graph_version') + '/'

#Conectar Notebook ao S3 Bucket:
AWS_PROFILE_NAME = os.getenv('AWS_PROFILE_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('MY_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
session = boto3.Session(profile_name=AWS_PROFILE_NAME) #Obs: omitir profile_name = AWS_PROFILE_NAME ao passar para AWS Lambda
s3 = session.client('s3')


def dfs(dados, path_atual=None):
    '''Realiza uma busca de profundidade (DFS) e gera uma lista com caminhos para todos os nós folha de uma árvore de dados, e os valores desses nós. \n
    Parameters:
        dados(dict or list): dicionário com estrutura semelhante a uma árvore, contendo sub-dicionários ou sub-listas;
        path_atual(list): lista cuja sequência de elementos corresponde ao caminho percorrido pela função.
        
    Returns:
        resultado(list): lista de tuplas no formato (chave, valor).'''

    if path_atual is None:
        path_atual = []

    resultado = []

    if isinstance(dados, dict):
        for chave, valor in dados.items():
            path_atual.append(chave)
            if isinstance(valor, (dict, list)):
                resultado.extend(dfs(valor, path_atual))
            else:
                resultado.append(("_".join(path_atual), valor))
            path_atual.pop()

    elif isinstance(dados, list):
        for i, elemento in enumerate(dados):
            path_atual.append(f'{i + 1}')
            if isinstance(elemento, (dict, list)):
                resultado.extend(dfs(elemento, path_atual))
            else:
                resultado.append(("_".join(path_atual), elemento))
            path_atual.pop()

    return resultado


fields_posts = ['timestamp', 'media_type', 'permalink', 'thumbnail_url', 'media_url', 'id', 'caption']
fields_stories = fields_posts

metrics_acc = ['impressions', 'profile_views', 'reach']
metrics_posts = ['impressions', 'reach', 'shares', 'saved', 'comments', 'video_views', 'likes'] #impressions parou de funcionar (shadow ban?)] #mas às vezes volta #video_views serve tanto pra media type CAROUSSEL ALBUM, VIDEO E IMAGE. Também funciona para reels. Para imagens e carossel albums, o videoviews é zerado 
metrics_stories = ['impressions', 'reach', 'shares', 'saved']

breakdown_demographics = ['age', 'city', 'country', 'gender']


#criar um df pandas com UIDs e dimensões de todas as mídias
#percorrer cada linha dessa tabela, usando o ID e a data no url para obter as métricas
#acrescentar à tabela os valores encontrados correspondentes a essa mídia

#for tabela in tabelas:
print(endpoint_base)


#Definir data de hoje:
data_epoch_str = os.getenv('data_epoch_str')
data_inicial = datetime.utcnow().replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
data_inicial_str = data_inicial.isoformat()
print(data_inicial_str)

#TABELA INSTAGRAM STORIES:

#dicionário de posts, só com i_name e i_values_1_value, e sem fields:
dicionario_stories = {'1_name': 'str', '1_values_1_value': 'str', '2_name': 'str', '2_values_1_value': 'str', '3_name': 'str', '3_values_1_value': 'str', '4_name': 'str', '4_values_1_value': 'str', }


stories_df = pd.DataFrame(columns = fields_posts + list(dicionario_stories.keys()) )

def lambda_handler(event, context):
after = None
achou_ultima_pagina = False
while not achou_ultima_pagina:
    url = endpoint_base + account_id + '/' + 'stories'
    params = {
        'access_token': long_lived_user_access_token,
        'fields': ','.join(fields_posts),
        'limit': 500,
        'period': 'day',
        'since': data_inicial_str
    }
    
    if after:
        params['after'] = after
        
    response = requests.get(url, params=params)
    results = response.json()
    data = results['data']
    
    # Criar um DataFrame temporário com os novos dados
    temp_df = pd.DataFrame.from_records(data)
    
    # Concatenar o DataFrame temporário ao DataFrame principal
    stories_df = pd.concat([stories_df, temp_df], ignore_index=True)
    
    if 'paging' in results and 'next' in results['paging']:
        after = results["paging"]['cursors']['after']
        print(f"Page {after} processing...")
    else:
        achou_ultima_pagina = True

stories_df['unique_identifier'] = stories_df['timestamp'] + stories_df['id']
stories_df['content_type'] = 'STORY'
print(len(stories_df), 'items found.')
display(stories_df)


for i, story_id in enumerate(stories_df['id']):
    url =  endpoint_base + story_id + '/' + 'insights'
    params = {
        'access_token': long_lived_user_access_token,
        'metric': ','.join(metrics_stories),
        'period': 'day',
        'since': data_inicial_str
    }
    
    
    response = requests.get(url, params=params)
    results = response.json()
    try:
        dados = results['data']
    except:
        print(f'Erro ao obter dados no story com ID {story_id} - muito recente, ou outro motivo. Passando para o próximo story...')
        print(results)
        continue
    dicionario_item = dict(dfs(dados))
    for chave in dicionario_item:
        if chave in dicionario_stories:
            stories_df.at[i, chave] = dicionario_item[chave]

display(stories_df)

#TABELA INSTAGRAM POSTS:

#dicionário de posts, só com i_name e i_values_1_value, e sem fields:
dicionario_posts = {'1_name': 'str', '1_values_1_value': 'str', '2_name': 'str', '2_values_1_value': 'str', '3_name': 'str', '3_values_1_value': 'str', '4_name': 'str', '4_values_1_value': 'str','5_name': 'str', '5_values_1_value': 'str','6_name': 'str', '6_values_1_value': 'str', '7_name': 'str', '7_values_1_value': 'str'}


posts_df = pd.DataFrame(columns = fields_posts + list(dicionario_posts.keys()) )

after = None
achou_ultima_pagina = False
while not achou_ultima_pagina:
    url = endpoint_base + account_id + '/' + 'media'
    params = {
        'access_token': long_lived_user_access_token,
        'fields': ','.join(fields_posts),
        'limit': 500,
        'period': 'day',
        'since': data_inicial_str
    }
    
    if after:
        params['after'] = after
        
    response = requests.get(url, params=params)
    results = response.json()
    data = results['data']
    
    # Criar um DataFrame temporário com os novos dados
    temp_df = pd.DataFrame.from_records(data)
    
    # Concatenar o DataFrame temporário ao DataFrame principal
    posts_df = pd.concat([posts_df, temp_df], ignore_index=True)
    
    if 'paging' in results and 'next' in results['paging']:
        after = results["paging"]['cursors']['after']
        print(f"Page {after} processing...")
    else:
        achou_ultima_pagina = True

posts_df['unique_identifier'] = posts_df['timestamp'] + posts_df['id']
posts_df['content_type'] = 'POST'

print(len(posts_df), 'posts found.')


for i, post_id in enumerate(posts_df['id']):
    url =  endpoint_base + post_id + '/' + 'insights'
    params = {
        'access_token': long_lived_user_access_token,
        'metric': ','.join(metrics_posts),
        'period': 'day',
        'since': data_inicial_str
    }
    
    
    response = requests.get(url, params=params)
    results = response.json()
    dados = results['data']
    
    dicionario_item = dict(dfs(dados))
    for chave in dicionario_item:
        if chave in dicionario_posts:
            posts_df.at[i, chave] = dicionario_item[chave]

display(posts_df)


stories_df = stories_df.astype(str)
posts_df = posts_df.astype(str)
content_df = pd.concat([posts_df, stories_df], ignore_index=True)
content_df.replace('nan', '', inplace=True)
display(content_df)


csv_string_ig_content = content_df.to_csv(index=False)

# Send to S3:
s3.put_object(Body=csv_string_ig_content.encode('utf-8'), Bucket=BUCKET_NAME, Key='database_ig_content.csv')
print('Dados enviados para S3 com sucesso.')

#TABELA INSTAGRAM CONTA:

#dicionário de account, só com i_name, i_values_1_value e i_values_1_end_time (i_values_1_ é correspondente ao dia de ontem):
dicionario_account_ontem = {'1_name': 'str', '1_values_1_value': 'str', '1_values_1_end_time': 'str', '2_name': 'str', '2_values_1_value': 'str', '2_values_1_end_time': 'str', '3_name': 'str', '3_values_1_value': 'str', '3_values_1_end_time': 'str'}
#dicionário de account, só com i_name, i_values_2_value e i_values_2_end_time (i_values_2_ é correspondente ao dia de hoje):
dicionario_account_hoje = {'1_name': 'str', '1_values_2_value': 'str', '1_values_2_end_time': 'str', '2_name': 'str', '2_values_2_value': 'str', '2_values_2_end_time': 'str', '3_name': 'str', '3_values_2_value': 'str', '3_values_2_end_time': 'str'}

dicionario_account = {**dicionario_account_ontem, **dicionario_account_hoje}
print(dicionario_account)


data_atual = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
data_atual_str = data_atual.isoformat()

data_ontem = data_atual - timedelta(days=1)
data_ontem_str = data_ontem.isoformat()

url = endpoint_base + account_id + '/' + 'insights'

params = {
    'access_token': long_lived_user_access_token,
    'metric': ','.join(metrics_acc),
    'limit': 500,
    'period': 'day',
    'since': data_ontem_str
}

response = requests.get(url, params=params)
results = response.json()
dados = results['data']

dicionario_item = dict(dfs(dados))

dicionario_item_tratado = {}
for chave in dicionario_item:
    if chave in dicionario_account:
        if '_values_2' in chave:
            chave_ontem = chave[:9] + '1' + chave[10:]
            dicionario_item_tratado[chave_ontem] = [dicionario_item[chave_ontem], dicionario_item[chave]]
            
        else:
            dicionario_item_tratado[chave] = [dicionario_item[chave], dicionario_item[chave]]


account_df = pd.DataFrame(dicionario_item_tratado)
account_df['timestamp'] = account_df['1_values_1_end_time']
display(account_df)


dicionario_followers = {'current_day': 'str', 'followers_count': 'str'}


url = endpoint_base + account_id

params = {
    'access_token': long_lived_user_access_token,
    'fields': f'business_discovery.username({account_username})' + '{followers_count}', #','.join(metrics_acc),
}

response = requests.get(url, params=params)
results = response.json()
dados = results['business_discovery']

dicionario_item = dict(dfs(dados))

dicionario_item_tratado = {}
for chave in dicionario_item:
    if chave in dicionario_followers:
        dicionario_item_tratado[chave] = dicionario_item[chave]

dicionario_item_tratado['timestamp'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')[:11] + '07:00:00+0000'

followers_df = pd.DataFrame([dicionario_item_tratado])
display(followers_df)


account_df = account_df.astype(str)
followers_df = followers_df.astype(str)
account_detailed_df = account_df.merge(followers_df, on='timestamp', how='outer')
display(account_detailed_df)


csv_string_ig_account = account_detailed_df.to_csv(index=False)

# Send to S3:
s3.put_object(Body=csv_string_ig_account.encode('utf-8'), Bucket=BUCKET_NAME, Key='database_ig_account.csv')
print('Dados enviados para S3 com sucesso.')

#TABELA CALENDÁRIO;

# Generate date range from 1970-01-01 to today
data_epoch_str = '1970-01-01T00:00:00'
dates = pd.date_range(start=data_epoch_str, end=data_atual_str)

# Format dates as strings in the required format
formatted_dates = [date.strftime('%Y-%m-%dT%H:%M:%S+0000') for date in dates]

# Create DataFrame
calendar_df = pd.DataFrame({'date': formatted_dates})

# Display the DataFrame (optional)
display(calendar_df)


csv_string_calendar = calendar_df.to_csv(index=False)

# Send to S3:
s3.put_object(Body=csv_string_calendar.encode('utf-8'), Bucket=BUCKET_NAME, Key='database_calendar.csv')
print('Dados enviados para S3 com sucesso.')

#TABELA DEMOGRAPHICS:

def criar_chaves_repetidas(qtde, lista_chaves):
    dicionario_resultante = {}
    for i in range(1, qtde + 1):
        for chave in lista_chaves:
            dicionario_resultante[chave.replace('{i}', str(i))] = 'str'
    return dicionario_resultante


breakdown_demographics = ['age', 'city', 'country', 'gender']
lista_chaves_demographics = [
    '1_total_value_breakdowns_1_results_{i}_dimension_values_1',
    '1_total_value_breakdowns_1_results_{i}_value'
]

dicionario_demographics = {'age': criar_chaves_repetidas(8, lista_chaves_demographics),
                           'city': criar_chaves_repetidas(50, lista_chaves_demographics),
                           'country': criar_chaves_repetidas(250, lista_chaves_demographics), 
                           'gender': criar_chaves_repetidas(3, lista_chaves_demographics)
}
dicionario_demographics_tratado = {'country': 'str', 'country_value': 'str', 'city': 'str', 'city_value': 'str', 'state': 'str', 'state_value': 'str', 'gender': 'str', 'gender_value': 'str', 'age': 'str', 'age_value': 'str', 'timestamp': 'str', 'unique_identifier': 'str'}

demographics_df_full = pd.DataFrame([dicionario_demographics_tratado])
demographics_df_full = demographics_df_full.drop(0)

current_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')[:11] + '07:00:00+0000'
for data_type in breakdown_demographics:
    #print(data_type)
    url = endpoint_base + account_id + '/' + 'insights'
    
    params = {
        'access_token': long_lived_user_access_token,
        'metric': 'follower_demographics',
        'limit': 500,
        'period': 'lifetime',
        #'since': data_ontem_str
        'metric_type': 'total_value',
        'breakdown': data_type
    }
    
    response = requests.get(url, params=params)
    results = response.json()
    print(results)
    dados = results['data']
    
    dicionario_item = dict(dfs(dados))
    #print(dicionario_item)
    
    dicionario_item_tratado = {}
    for chave in dicionario_item:
        if chave in dicionario_demographics[data_type]:
            dicionario_item_tratado[chave] = dicionario_item[chave]

    #if data_type == 'city':
    #    dicionario_item_tratado['state'] = 'str'
    #    dicionario_item_tratado['state_value'] = 'str'
            
    #print(dicionario_item_tratado)
    demographics_df = pd.DataFrame([dicionario_item_tratado])
    demographics_df['date'] = current_date
    demographics_df_tratado = pd.DataFrame({data_type: [], f'{data_type}_value': [], 'timestamp': []})
    
    for i, coluna in enumerate(demographics_df):
        
        if i % 2 == 0:
            continue
        if coluna != 'timestamp':
            new_row_demographics_df_tratado = {data_type: demographics_df.iloc[0, i - 1], f'{data_type}_value': str(demographics_df.iloc[0, i]), 'timestamp': current_date,
                                              'unique_identifier': current_date + data_type + demographics_df.iloc[0, i - 1]}
            #if data_type == 'city':
                #print(new_row_demographics_df_tratado[data_type])
                #city, state = new_row_demographics_df_tratado[data_type].split(', ')
                #new_row_demographics_df_tratado[data_type] = city
                
                #state = state.replace(' (state)', '')
                #new_row_demographics_df_tratado['state'] = state
                #new_row_demographics_df_tratado['state_value'] += int(new_row_demographics_df_tratado['city_value'])
                
            demographics_df_tratado.loc[len(demographics_df_tratado)] = new_row_demographics_df_tratado
            demographics_df_full.loc[len(demographics_df_full)] = new_row_demographics_df_tratado
            
    display(demographics_df_tratado)
display(demographics_df_full)


states = {}

for i, row in demographics_df_full.iterrows():
    if pd.notna(row['city']):
        #display(row)
        state = row['city'].split(', ')[1].replace(' (state)', '')
        if state in states:
            states[state] += int(row['city_value'])
        else:
            states[state] = int(row['city_value'])


print(states)


for state in states:
    new_row_demographics_df_tratado = {'state': state, 'state_value': str(states[state]), 'timestamp': current_date,
                                      'unique_identifier': current_date + 'state' + state}
    
    demographics_df_full.loc[len(demographics_df_full)] = new_row_demographics_df_tratado
display(demographics_df_full)


csv_string_demographics = demographics_df_full.to_csv(index=False)

# Send to S3:
s3.put_object(Body=csv_string_demographics.encode('utf-8'), Bucket=BUCKET_NAME, Key='database_demographics.csv')
print('Dados enviados para S3 com sucesso.')
