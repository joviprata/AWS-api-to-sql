#Import libraries:
#AWS SDKs:
import logging
import boto3
from botocore.exceptions import ClientError

#HTTP requests:
import requests
import json

#Set date:
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import calendar

#Save CSV files:
import csv
from pathlib import Path
import io

#Load environment variables:
import os
from dotenv import load_dotenv

#Merge tables:
import pandas as pd

#Make async API requests:
import aiohttp
import asyncio
import nest_asyncio


#Define function that processes data from HTTP Response:
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

#Define function that checks if a column has a specific word:
def contem_palavra(input_string, palavra):
    palavras = input_string.split('_')
    return palavra in palavras

#Define function that checks async job status:
async def check_async_job_status(report_run_id):
    url = f"https://graph.facebook.com/v20.0/{report_run_id}"
    params = {
        'access_token': long_lived_user_access_token
    }
    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get(url, params=params) as response:
                job_status = await response.json()
                try:
                    status = job_status['async_status']
                except:
                    raise RuntimeError(job_status)
                if status == 'Job Completed':
                    return True
                if status == 'Job Skipped':
                    print('Job Skipped')
                    return False
                if status == 'Job Failed':
                    return False
                await asyncio.sleep(0.1)  # Esperar 0.1 segundos para buscar novamente, evitando erro de muitos requests simultâneos na API.

#Define function that gets the final result of the async search:
async def fetch_async_job_result(report_run_id, params):
    url = f"https://graph.facebook.com/v20.0/{report_run_id}/insights"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            return await response.json()

#Define function that processes data:
def process_data(itens_tratados):
    valores_padrao = {'str': '', 'int': '', 'float': '', 'bool': False}
    processed_data = []

    for dicionario_venda in itens_tratados:
        dicionario_tratado = {}
        
        for chave in dicionario_padrao:
            if chave in dicionario_venda:
                if dicionario_padrao[chave] == 'float':
                    dicionario_tratado[chave] = float(dicionario_venda[chave])
                elif contem_palavra(chave, 'date') and dicionario_venda[chave] == 0:
                    dicionario_tratado[chave] = None
                else:
                    dicionario_tratado[chave] = dicionario_venda[chave]
            else:
                dicionario_tratado[chave] = valores_padrao[dicionario_padrao[chave]]
        
        str_unique_identifier = ''.join([dicionario_tratado[uid] for uid in unique_identifiers])
        dicionario_tratado['unique_identifier'] = str_unique_identifier
        processed_data.append(dicionario_tratado)
    
    return processed_data
    
    
#Load credentials:
load_dotenv()
long_lived_user_access_token = os.getenv('long_lived_user_access_token')
account_id = os.getenv('account_id')

#Connect to S3 Bucket:
AWS_PROFILE_NAME = os.getenv('AWS_PROFILE_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('MY_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
session = boto3.Session() #Obs: ommit "profile_name = AWS_PROFILE_NAME" when using this code in AWS Lambda
s3 = session.client('s3')

#Define which columns to get from the API:
total_fields = [
    "action_values",
    "actions",
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "attribution_setting",
    "buying_type",
    "campaign_id",
    "campaign_name",
    "canvas_avg_view_percent",
    "canvas_avg_view_time",
    "catalog_segment_value",
    "clicks",
    "conversion_rate_ranking",
    "conversion_values",
    "conversions",
    "converted_product_quantity",
    "converted_product_value",
    "cost_per_action_type",
    "cost_per_conversion",
    "cost_per_estimated_ad_recallers",
    "cost_per_inline_link_click",
    "cost_per_inline_post_engagement",
    "cost_per_outbound_click",
    "cost_per_thruplay",
    "cost_per_unique_action_type",
    "cost_per_unique_click",
    "cost_per_unique_inline_link_click",
    "cost_per_unique_outbound_click",
    "cpc",
    "cpm",
    "cpp",
    "ctr",
    "date_start",
    "date_stop",
    "dda_results",
    "engagement_rate_ranking",
    "estimated_ad_recall_rate",
    "estimated_ad_recallers",
    "frequency",
    "full_view_impressions",
    "full_view_reach",
    "impressions",
    "inline_link_click_ctr",
    "inline_link_clicks",
    "inline_post_engagement",
    "instant_experience_clicks_to_open",
    "instant_experience_clicks_to_start",
    "instant_experience_outbound_clicks",
    "mobile_app_purchase_roas",
    "objective",
    "optimization_goal",
    "outbound_clicks",
    "outbound_clicks_ctr",
    "place_page_name",
    "purchase_roas",
    "qualifying_question_qualify_answer_rate",
    "quality_ranking",
    "reach",
    "social_spend",
    "spend",
    "total_postbacks_detailed",
    "total_postbacks_detailed_v4",
    "video_30_sec_watched_actions",
    "video_avg_time_watched_actions",
    "video_p100_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "video_play_actions",
    "video_play_curve_actions",
    "website_ctr",
    "website_purchase_roas",
    "account_currency",
    "account_id",
    "account_name",
]

#Define unique identifiers that will be used together to distinguish rows in the SQL table:
unique_identifiers = ['campaign_name', 'adset_name', 'ad_name', 'date_start', 'date_stop']

#DFS-generated dictionary obtained from API request, used as standard structure for allocating upcoming data from API:
dicionario_padrao = {'date_start': 'str', 'date_stop': 'str', 'actions_1_action_type': 'str', 'actions_1_value': 'str', 'actions_2_action_type': 'str', 'actions_2_value': 'str', 'actions_3_action_type': 'str', 'actions_3_value': 'str', 'actions_4_action_type': 'str', 'actions_4_value': 'str', 'actions_5_action_type': 'str', 'actions_5_value': 'str', 'actions_6_action_type': 'str', 'actions_6_value': 'str', 'actions_7_action_type': 'str', 'actions_7_value': 'str', 'actions_8_action_type': 'str', 'actions_8_value': 'str', 'actions_9_action_type': 'str', 'actions_9_value': 'str', 'actions_10_action_type': 'str', 'actions_10_value': 'str', 'ad_id': 'str', 'ad_name': 'str', 'adset_id': 'str', 'adset_name': 'str', 'buying_type': 'str', 'campaign_id': 'str', 'campaign_name': 'str', 'clicks': 'str', 'conversion_rate_ranking': 'str', 'cost_per_action_type_1_action_type': 'str', 'cost_per_action_type_1_value': 'str', 'cost_per_action_type_2_action_type': 'str', 'cost_per_action_type_2_value': 'str', 'cost_per_action_type_3_action_type': 'str', 'cost_per_action_type_3_value': 'str', 'cost_per_action_type_4_action_type': 'str', 'cost_per_action_type_4_value': 'str', 'cost_per_action_type_5_action_type': 'str', 'cost_per_action_type_5_value': 'str', 'cost_per_action_type_6_action_type': 'str', 'cost_per_action_type_6_value': 'str', 'cost_per_action_type_7_action_type': 'str', 'cost_per_action_type_7_value': 'str', 'cost_per_action_type_8_action_type': 'str', 'cost_per_action_type_8_value': 'str', 'cost_per_action_type_9_action_type': 'str', 'cost_per_action_type_9_value': 'str', 'cost_per_action_type_10_action_type': 'str', 'cost_per_action_type_10_value': 'str', 'cost_per_inline_link_click': 'str', 'cost_per_inline_post_engagement': 'str', 'cost_per_thruplay_1_action_type': 'str', 'cost_per_thruplay_1_value': 'str', 'cost_per_unique_action_type_1_action_type': 'str', 'cost_per_unique_action_type_1_value': 'str', 'cost_per_unique_action_type_2_action_type': 'str', 'cost_per_unique_action_type_2_value': 'str', 'cost_per_unique_action_type_3_action_type': 'str', 'cost_per_unique_action_type_3_value': 'str', 'cost_per_unique_action_type_4_action_type': 'str', 'cost_per_unique_action_type_4_value': 'str', 'cost_per_unique_action_type_5_action_type': 'str', 'cost_per_unique_action_type_5_value': 'str', 'cost_per_unique_action_type_6_action_type': 'str', 'cost_per_unique_action_type_6_value': 'str', 'cost_per_unique_click': 'str', 'cost_per_unique_inline_link_click': 'str', 'cpc': 'str', 'cpm': 'str', 'cpp': 'str', 'ctr': 'str', 'engagement_rate_ranking': 'str', 'frequency': 'str', 'full_view_impressions': 'str', 'full_view_reach': 'str', 'impressions': 'str', 'inline_link_click_ctr': 'str', 'inline_link_clicks': 'str', 'inline_post_engagement': 'str', 'objective': 'str', 'optimization_goal': 'str', 'quality_ranking': 'str', 'reach': 'str', 'social_spend': 'str', 'spend': 'str', 'video_30_sec_watched_actions_1_action_type': 'str', 'video_30_sec_watched_actions_1_value': 'str', 'video_avg_time_watched_actions_1_action_type': 'str', 'video_avg_time_watched_actions_1_value': 'str', 'video_p100_watched_actions_1_action_type': 'str', 'video_p100_watched_actions_1_value': 'str', 'video_p25_watched_actions_1_action_type': 'str', 'video_p25_watched_actions_1_value': 'str', 'video_p50_watched_actions_1_action_type': 'str', 'video_p50_watched_actions_1_value': 'str', 'video_p75_watched_actions_1_action_type': 'str', 'video_p75_watched_actions_1_value': 'str', 'video_p95_watched_actions_1_action_type': 'str', 'video_p95_watched_actions_1_value': 'str', 'video_play_actions_1_action_type': 'str', 'video_play_actions_1_value': 'str', 'video_play_curve_actions_1_action_type': 'str', 'video_play_curve_actions_1_value_1': 'int', 'video_play_curve_actions_1_value_2': 'int', 'video_play_curve_actions_1_value_3': 'int', 'video_play_curve_actions_1_value_4': 'int', 'video_play_curve_actions_1_value_5': 'int', 'video_play_curve_actions_1_value_6': 'int', 'video_play_curve_actions_1_value_7': 'int', 'video_play_curve_actions_1_value_8': 'int', 'video_play_curve_actions_1_value_9': 'int', 'video_play_curve_actions_1_value_10': 'int', 'website_ctr_1_action_type': 'str', 'website_ctr_1_value': 'str', 'account_id': 'str', 'account_name': 'str'}
#Columns with up to 10 repetitions may have more repetitions; I set a limit of up to 10 in the DFS function used for generating this


#Get list of campaigns:
effective_status = ['ACTIVE', 'PAUSED', 'PENDING_REVIEW', 'DISAPPROVED', 'PREAPPROVED', 'PENDING_BILLING_INFO', 'CAMPAIGN_PAUSED', 'ARCHIVED', 'ADSET_PAUSED', 'IN_PROCESS', 'WITH_ISSUES']
campaign_ids = []

after = None
achou_ultima_pagina = False
while not achou_ultima_pagina:
    url = f"https://graph.facebook.com/v20.0/act_{account_id}/campaigns"
    params = {
        'date_preset': 'maximum',
        'access_token': long_lived_user_access_token,
        'fields': 'name',
        'effective_status': json.dumps(effective_status),
        'limit': 500
    }
    
    if after:
        params['after'] = after
        
    response = requests.get(url, params=params)
    results = response.json()
    data = results['data']
    campaign_ids.extend(campaign['id'] for campaign in data)
    
    if 'paging' in results and 'next' in results['paging']:
        after = results["paging"]['cursors']['after']
        print(f"Page {after} processing...")
    else:
        achou_ultima_pagina = True

campaign_ids = list(set(campaign_ids))
print(len(campaign_ids), 'unique campaigns received.')


conn = aiohttp.TCPConnector(limit_per_host=10, keepalive_timeout=910)
nest_asyncio.apply()

all_data = []
empty_campaigns = []
async def fetch_campaign_data(session, campaign_id, semaphore, timeout=910):
    async with semaphore:
        itens_tratados = []
        after = None
        achou_ultima_pagina = False
        
        try:
            while not achou_ultima_pagina:
                url = f"https://graph.facebook.com/v20.0/act_{account_id}/insights"
                params = {
                    'time_increment': 1,
                    'date_preset': 'this_year',
                    'level': 'ad',
                    'fields': ','.join(total_fields),
                    'access_token': long_lived_user_access_token,
                    'limit': 500,
                    'filtering': json.dumps([{'field': 'campaign.id', 'operator': 'IN', 'value': [campaign_id]}])
                }
    
                if after:
                    params['after'] = after
    
                async with session.post(url, params=params, timeout=timeout) as response:
                    response_json = await response.json()
                    print(response_json)
                    report_run_id = response_json['report_run_id']
    
                    if await check_async_job_status(report_run_id):
                        results = await fetch_async_job_result(report_run_id, params)
                        if 'paging' in results and 'next' in results['paging']:
                            after = results["paging"]['cursors']['after']
                            print(f"{campaign_id}'s page {after} processing...")
                        else:
                            achou_ultima_pagina = True
                    else:
                        raise RuntimeError('Facebook Ads Async Job did not complete successfully')
    
                    try:
                        itens = results['data']
                    except:
                        raise RuntimeError(results)
    
                    for dados in itens:
                        dicionario_venda = dict(dfs(dados))
                        itens_tratados.append(dicionario_venda)
    
            if not itens_tratados:
                print(campaign_id, 'empty.')
                empty_campaigns.append(campaign_id)
            else:
                print(f'{campaign_id} contains {len(itens_tratados)} items.')
    
            return itens_tratados
        
        except Exception as e:
            print(f"Error fetching data for campaign {campaign_id}: {e}")
            return e

async def main():
    semaphore = asyncio.Semaphore(10)  # Limit the number of concurrent requests
    async with aiohttp.ClientSession(trust_env=True, connector=conn) as session:
        tasks = [fetch_campaign_data(session, campaign_id, semaphore) for campaign_id in campaign_ids[50:100]]
        try:
            all_itens_tratados = await asyncio.gather(*tasks, return_exceptions=True)
    
            for result in all_itens_tratados:
                if isinstance(result, Exception):
                    raise result  # Re-raise the exception to handle it outside gather
            
            for itens_tratados in all_itens_tratados:
                if not isinstance(itens_tratados, Exception):
                    all_data.extend(process_data(itens_tratados))
            print(f'All {len(campaign_ids)} campaigns fully processed.')
            print(f'{len(empty_campaigns)} empty campaigns.')
    
            df_facebook_ads = pd.DataFrame(all_data)
            sorted_df_facebook_ads = df_facebook_ads.sort_values(by=['date_start', 'date_stop', 'campaign_name', 'adset_name', 'ad_name'])
            csv_string_facebook_ads = sorted_df_facebook_ads.to_csv(index=False)
    
            # Send to S3:
            s3.put_object(Body=csv_string_facebook_ads.encode('utf-8'), Bucket=BUCKET_NAME, Key='database_facebook_ads.csv')
            print('Dados enviados para S3 com sucesso')
            
        except Exception as e:
            print(f"An error occurred: {e}")
            # Optionally, handle or log the exception here
            raise

def lambda_handler(event, context):
    asyncio.run(main())
    return {
        'statusCode': 200,
        'body': 'Dados enviados para S3 com sucesso'
    }
