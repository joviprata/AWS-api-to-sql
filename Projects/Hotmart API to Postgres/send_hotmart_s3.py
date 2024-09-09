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

#Save CSV files:
import csv
from pathlib import Path
import io

#Load environment variables:
import os
from dotenv import load_dotenv

#Merge tables:
import pandas as pd


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


#Load credentials:
load_dotenv()
client_id_dev = os.getenv('client_id_dev')
client_secret_dev = os.getenv('client_secret_dev')
basic_dev = os.getenv('basic_dev')

#Connect to S3 Bucket:
AWS_PROFILE_NAME = os.getenv('AWS_PROFILE_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('MY_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

session = boto3.Session() #Obs: ommit "profile_name = AWS_PROFILE_NAME" when using this code in AWS Lambda
s3 = session.client('s3')


#Run code on AWS Lambda:
def lambda_handler(event, context):
    #Get access token:
    url = 'https://api-sec-vlc.hotmart.com/security/oauth/token'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': basic_dev
    }
    params = {
        'grant_type': 'client_credentials',
        'client_id': client_id_dev,
        'client_secret': client_secret_dev
    }
    response = requests.post(url, headers=headers, params=params)
    access_token_dev = response.json()['access_token']


    #Set date for Sales History:
    data_epoch_str = os.getenv('data_epoch_str')
    data_inicial = datetime.utcnow().replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    #Obs: I'm using utcnow because Hotmart's API uses UTC for its dates
    data_inicial_str = data_inicial.isoformat()

    data_final = data_inicial.replace(year=data_inicial.year + 1)
    data_final_str = data_final.isoformat()
    #Obs: date strings should be in the format 'yyyy-MM-ddTHH:mm:ss' (Ex: '2024-01-01T00:00:00')

    data_epoch = datetime.strptime(data_epoch_str, '%Y-%m-%dT%H:%M:%S')
    data_inicial = datetime.strptime(data_inicial_str, '%Y-%m-%dT%H:%M:%S')
    data_final = datetime.strptime(data_final_str, '%Y-%m-%dT%H:%M:%S')

    delta_inicial = int((data_inicial - data_epoch).total_seconds() * 1000)
    delta_final = int((data_final - data_epoch).total_seconds() * 1000)


    #Make different requests to get all possible information from the API:
    consultas = ['history', 'summary', 'users', 'commissions', 'price_details']
    status_transacoes = ['APPROVED', 'BLOCKED', 'CANCELLED', 'CHARGEBACK', 'COMPLETE', 'EXPIRED',
                         'NO_FUNDS', 'OVERDUE', 'PARTIALLY_REFUNDED', 'PRE_ORDER', 'PRINTED_BILLET',
                         'PROCESSING_TRANSACTION', 'PROTESTED', 'REFUNDED', 'STARTED', 'UNDER_ANALISYS', 'WAITING_PAYMENT']
    
    dicionarios_padrao_consultas = {
        "history": {'purchase_payment_method': 'str', 'purchase_payment_type': 'str', 'purchase_payment_installments_number': 'int', 'purchase_hotmart_fee_currency_code': 'str', 'purchase_hotmart_fee_base': 'float', 'purchase_hotmart_fee_total': 'float', 'purchase_hotmart_fee_percentage': 'float', 'purchase_hotmart_fee_fixed': 'int', 'purchase_is_subscription': 'bool', 'purchase_warranty_expire_date': 'int', 'purchase_approved_date': 'int', 'purchase_tracking_source': 'str', 'purchase_tracking_external_code': 'str', 'purchase_tracking_source_sck': 'str', 'purchase_price_value': 'float', 'purchase_price_currency_code': 'str', 'purchase_commission_as': 'str', 'purchase_recurrency_number': 'int', 'purchase_offer_code': 'str', 'purchase_offer_payment_mode': 'str', 'purchase_transaction': 'str', 'purchase_order_date': 'int', 'purchase_status': 'str', 'buyer_name': 'str', 'buyer_email': 'str', 'buyer_ucode': 'str', 'producer_name': 'str', 'producer_ucode': 'str', 'product_id': 'int', 'product_name': 'str'},
        "summary": {'total_value_value': 'float', 'total_value_currency_code': 'str', 'total_items': 'int', 'purchase_status': 'str'},
        "users": {'product_name': 'str', 'product_id': 'int', 'transaction': 'str', 'users_1_role': 'str',  'users_1_user_ucode': 'str',  'users_1_user_locale': 'str',  'users_1_user_name': 'str',  'users_1_user_trade_name': 'str',  'users_1_user_cellphone': 'str',  'users_1_user_phone': 'str',  'users_1_user_email': 'str',  'users_1_user_documents_1_value': 'str',  'users_1_user_documents_1_type': 'str',  'users_1_user_documents_2_value': 'str',  'users_1_user_documents_2_type': 'str',  'users_1_user_address_city': 'str',  'users_1_user_address_state': 'str',  'users_1_user_address_country': 'str',  'users_1_user_address_zip_code': 'str',  'users_1_user_address_address': 'str',  'users_1_user_address_complement': 'str',  'users_1_user_address_neighborhood': 'str',  'users_1_user_address_number': 'str',  'users_2_role': 'str',  'users_2_user_ucode': 'str',  'users_2_user_locale': 'str',  'users_2_user_name': 'str',  'users_2_user_trade_name': 'str',  'users_2_user_cellphone': 'str',  'users_2_user_phone': 'str',  'users_2_user_email': 'str',  'users_2_user_documents_1_value': 'str',  'users_2_user_documents_1_type': 'str',  'users_2_user_documents_2_value': 'str',  'users_2_user_documents_2_type': 'str',  'users_2_user_address_city': 'str',  'users_2_user_address_state': 'str',  'users_2_user_address_country': 'str',  'users_2_user_address_zip_code': 'str',  'users_2_user_address_address': 'str',  'users_2_user_address_complement': 'str',  'users_2_user_address_neighborhood': 'str',  'users_2_user_address_number': 'str',  'users_3_role': 'str',  'users_3_user_ucode': 'str',  'users_3_user_locale': 'str',  'users_3_user_name': 'str',  'users_3_user_trade_name': 'str',  'users_3_user_cellphone': 'str',  'users_3_user_phone': 'str',  'users_3_user_email': 'str',  'users_3_user_documents_1_value': 'str',  'users_3_user_documents_1_type': 'str',  'users_3_user_documents_2_value': 'str',  'users_3_user_documents_2_type': 'str',  'users_3_user_address_city': 'str',  'users_3_user_address_state': 'str',  'users_3_user_address_country': 'str',  'users_3_user_address_zip_code': 'str',  'users_3_user_address_address': 'str',  'users_3_user_address_complement': 'str',  'users_3_user_address_neighborhood': 'str',  'users_3_user_address_number': 'str',  'users_4_role': 'str',  'users_4_user_ucode': 'str',  'users_4_user_locale': 'str',  'users_4_user_name': 'str',  'users_4_user_trade_name': 'str',  'users_4_user_cellphone': 'str',  'users_4_user_phone': 'str',  'users_4_user_email': 'str',  'users_4_user_documents_1_value': 'str',  'users_4_user_documents_1_type': 'str',  'users_4_user_documents_2_value': 'str',  'users_4_user_documents_2_type': 'str',  'users_4_user_address_city': 'str',  'users_4_user_address_state': 'str',  'users_4_user_address_country': 'str',  'users_4_user_address_zip_code': 'str',  'users_4_user_address_address': 'str',  'users_4_user_address_complement': 'str',  'users_4_user_address_neighborhood': 'str',  'users_4_user_address_number': 'str',  'users_5_role': 'str',  'users_5_user_ucode': 'str',  'users_5_user_locale': 'str',  'users_5_user_name': 'str',  'users_5_user_trade_name': 'str',  'users_5_user_cellphone': 'str',  'users_5_user_phone': 'str',  'users_5_user_email': 'str',  'users_5_user_documents_1_value': 'str',  'users_5_user_documents_1_type': 'str',  'users_5_user_documents_2_value': 'str',  'users_5_user_documents_2_type': 'str',  'users_5_user_address_city': 'str',  'users_5_user_address_state': 'str',  'users_5_user_address_country': 'str',  'users_5_user_address_zip_code': 'str',  'users_5_user_address_address': 'str',  'users_5_user_address_complement': 'str',  'users_5_user_address_neighborhood': 'str',  'users_5_user_address_number': 'str',  'users_6_role': 'str',  'users_6_user_ucode': 'str',  'users_6_user_locale': 'str',  'users_6_user_name': 'str',  'users_6_user_trade_name': 'str',  'users_6_user_cellphone': 'str',  'users_6_user_phone': 'str',  'users_6_user_email': 'str',  'users_6_user_documents_1_value': 'str',  'users_6_user_documents_1_type': 'str',  'users_6_user_documents_2_value': 'str',  'users_6_user_documents_2_type': 'str',  'users_6_user_address_city': 'str',  'users_6_user_address_state': 'str',  'users_6_user_address_country': 'str',  'users_6_user_address_zip_code': 'str',  'users_6_user_address_address': 'str',  'users_6_user_address_complement': 'str',  'users_6_user_address_neighborhood': 'str',  'users_6_user_address_number': 'str',  'users_7_role': 'str',  'users_7_user_ucode': 'str',  'users_7_user_locale': 'str',  'users_7_user_name': 'str',  'users_7_user_trade_name': 'str',  'users_7_user_cellphone': 'str',  'users_7_user_phone': 'str',  'users_7_user_email': 'str',  'users_7_user_documents_1_value': 'str',  'users_7_user_documents_1_type': 'str',  'users_7_user_documents_2_value': 'str',  'users_7_user_documents_2_type': 'str',  'users_7_user_address_city': 'str',  'users_7_user_address_state': 'str',  'users_7_user_address_country': 'str',  'users_7_user_address_zip_code': 'str',  'users_7_user_address_address': 'str',  'users_7_user_address_complement': 'str',  'users_7_user_address_neighborhood': 'str',  'users_7_user_address_number': 'str',  'users_8_role': 'str',  'users_8_user_ucode': 'str',  'users_8_user_locale': 'str',  'users_8_user_name': 'str',  'users_8_user_trade_name': 'str',  'users_8_user_cellphone': 'str',  'users_8_user_phone': 'str',  'users_8_user_email': 'str',  'users_8_user_documents_1_value': 'str',  'users_8_user_documents_1_type': 'str',  'users_8_user_documents_2_value': 'str',  'users_8_user_documents_2_type': 'str',  'users_8_user_address_city': 'str',  'users_8_user_address_state': 'str',  'users_8_user_address_country': 'str',  'users_8_user_address_zip_code': 'str',  'users_8_user_address_address': 'str',  'users_8_user_address_complement': 'str',  'users_8_user_address_neighborhood': 'str',  'users_8_user_address_number': 'str',  'users_9_role': 'str',  'users_9_user_ucode': 'str',  'users_9_user_locale': 'str',  'users_9_user_name': 'str',  'users_9_user_trade_name': 'str',  'users_9_user_cellphone': 'str',  'users_9_user_phone': 'str',  'users_9_user_email': 'str',  'users_9_user_documents_1_value': 'str',  'users_9_user_documents_1_type': 'str',  'users_9_user_documents_2_value': 'str',  'users_9_user_documents_2_type': 'str',  'users_9_user_address_city': 'str',  'users_9_user_address_state': 'str',  'users_9_user_address_country': 'str',  'users_9_user_address_zip_code': 'str',  'users_9_user_address_address': 'str',  'users_9_user_address_complement': 'str',  'users_9_user_address_neighborhood': 'str',  'users_9_user_address_number': 'str',  'users_10_role': 'str',  'users_10_user_ucode': 'str',  'users_10_user_locale': 'str',  'users_10_user_name': 'str',  'users_10_user_trade_name': 'str',  'users_10_user_cellphone': 'str',  'users_10_user_phone': 'str',  'users_10_user_email': 'str',  'users_10_user_documents_1_value': 'str',  'users_10_user_documents_1_type': 'str',  'users_10_user_documents_2_value': 'str',  'users_10_user_documents_2_type': 'str',  'users_10_user_address_city': 'str',  'users_10_user_address_state': 'str',  'users_10_user_address_country': 'str',  'users_10_user_address_zip_code': 'str',  'users_10_user_address_address': 'str',  'users_10_user_address_complement': 'str',  'users_10_user_address_neighborhood': 'str',  'users_10_user_address_number': 'str', 'purchase_status': 'str'},
        "commissions": {'product_name': 'str', 'product_id': 'int', 'exchange_rate_currency_payout': 'float', 'transaction': 'str', 'commissions_1_source': 'str', 'commissions_1_commission_value': 'float', 'commissions_1_commission_currency_code': 'str', 'commissions_1_user_ucode': 'str', 'commissions_1_user_email': 'str', 'commissions_1_user_name': 'str', 'commissions_2_source': 'str', 'commissions_2_commission_value': 'float', 'commissions_2_user_ucode': 
    'str', 'commissions_2_user_email': 'str', 'commissions_2_user_name': 'str', 'commissions_3_source': 'str', 'commissions_3_commission_value': 'float', 'commissions_3_commission_currency_code': 'str', 'commissions_3_user_ucode': 'str', 'commissions_3_user_email': 'str', 'commissions_3_user_name': 'str', 'commissions_2_commission_currency_code': 'str', 'commissions_4_commission_value': 'float', 'commissions_4_commission_currency_code': 'str', 'commissions_4_user_ucode': 'str', 'commissions_4_user_email': 'str', 'commissions_4_user_name': 'str', 'commissions_4_source': 'str', 'commissions_5_commission_value': 'float', 'commissions_5_commission_currency_code': 'str', 'commissions_5_user_ucode': 'str', 'commissions_5_user_email': 'str', 'commissions_5_user_name': 'str', 'commissions_5_source': 'str', 'commissions_6_commission_value': 'float', 'commissions_6_commission_currency_code': 'str', 'commissions_6_user_ucode': 'str', 'commissions_6_user_email': 'str', 'commissions_6_user_name': 'str', 'commissions_6_source': 'str', 'commissions_7_commission_value': 'float', 'commissions_7_commission_currency_code': 'str', 'commissions_7_user_ucode': 'str', 'commissions_7_user_email': 'str', 'commissions_7_user_name': 'str', 'commissions_7_source': 'str', 'commissions_8_commission_value': 'float', 'commissions_8_commission_currency_code': 'str', 'commissions_8_user_ucode': 'str', 'commissions_8_user_email': 'str', 'commissions_8_user_name': 'str', 
    'commissions_8_source': 'str', 'commissions_9_commission_value': 'float', 'commissions_9_commission_currency_code': 'str', 'commissions_9_user_ucode': 'str', 'commissions_9_user_email': 'str', 'commissions_9_user_name': 'str', 'commissions_9_source': 'str', 'commissions_10_commission_value': 'float', 'commissions_10_commission_currency_code': 'str', 'commissions_10_user_ucode': 'str', 'commissions_10_user_email': 'str', 'commissions_10_user_name': 'str', 'commissions_10_source': 'str', 'purchase_status': 'str'},
        "price_details": {'coupon_code': 'str', 'coupon_value': 'float', 'fee_value': 'int', 'fee_currency_code': 'str', 'real_conversion_rate': 'float', 'vat_value': 'float', 'vat_currency_code': 'str', 'product_id': 'int', 'product_name': 'str', 'total_value': 'float', 'total_currency_code': 'str', 'base_value': 'int', 'base_currency_code': 'str', 'transaction': 'str', 'purchase_status': 'str'}}

    #Get Sales History:
    for consulta in consultas:
        dicionario_padrao = dicionarios_padrao_consultas[consulta]

        itens_tratados = []
        for status in status_transacoes:
            #Handle paging in the API requests:
            primeiro_request = True
            achou_ultima_pagina = False
            while(not achou_ultima_pagina):
                url = f'https://developers.hotmart.com/payments/api/v1/sales/{consulta if consulta != "price_details" else "price/details"}'
                payload = {}
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {access_token_dev}'

                    #rate limit headers:
                    #'RateLimit-Limit': (filter, not used)
                    #'RateLimit-Remaining': (filter, not used)
                    #'RateLimit-Reset': (filter, not used)
                    #'X-RateLimit-Limit-Minute': (filter, not used)
                    #'X-RateLimit-Remaining-Minute': (filter, not used)
                }
                params = {
                    #pagination params: (filter, not used)
                    'max_results': 500,
                    #'page_token': (not needed on the first page)
                    
                    #custom response params:
                    #'select': (filter, not used)
                    
                    #sales params:
                    #product_id: (filter, not used)
                    'start_date': delta_inicial,
                    'end_date': delta_final,
                    #sales_source: (filter, not used)
                    #transaction: (filter, not used)
                    #buyer_name: (filter, not used)
                    'transaction_status': status
                    #payment_type: (filter, not used)
                    #offer_code: (filter, not used)
                    #commission_as: (filter, not used)
                    #affiliate_name: (filter, not used)
                }
                if not primeiro_request:
                    params['page_token'] = next_page_token

                response = requests.get(url, headers=headers, params=params, data=payload)
                time.sleep(0.5) #0.5 seconds sleep time to avoid too many requests error when using Hotmart's API. This error usually only occurs when using this code on Lambda.
                
                try:
                    if 'next_page_token' in response.json()["page_info"]:
                        next_page_token = response.json()["page_info"]['next_page_token']
                    else:
                        achou_ultima_pagina = True
                except:
                    return response.json()

                primeiro_request = False
                
                #Process HTTP Response and convert to CSV:
                itens = response.json()['items']

                for venda in itens:
                    dicionario_venda = dict(dfs(venda))
                    dicionario_venda['purchase_status'] = status
                    itens_tratados.append(dicionario_venda)

        csv_data = io.StringIO()
        csv_writer = csv.writer(csv_data)
        csv_writer.writerow(dicionario_padrao.keys())

        valores_padrao = {'str': '', 'int': '', 'float': '', 'bool': False}
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

            csv_writer.writerow(dicionario_tratado.values())
        csv_string = csv_data.getvalue()
        
        csv_data = io.StringIO(csv_string)
        
        if consulta == "history":
            df_relevant_data = pd.read_csv(csv_data, dtype=str)

        elif consulta != "summary":
            df_relevant_data = pd.merge(how = "inner", left = df_relevant_data, right = pd.read_csv(csv_data, dtype=str), left_on = 'purchase_transaction', right_on = 'transaction', suffixes=('', f'_{consulta.upper()}_DUPLICATE'))

        #Send to S3:
        s3.put_object(Body=csv_string.encode('utf-8'), Bucket=BUCKET_NAME, Key=f'database_hotmart_{consulta}.csv')

    csv_string_relevant_data = df_relevant_data.to_csv(index=False)
    s3.put_object(Body=csv_string_relevant_data.encode('utf-8'), Bucket=BUCKET_NAME, Key=f'database_hotmart_relevant_data.csv')

    return {
        'statusCode': 200,
        'body': 'Dados enviados para S3 com sucesso'
    }

print(lambda_handler(None, None))
