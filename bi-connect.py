from fast_bitrix24 import Bitrix
from bitrix24 import Bitrix24
from pprint import pprint
import workYDB
from tqdm import tqdm
import os

sql = workYDB.Ydb()
webhook = os.getenv('webHook')
bit = Bitrix(webhook)

def get_user_fields():
    user_fields = bit.get_all('user.fields')
    return user_fields

def get_deal_fields():
    dealFields = bit.get_all('crm.deal.fields')
    return dealFields

def get_lead_fields():
    leadFields = bit.get_all('crm.lead.fields')
    return leadFields

def get_user_list():
    users = bit.get_all(
    'user.get',
    params={
        'filter': {'ACTIVE': True},
    }) 
    return users

def get_lead_list():
    try:
        date = sql.get_last_date_modify('lead')[0]
    except Exception as e:
        print(e)
        date = '2022-12-01T00:00:00+03:00'
    
    leads = bit.get_all(
    'crm.lead.list',
    params={
        'select': ['*', 'UF_*'],
        'filter': {'>=DATE_MODIFY': date.replace('T', ' ').split('+')[0]},
        #'filter': {'>=DATE_CREATE': date.replace('T', ' ').split('+')[0]},
    })
    return leads

def get_deal_list():
    try:
        date = sql.get_last_date_modify('deal')[0]
    except Exception as e:
        print(e)
        date = '2022-12-01T00:00:00+03:00'
    
    deals = bit.get_all(
    'crm.deal.list',
    params={
        'select': ['*', 'UF_*'],
        'filter': {'>=DATE_MODIFY': date.replace('T', ' ').split('+')[0]},
        #'filter': {'>=DATE_CREATE': date.replace('T', ' ').split('+')[0]},
    })
    return deals

def send_deal_to_ydb(tabName: str,entity:list):
    for ent in tqdm(entity):
        sql.replace_query(tabName, ent)

def handler(event, context):
    #fields = get_deal_fields()
    #sql.create_table('deal', fields)
    deals = get_deal_list()
    send_deal_to_ydb('deal', deals)

    # fields = get_lead_fields()
    # sql.create_table('lead', fields)
    leads = get_lead_list()
    send_deal_to_ydb('lead', leads)
    
    # fields = get_user_fields()
    # sql.create_table('user', fields)
    users = get_user_list()
    send_deal_to_ydb('user', users)
    
    return {
        'statusCode': 200,
        'body': 'OK'
    }

handler(1,1)
