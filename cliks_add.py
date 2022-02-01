
import pandas as pd
import numpy as np
import math
import re

path = input('Введите путь к файлам: ')


click = pd.read_csv(path + '/click.csv', sep=';', error_bad_lines=False)
events_pushka_accepted_30122021 = pd.read_csv(path + '/events_pushka_accepted_30122021.csv'
                                              , sep = ',', error_bad_lines=False)

events_pushka_accepted_30122021 = events_pushka_accepted_30122021[events_pushka_accepted_30122021[
    'entity.saleLink'].notna() == True]

events_pushka_accepted_30122021['event_identity'] = events_pushka_accepted_30122021['entity.name'
                                                                                     ].apply(lambda x:
                                                                                            x.split()[0].strip())

events_pushka_accepted_30122021 = events_pushka_accepted_30122021.rename(columns= {'entity.saleLink': 'url',
                                                                                  'entity._id': 'event_id',
                                                                                  'entity.name': 'event_name',
                                                                                  'entity.additionalSaleLinks.0': 'additional_url',
                                                                                  'entity.organization._id': 'organization_id',
                                                                                  'entity.organization.name': 'organization_name'}) 

events_pushka_accepted_30122021 = events_pushka_accepted_30122021.astype({"organization_id": "object",
                                                                         'event_id': 'object'})

cliks_add = pd.merge(click, events_pushka_accepted_30122021, how = 'left', on = 'url')

organizations = pd.read_csv(path + '/organizations.csv', sep = ';', error_bad_lines=False)

organizations = organizations[organizations['ИНН'].notna()]

organizations['Org_region_number'] = organizations['ИНН'].apply(lambda x:
                                                               int(str(x)[:2]) 
                                                                if len(str(x)) == 12
                                                               else int(str(x)[:1]))

organizations = organizations.rename(
    columns= {'ID': 'organization_id','Категория': 'org_category'}) 

cliks_add = pd.merge(cliks_add, organizations[['organization_id', 'Org_region_number', 'org_category']],
                     how = 'left', on = 'organization_id')

users = pd.read_csv(path + '/users_full.csv', sep=';', error_bad_lines=False)

cliks_add = pd.merge(cliks_add, users[['user_id', 'age', 'user_region']],
                     how = 'left', on = 'user_id')

cliks_add.drop(['additional_url'],axis = 1, inplace = True)

cliks_add['user_phone_details_id'] = cliks_add['user_phone_details'].apply(lambda x: 
                                                                          re.sub('(\..*)', '', x.replace('iOS ', '')) 
                                                                           if 'iOS' in x 
                                                                           else re.sub('\ \(.*', '', x))

cliks_add['user_phone_details_id'] = cliks_add['user_phone_details_id'].apply(lambda x: 
                                                                          re.sub('(\..*)', '', x) 
                                                                           if 'Android' in x
                                                                          else x)

cliks_add.drop(labels = [0],axis = 0, inplace = True)

cliks_add['user_phone_details_id_2'] = cliks_add['user_phone_details_id'].apply(lambda x: 
                                                                          x.replace('iOS,', '2').split()[0]+x.split()[1]
                                                                           if 'iOS' in x 
                                                                           else x.replace('Android,', '1').split()[0]+x.split()[1])

cliks_add.to_csv(path + '/cliks_add.csv', sep=';', index=False)
