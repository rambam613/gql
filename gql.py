import pandas as pd
import os
import requests as re
import numpy as np
import datetime as dt
import time
import random
import sys
from bs4 import BeautifulSoup
import numpy as np
import time
#import psycopg2
import io
import os
import shutil
from pandasql import PandaSQL

if sys.platform == 'linux':
    from google.colab import files as gfiles

def download(df,name):
        if '.csv' not in name:
                name += '.csv'
        df.to_csv(name)
        if sys.platform == 'linux':
            gfiles.download(name)

#shutil.copy('/'.join(__file__.split('/')[:-1]) + '/client_secrets.json',
#                        '/'.join(__file__.split('/')[:-2]))

if 'client_secrets.json' not in os.listdir():
        shutil.copy('/'.join(__file__.split('/')[:-1]) + '/client_secrets.json',
                             'client_secrets.json')

#!pip install -U -q PyDrive
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
#from google.colab import auth
#from google.colab import files
#from oauth2client.client import GoogleCredentials

#----- google drive -----#

def login():
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile("pydrive-credentials.json")
        if gauth.credentials is None:
                # Authenticate if they're not there
                gauth.CommandLineAuth()
        elif gauth.access_token_expired:
                # Refresh them if expired
                gauth.Refresh()
        else:
                # Initialize the saved creds
                gauth.Authorize()
        # Save the current credentials to a file
        gauth.SaveCredentialsFile("pydrive-credentials.json")
        drive = GoogleDrive(gauth)
        return(drive)



def get_drive_id(full_path):
    gdriver = login()

    if type(full_path) is not list:
        if '/' in full_path:
            full_path = full_path.split('/')
        else:
            full_path = [full_path]

    if full_path == []:
        return gdriver, 'root'

    shortcut= {'sandbox': '1EuD6_o5R8Fmdo3xJuZ6gS4TtLA30YV2A',
               'uman': '1rqtlgkKIosbEI9MAdHGkdtPmNfEOlMrk',
               'oil': '19Zn2MRCetGNM3vfdVYyn0ss3nbdt_eyq',
               'modules':'1qLz1Kf4hdlytjb9cJdBEBCKRh4fJjRuj'}

    if full_path[0] in shortcut.keys():
        if len(full_path) == 1:
            return gdriver, shortcut[full_path[0]]
            print('1')
        else:
            starter = shortcut[full_path[0]]
            full_path = full_path[1:]
    else:
        starter = 'root'

    file_list = gdriver.ListFile({'q': "'{}' in parents and trashed=false".format(starter)}).GetList()

    try:
        ender = full_path[-1]

        if len(full_path) >= 1:
            full_path = full_path[:-1]
            for l in full_path:
                for f in file_list:
                    if f['title'] == l:
                        next_id = f['id']
                        break
                file_list = gdriver.ListFile({'q': "'{}' in parents and trashed=false".format(next_id)}).GetList()

        next_id = {f['title']:f['id'] for f in file_list}[ender]
        return gdriver, next_id
    except:
        print('cant find folder, sending to home')
        return gdriver, 'root'


def pull(file_name,header=0,sept=','):
    if '.' in file_name:
        lookup = file_name
        file_name, tail = file_name.split('.')[:2]
    else:
        lookup = file_name
        tail = 'csv'

    driver, file_id = get_drive_id(lookup)

    mf = driver.CreateFile({'id': file_id})
    if tail == 'csv':
        mf = mf.GetContentString()
        files = pd.read_csv(io.StringIO(mf),header=header,sep=sept)

    elif tail == 'parquet':
        mf = mf.GetContentString(encoding='latin')
        mf = io.BytesIO(bytes(mf,'latin'))
        files = pd.read_parquet(mf)

    else:
        mf.GetContentFile('{}.{}'.format(file_name.split('/'),tail))
        files = None

    return files


def push(df, file_name):

    path = file_name.split('/')
    file_output = path[-1]

    if '.' in file_output:
        tail=file_output.split('.')[-1]
    else:
        tail='csv'

    path = path[:-1]

    driver, file_id = get_drive_id(path)

    if sys.platform == 'linux':
        if tail == 'csv':
            temp_name = f'{file_output}.csv'
            df.to_csv(temp_name, index = False)
            mimetype = 'text/csv'
        else:
            temp_name = f'{file_output}.parquet'            
            df.to_parquet(temp_name, index = False,engine='pyarrow',allow_truncated_timestamps=True)
            mimetype = ''

        uploaded = driver.CreateFile({'title':file_output,
                                    "parents": [{"kind": "drive#fileLink",
                                    "id": file_id,
                                    'mimeType':mimetype}]})
        uploaded.SetContentFile(temp_name)
        uploaded.Upload()
        os.remove(temp_name)
    else:
        if tail == 'csv':
            mimetype = 'text/csv'
            uploaded = driver.CreateFile({'title':file_output,
                                        "parents": [{"kind": "drive#fileLink",
                                        "id": file_id,'mimeType':mimetype}]})
            uploaded.SetContentString(df.to_csv(index = False))
            uploaded.Upload()
        elif tail == 'parquet':
            s_buf = io.BytesIO()
            df.to_parquet(s_buf, index=False, engine='pyarrow', allow_truncated_timestamps=True, use_dictionary=False)
            s_buf.seek(0)
            s_buf = s_buf.read()
            s_buf = s_buf.decode('latin')

            mimetype = ''
            uploaded = driver.CreateFile({'title':file_output,
                                        "parents": [{"kind": "drive#fileLink",
                                        "id": file_id,'mimeType':mimetype}]})
            uploaded.SetContentString(s_buf,encoding='latin')
            uploaded.Upload()

        else:
            mimetype = 'text/csv'
            uploaded = driver.CreateFile({'title':file_output,
                                        "parents": [{"kind": "drive#fileLink",
                                        "id": file_id,'mimeType':mimetype}]})
            uploaded.SetContentFile(file_output)
            uploaded.Upload()




def sql(query):
        df_name = query.split('from ')[1]\
                                     .split(' ')[0].strip()
        print('\t    '+df_name)
        df = pull(df_name)

        query = query.replace(df_name,'df')

        if 'join' in query:
                other_dfs = [i.split(' ')[0]
                                         for i in query.split('join ')[1:]]
                for o in range(len(other_dfs)):
                        print('\t '+other_dfs[o])
                        exec("df{} = pull('{}')".format(o,other_dfs[o].strip()))
                        query = query.replace(other_dfs[o],'df{}'.format(o))

        pdsql = PandaSQL()
        df = pdsql(query)
        return df

def ls(path):
    driver, file_id = get_drive_id(path)
    file_list = driver.ListFile({'q': "'{}' in parents and trashed=false".format(file_id)}).GetList()
    file_list = [f['title'] for f in file_list]
    return file_list



def push_file(file_path, file_dest):
    #print('~~uploading~~')
    path = file_dest.split('/')

    file_output = path[-1]
    path = path[:-1]

    driver, file_id = get_drive_id(path)

    uploaded = driver.CreateFile({'title':file_output,
                                                             "parents": [{"kind": "drive#fileLink",
                                                                                        "id": file_id}]})
    uploaded.SetContentFile(file_path)
    uploaded.Upload()

def create(full_path):
    full_path = full_path.split('/')
    drive, fid    = get_drive_id(full_path[:-1])
    file1 = drive.CreateFile({'title': full_path[-1],
                                                        "parents":    [{"id": fid}],
                                                        "mimeType": "application/vnd.google-apps.folder"})
    file1.Upload()

def wipe(file_name):
    path = file_name.split('/')
    drive, file_id = get_drive_id(path)
    file1 = drive.CreateFile({'id': file_id})
    file1.Trash()

def update(df,path,subset=False):
    orig = pull(path)

    if (len([i for i in df.columns if i not in orig.columns])==0) &\
       (len([i for i in orig.columns if i not in df.columns])==0):
        orig = pd.concat([orig,df])
        if subset==None:
            pass
        elif subset==False:
            orig = orig.drop_duplicates()
        else:
            orig = orig.drop_duplicates(subset=subset)

        wipe(path)
        push(orig,path)
    else:
        if '.' in path:
            path+="_copy"
        else:
            path, tail = path.split('.')
            path+="_copy."
            path+=tail
        push(df,path)


#----- scrapes -----#

def get_table(urls,table_pos=0):
    df = []
    if type(urls) != list:
            urls = [urls]
    for g in urls:
        x = re.get(g)
        soup = BeautifulSoup(x.content,'html.parser')
        bobs = soup.find_all('table')[table_pos]
        table_rows = bobs.find_all('tr')

        for u in table_rows:
            td = u.find_all('td')
            row = [i.text.strip() for i in td]
            if len(row) != 0:
                df.append(pd.DataFrame({0:row}).T)
    df = pd.concat(df)
    return df
