import pandas as pd
import os
import requests as re 
import datetime as dt
import time
import random
import sys
from bs4 import BeautifulSoup
import numpy as np
import time
from io import StringIO 
import os
import shutil
from pandasql import PandaSQL
from google.colab import files as gfiles

def download(df,name):
    if '.csv' not in name:
        name += '.csv'
    df.to_csv(name)
    gfiles.download(name)

#shutil.copy('/'.join(__file__.split('/')[:-1]) + '/client_secrets.json', 
#            '/'.join(__file__.split('/')[:-2]))

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
  
  shortcut= {}

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

  
def pull(file_name,sept=','):
  if '.' in file_name:
    file_name, tail = file_name.split('.')[0], file_name.split('.')[1]
  else:
    tail = 'csv'
  
  path = file_name.split('/')

  driver, file_id = get_drive_id(path)

  myfile = driver.CreateFile({'id': file_id})
  myfile.GetContentFile('{}.{}'.format(path[-1],tail))
  if tail == 'csv':
    files = pd.read_csv('{}.{}'.format(path[-1],tail),sep=sept)
    os.remove('{}.{}'.format(path[-1],tail))
    return files

def pull_excel(file_name,sheet_name=None,header=0):
  if '.' in file_name:
    file_name, tail = file_name.split('.')[0], file_name.split('.')[1]
  
  path = file_name.split('/')

  driver, file_id = get_drive_id(path)

  myfile = driver.CreateFile({'id': file_id})
  myfile.GetContentFile('{}.{}'.format(path[-1],tail))
  files = pd.read_excel('{}.{}'.format(path[-1],tail),sheet_name=sheet_name,header=header)
  os.remove('{}.{}'.format(path[-1],tail))
  return files


def sql(query):        
    df_name = query.split('from ')[1]\
                   .split(' ')[0].strip()
    print('\t  '+df_name)
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

def push(df, file_name):
  #print('~~uploading~~')
  path = file_name.split('/')
                                                                                      
  file_output = path[-1]
  path = path[:-1]
  
  driver, file_id = get_drive_id(path)                                                                           
  rand = str(dt.datetime.today()).replace(':','').replace('.','').replace(' ','').replace('-','')
  df.to_csv(rand+".csv", index = False)                                        
  uploaded = driver.CreateFile({'title':file_output,
                               "parents": [{"kind": "drive#fileLink",
                                            "id": file_id,
                                            'mimeType':'text/csv'}]})
  uploaded.SetContentFile("example.csv")
  uploaded.Upload()
  os.remove(rand+'.csv')
  
  

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
  drive, fid  = get_drive_id(full_path[:-1])
  file1 = drive.CreateFile({'title': full_path[-1], 
                            "parents":  [{"id": fid}], 
                            "mimeType": "application/vnd.google-apps.folder"})
  file1.Upload()
  
def wipe(file_name):
  path = file_name.split('/')
  drive, file_id = get_drive_id(path)
  file1 = drive.CreateFile({'id': file_id})
  file1.Trash()
  
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



