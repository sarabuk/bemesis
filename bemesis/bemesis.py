import paramiko
import pandas as pd
import numpy as np
import simplejson as json
import collections
import time
import zipfile
import os

def getcsv(host, uname, pword, dataset, date, filecopypath):
    
        get_remote_file(host, uname, pword, dataset, date, filecopypath, 'csv')
        

def getjson(host, uname, pword, dataset, date, filecopypath):  
    
        get_remote_file(host, uname, pword, dataset, date, filecopypath, 'json')
        
def get_remote_file(host, uname, pword, dataset, date, filecopypath, file_type):
    
    try:

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys() 
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=uname, password=pword)
        sftp = ssh.open_sftp()
    
    except:
    
        print('Unable to Connect ')
    
    else:
        
        print('SSH Connection Made')
        
    try:
        
        remote_file = 'outgoing/'+ dataset + '_'+ date + '.' + file_type + '.zip'    
        print(remote_file)      
        local_file = filecopypath + dataset + '_'+ date + '.' + file_type + '.zip'  
        print(local_file)
        sftp.get(remote_file, local_file)
        
    except:
        print ('Error: can\'t find file or read data')
    
    else:
        print('File Copied')  
        
    try:
        sftp.close()      
        ssh.close()  
        
    except:
        print ('connection was not closed')
    
    else:
        print('SSH Connection Closed')  
    
    try:
        pickup_zip=zipfile.ZipFile(local_file, 'r')
        pickup_zip.extractall(filecopypath) 
        os.remove(local_file)
        
    except:
        print ('zip file not found')
  
    else:
        print ('Copy Complete')
    
def get(host, uname, pword, dataset, date, view_type):
    
    try:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys() 
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=uname, password=pword)
        sftp = ssh.open_sftp()
        
    
        if view_type == 'df':
            file_extension = '.csv'
        if view_type == 'json':
            file_extension = '.json.csv'

        remote_filename = '/Home/GENESIS/'+ uname +'/outgoing/'+ dataset + '_'+ date + file_extension 

        
        remote_file_handle = sftp.open(remote_filename)
        
        if view_type == 'df':
            return_value = pd.read_csv(remote_file_handle)
            return_value = return_value.replace(np.nan, '', regex=True)
            return_value = return_value.replace('None', '', regex=True)
            
        if view_type == 'json':
            return_value = json.load(remote_file_handle)
            
        remote_file_handle.close()
        ssh.close()
        
    except:
        print ('File Not Found')

    except IOError:
            print('An error occurred trying to read the file.')

    except ValueError:
            print('Non-numeric data found in the file.')

    except ImportError:
            print ('No module found')
    
    else:
        return return_value


    
def jsondatatodf(path, data):

    if data=='tribe':
        with open (path) as f:
            data = json.load(f)
            
        brand_colnames = {}
        for i in range(len(data['columns'][0:5])):
            brand_colnames[i]=list(data['columns'][0:5][i].values())[0]
        brand_colnames=list(brand_colnames.values())

        post_colnames = {}
        for i in range(len(data['columns'][5:6][0])):
            post_colnames[i]=list(data['columns'][5:6][0][i].values())[0]
        post_colnames=list(post_colnames.values())
    
        brand_df_one={}
        brand_df=[]
        brand_df_II=[]
        post_df_one={}
        post_df_two={}

        for i in range(0, (len(data['datatable']['data']))):
    
    
            if len(data['datatable']['data'][i]) <= 6:
                brand_df_one[i]=pd.DataFrame(data['datatable']['data'][i][:5]).transpose()
        
            if len(data['datatable']['data'][i]) <= 6 and len(data['datatable']['data'][i][5]) > 1:

                    for j in range(len(data['datatable']['data'][i][5])):
                        post_df_one[i]=pd.DataFrame(data['datatable']['data'][i][5][j]).transpose()
                        post_df_one[i].columns=post_colnames
                        post_df_one[i]['brand_id']=data['datatable']['data'][i][0]
            
            if len(data['datatable']['data'][i]) <= 6:
                post_df_one[i]=pd.DataFrame(data['datatable']['data'][i][5])
                post_df_one[i].columns=post_colnames
                post_df_one[i]['brand_id']=data['datatable']['data'][i][0]
            
            if len(data['datatable']['data'][i]) <= 6 and len(data['datatable']['data'][i][len(data['datatable']['data'][i])-1]) == 1:
                post_loc=len(data['datatable']['data'][i])-1
                post_df_two[i]=pd.DataFrame(data['datatable']['data'][i][post_loc])
                post_df_two[i].columns=post_colnames
                post_df_two[i]['brand_id']=data['datatable']['data'][i][0]
    
            if len(data['datatable']['data'][i]) <= 6 and len(data['datatable']['data'][i][len(data['datatable']['data'][i])-1]) > 1:
                post_loc=len(data['datatable']['data'][i])-1
                post_df_two[i]=pd.DataFrame(data['datatable']['data'][i][post_loc])
                post_df_two[i].columns=post_colnames
                post_df_two[i]['brand_id']=data['datatable']['data'][i][0]
              
                  
            if len(data['datatable']['data'][i]) > 6: 
        
                for h in range(0, (len(data['datatable']['data'][i])-5)):
                    brand_df.append(data['datatable']['data'][i][0:4])
                    brand_df_II.append(data['datatable']['data'][i][4+h])
            
            if len(data['datatable']['data'][i]) > 6 and len(data['datatable']['data'][i][len(data['datatable']['data'][i])-1]) > 1:

                post_loc=len(data['datatable']['data'][i])-1
                post_df_two[i]=pd.DataFrame(data['datatable']['data'][i][post_loc])
                post_df_two[i].columns=post_colnames
                post_df_two[i]['brand_id']=data['datatable']['data'][i][0]

            if len(data['datatable']['data'][i]) > 6 and len(data['datatable']['data'][i][len(data['datatable']['data'][i])-1])==1:
                post_loc=len(data['datatable']['data'][i])-1
                post_df_two[i]=pd.DataFrame(data['datatable']['data'][i][post_loc])
                post_df_two[i].columns=post_colnames
                post_df_two[i]['brand_id']=data['datatable']['data'][i][0]
    
            

        brandI=pd.concat(brand_df_one)
        brandI=brandI.reset_index(drop=True)
        brandI.columns=brand_colnames

        brand_df_II=pd.DataFrame(brand_df_II)
        brand_df=pd.DataFrame(brand_df)
        brandII=brand_df.merge(brand_df_II, left_index=True, right_index=True)
        brandII=brandII.reset_index(drop=True)
        brandII.columns=brand_colnames

        postI=pd.concat(post_df_one)
        postI=postI.reset_index(drop=True)
        postII=pd.concat(post_df_two)
        postII=postII.reset_index(drop=True)

        final_brand=pd.concat([brandII, brandI])
        final_post=pd.concat([postII, postI])

        results=pd.merge(final_brand, final_post, how='outer', on=["brand_id"])
        results=results.replace(np.nan, '', regex=True)
        results = results.replace('None', '', regex=True)

        results = results.drop_duplicates()
        
        return results
