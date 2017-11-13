
# coding: utf-8

# In[19]:


import paramiko
import pandas as pd
import numpy as np
import simplejson as json
import collections
import time


def getcsv(host, uname, pword, dataset, date, filecopypath):
        
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys() 
        ssh.connect(hostname=host, username=uname, password=pword)
       
    
        sftp = ssh.open_sftp()
        logging.debug('SFTP Open')
    
        file_remote='outgoing/'+ dataset + '_'+ date + '.csv'
    
        sftp.get(file_remote, filecopypath)

        sftp.close()
      
        ssh.close()
    
        

def getjson(host, uname, pword, dataset, date, filecopypath):


        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys() 
        ssh.connect(hostname=host, username=uname, password=pword)
      
        sftp = ssh.open_sftp()
       
    
        file_remote='outgoing/'+ dataset + date + '.json'
    
        sftp.get(file_remote, filecopypath)

        sftp.close()
    
        ssh.close()
      
        
def loadjson(f):
    with open (f) as f:
        file = json.load(f)
    return file

    
def getcsvtodf(host, uname, pword, dataset, date, filecopypath):
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys() 
    ssh.connect(hostname=host, username=uname, password=pword)
    sftp = ssh.open_sftp()
    file_remote='/Users/paul/data/'+ dataset + date + '.csv'
    sftp.get(file_remote, filecopypath)
    sftp.close()
    ssh.close()
    df=pd.read_csv(filecopypath + dataset + date + '.csv' )
    
    return df
    
   
    
def jsondatatodf(path, data):
    
    if data=='cc':
        with open (path) as f:
            data = json.load(f)
    
        cust_colnames = {}
        for i in range(len(data['columns'][0:115])):
            cust_colnames[i]=list(data['columns'][0:115][i].values())[1]
        cust_colnames=list(cust_colnames.values())
    
        acct_colnames = {}
        for i in range(len(data['columns'][115:116][0][0:397])):
            acct_colnames[i]=list(data['columns'][115:116][0][0:397][i].values())[1]
        acct_colnames=list(acct_colnames.values())

        tran_colnames = {}
        for i in range(len(data['columns'][0:116][115][397])):
            tran_colnames[i]=list(data['columns'][0:116][115][397][i].values())[1]
        tran_colnames=list(tran_colnames.values())

        cust_df = {}
        acct_df = {}
        tran_df = {}
        result = []

        for i in range(len(data['datatable']['data'])):
            cust_df[i]= pd.DataFrame(data['datatable']['data'][i][:115:]).transpose()
            cust_df[i].columns=cust_colnames
            cust_id = []
            for j in range(0,len(data['datatable']['data'][i][115])):
                acct_df[j]=pd.DataFrame(data['datatable']['data'][i][115][j][:397]).transpose()
                cust_id.append(data['datatable']['data'][i][0])
                acct_df[j].columns=acct_colnames
                acct_df[j]['cust_id']=data['datatable']['data'][i][0]
                for k in range(len(data['datatable']['data'][i][115][j][397])):
                    tran_df[k]=pd.DataFrame(data['datatable']['data'][i][115][j][397][k]).transpose()
                    acct_id=data['datatable']['data'][i][115][j][0]
                    cust_id_t=data['datatable']['data'][i][0]
                    tran_df[k].columns=tran_colnames
                    tran_df[k]['cust_id'] = cust_id_t
                    tran_df[k]['acct_id'] = acct_id
                    result.append(pd.merge((pd.merge(acct_df[j], tran_df[k], on=["cust_id","acct_id"])), 
                                   cust_df[i], on='cust_id'))
            results=pd.concat(result)
        
        return results  
        
    
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
                for j in range(len(data['datatable']['data'][i][5])):
                    post_df_one[i]=pd.DataFrame(data['datatable']['data'][i][5][j]).transpose()
                    post_df_one[i].columns=post_colnames
                    post_df_one[i]['brand_id']=data['datatable']['data'][i][0]
            
            if len(data['datatable']['data'][i]) <= 6 and len(data['datatable']['data'][i][len(data['datatable']['data'][i])-1]) == 1:
                post_loc=len(data['datatable']['data'][i])-1
                post_df_two[i]=pd.DataFrame(data['datatable']['data'][i][post_loc])
                post_df_two[i].columns=post_colnames
                post_df_two[i]['brand_id']=data['datatable']['data'][i][0]
    
            if len(data['datatable']['data'][i]) <= 6 and len(data['datatable']['data'][i][len(data['datatable']['data'][i])-1]) > 1:
                for m in range(0, (len(data['datatable']['data'][i][len(data['datatable']['data'][i])-1]))):
                    post_loc=len(data['datatable']['data'][i])-1
                    post_df_two[m]=pd.DataFrame(data['datatable']['data'][i][post_loc][m]).transpose()
                    post_df_two[m].columns=post_colnames
                    post_df_two[m]['brand_id']=data['datatable']['data'][i][0]
            
            
                  
            if len(data['datatable']['data'][i]) > 6: 
        
                for h in range(0, (len(data['datatable']['data'][i])-5)):
                    brand_df.append(data['datatable']['data'][i][0:4])
                    brand_df_II.append(data['datatable']['data'][i][4+h])
            
            if len(data['datatable']['data'][i]) > 6 and len(data['datatable']['data'][i][len(data['datatable']['data'][i])-1]) > 1:
                for m in range(0, (len(data['datatable']['data'][i][len(data['datatable']['data'][i])-1]))):
                    post_loc=len(data['datatable']['data'][i])-1
                    post_df_two[m]=pd.DataFrame(data['datatable']['data'][i][post_loc][m]).transpose()
                    post_df_two[m].columns=post_colnames
                    post_df_two[m]['brand_id']=data['datatable']['data'][i][0]

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
        
        return results
        
    


# In[38]:

#getcsv('192.168.200.239', 'Paul','bmotest','cc','20170925','/Users/kiransarabu/Documents/test.csv')
#loadjson('/Users/kiransarabu/AnacondaProjects/tribe_data_mp_2.json')
#jsondatatodf('/Users/kiransarabu/AnacondaProjects/tribe_data_mp_2.json', 'tribe')


# In[ ]:



