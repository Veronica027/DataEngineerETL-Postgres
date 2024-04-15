import os
import glob
import psycopg2
import pandas as pd
from pandas import json_normalize
import json
from sql_queries import *

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files



def process_song_files(cur,conn,filepath):
    song_files = get_files(filepath)
    print('{} files found in {}'.format(len(song_files), filepath))
    
    #一次只处理一个文件下的数据，不要先把所有数据取出来放在df里
    for i, song_file in enumerate(song_files, 1):
        df = pd.DataFrame()
        with open(song_file,'r') as f:
            json_data = json.load(f)
            song_data = json_normalize(json_data)
            df = pd.concat([df,song_data],axis=0,ignore_index=True)

        for value in df.values:
            #解包赋值给变量名
            num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value

            artist_data = (artist_id,artist_name,artist_location,artist_latitude,artist_longitude)
            try:
                cur.execute(artist_table_insert, artist_data)
                conn.commit()
                print(f"artist_table value inserted for file {song_file}")
            except:
                print(f'Some error occur when insert {song_file} data into artist_table')
                pass

            # py迭代器写法，小的df的时候可用
            #song_data = list(json_normalize(json_data)[['song_id','title','artist_id','year','duration']].values)
            song_data = (song_id,title,artist_id, year, duration)
            try:
                cur.execute(song_table_insert,song_data)
                conn.commit()
                print(f"song_table value inserted for file {song_file}")
            except:
                print(f'Some error occur when insert {song_file} data into song_table')
                pass
        
    print(f'Finished process {i}/{len(song_files)} file in song_files')

def get_timeinfo(df,datetime):
    df_copy =df.copy()

    df_copy['hour'] = df_copy[datetime].dt.hour
    df_copy['day'] = df_copy[datetime].dt.day
    df_copy['weekofyear'] = df_copy[datetime].dt.weekofyear
    df_copy['month'] = df_copy[datetime].dt.month
    df_copy['year'] = df_copy[datetime].dt.year
    df_copy['weekday'] = df_copy[datetime].dt.weekday

    return df_copy[['ts','hour', 'day', 'weekofyear', 'month', 'year', 'weekday']]

def process_log_files(cur,conn,filepath):
    log_files = get_files(filepath)
    for i, log_file in enumerate(log_files, 1):
        df = pd.DataFrame()
        with open(log_file,'r') as f:
            for log in f:
                try:
                    json_data = json.loads(log)
                    df = pd.concat([df,json_normalize(json_data)],axis=0,ignore_index=True)
                except Exception as e:
                    print(f'There is an error happened {e}')
                    pass

        df = df[df['page']=='NextSong']
        df['ts'] = pd.to_datetime(df['ts'],unit='ms')

        df_timeinfo = get_timeinfo(df,'ts')
        for i, row in df_timeinfo.iterrows():
            try:
                cur.execute(time_table_insert, list(row))
                conn.commit()
                print(f"time_table value inserted for {i} row of data in {len(df_timeinfo)}")
            
            except Exception as e:
                    print(f'There is an error happened {e}')
                    pass
                
        #在处理大型DataFrame时，直接解包赋值可能会更高效一些, 这个用df的方法是second choice
        user_df = df[['userId','firstName','lastName','gender','level']]
        for i, row in user_df.iterrows():
            try:
                cur.execute(user_table_insert, row)
                conn.commit()
                print(f"user_table value inserted for {i} row of data in {len(user_df)}")
            except Exception as e:
                    print(f'There is an error happened {e}')
                    pass

        
        for index, row in df.iterrows():

            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
            
            if results:
                songid, artistid = results
                songplay_data = (row['ts'],row['userId'],row['level'],songid,artistid,row['sessionId'],row['location'],row['userAgent'])
                
                # insert songplay record
                try:
                    cur.execute(songplay_table_insert, songplay_data)
                    conn.commit()
                except Exception as e:
                    print(f'There is an error happened {e}')
                    pass
           
            else:
                songid, artistid = None, None
                print(f'for {row.song},can not find id')
                pass


    print(f'Finished processing {i}/{len(log_files)} file in log_files')

def main():

    conn = psycopg2.connect(f"host=127.0.0.1 dbname=sparkifydb user=postgres password=hy530370")
    cur = conn.cursor()

    process_song_files(cur,conn,filepath='data/song_data')
    process_log_files(cur,conn, filepath='data/log_data')

    conn.close()
 
    
if __name__ == "__main__":
    main()
    print('\n\nFinished processing!!Yayyyy!\n\n')

