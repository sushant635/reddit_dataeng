
from utils.constants import SECRET,CLIENT_ID,OUTPUT_PATH
from etls.reddit_etl import connect_reddit,extract_post,transform_data,load_data_to_csv
import pandas as pd 

def reddit_pipline(file_name: str,subreddit: str,time_filter='day',limit=None):
    try:
        instance = connect_reddit(CLIENT_ID,SECRET,'Airscholar Agent')
        posts = extract_post(instance,subreddit,time_filter,limit)

        post_df = pd.DataFrame(posts)

        post_df = transform_data(post_df)

        file_path = f'{OUTPUT_PATH}/{file_name}.csv'

        load_data_to_csv(post_df,file_path)
        # print(post_df)
        return file_path

    except Exception as e:
        print(e)