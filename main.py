import pandas as pd
import pandasql
import os
import logging

l = lambda m: logging.info(m)

#loading data from csv
def load_started_streams():
    started_streams = pd.read_csv("started_streams.csv",sep=";")
    return  started_streams

def load_whatson():
    whatson = pd.read_csv("whatson.csv",sep=",")
    return whatson

def checkResultFolder():
    if os.path.isdir("Result") == False:
        os.mkdir("Result")
        l("Result Folder created")

def solve_question1(started_streams, whatson):
    #Question 1 -
    #mappping country code with country
    l("Mapping county code with country")
    started_streams_a = started_streams
    started_streams_a.loc[started_streams_a['country_code'] == 'dk','country_code'] = 'Denmark'
    started_streams_a.loc[started_streams_a['country_code'] == 'no','country_code'] = 'Normway'
    started_streams_a.loc[started_streams_a['country_code'] == 'se','country_code'] = 'Sweden'
    started_streams_a.loc[started_streams_a['country_code'] == 'fi','country_code'] = 'Finland'

    l("Joining two tables")
    #query to join two tables
    query = """
        SELECT
        w.dt,
        s.time,
        s.device_name,
        s.house_number,
        s.user_id,
        s.country_code as Country_Code,
        s.program_title,
        s.season,
        s.season_episode,
        s.genre,
        s.product_type,
        w.broadcast_right_start_date,
        w.broadcast_right_end_date
        FROM whatson as w inner join started_streams_a as s on w.house_number = s.house_number and w.broadcast_right_region = s.country_code where s.product_type in ("svod","est"); 
    """

    df = pandasql.sqldf(query, locals())

    l("Getting recent date")
    #apply row_number over partition by to find recent date
    df['rk'] = df.sort_values(['dt'], ascending=False).groupby(['user_id']).cumcount()+1
    df = df.sort_values(['dt','rk'], ascending=False).query('rk<2')
    checkResultFolder()
    df.to_csv("Result/question1.csv",index=False)
    l("Please check output in question1.csv")


def solve_question2(started_streams):
    #Question 2 -

    query= """ 
    select 
    dt,
    program_title,
    device_name,
    country_code,
    product_type,
    count(distinct user_id) as unique_users,
    count(genre) as content_count 
    from started_streams 
    group by dt,program_title,device_name, country_code,product_type,genre;"""
    ans = pandasql.sqldf(query, locals())
    df = pd.DataFrame(ans,columns=['dt','program_title','device_name','country_code','product_type','unique_users','content_count'])
    df.to_csv("Result/question2.csv",index=False)
    l("Please check output in question2.csv")


def solve_question3(started_streams):
    #Question 3 -
    #extracting hour from time
    l("extracting hour from time")
    started_streams['hour'] = pd.to_datetime(started_streams['time'], format='%H:%M:%S').dt.hour
    #print (started_streams)

    l("applying group by to get the count of user on particular hour for a particular genre")
    #applying group by to get the count of user on particular hour for a particular genre
    ans = pandasql.sqldf(""" select hour,genre,count(user_id) as user_id from started_streams group by 1,2;""", locals())

    l("taking the max count of users for an hour")
    #taking the max count of users for an hour
    ans = pandasql.sqldf(""" select hour,max(user_id) as user_id,genre as genre from ans group by 1;""", locals())

    #converting to dataframe to write it to csv file
    df = pd.DataFrame(ans,columns=['hour','user_id','genre'])
    df.to_csv("Result/question3.csv",index=False)
    l("Please check output in question3.csv")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    l("Script started")
    l("Loading Data...!!!")
    whatson = load_whatson()
    started_streams = load_started_streams()
    l("Data loaded")
    checkResultFolder()
    l("Solving question 1")
    solve_question1(started_streams,whatson)
    l("Solving question 2")
    solve_question2(started_streams)
    l("Solving question 3")
    solve_question3(started_streams)
    l("Script ended")

