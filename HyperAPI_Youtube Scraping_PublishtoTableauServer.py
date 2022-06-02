###################################################################################################################################
# This script is structured in three main parts:
# 1. Using the Youtube API to extract information on the most popular Data science and Data engineering videos
# 2. Push the output to a hyper file so that it can be open straight in Tableau
    # 2a. Each table is pushed to a single hyper file
    # 2b. The two tables are combined in a single hyper file with multiple tables 
# 3. Push the hyper files to Tableau Server as Published DataSources so they can be accessed from there straight  
###################################################################################################################################

import googleapiclient.discovery
import os
import re
import pandas as pd
import pantab
from pathlib import Path
import tableauserverclient as TSC

# 1. Using the Youtube API to extract information on the most popular Data science and Data eng videos

# API info
api_service_name = "youtube"
api_version = "v3"
api_key = 'YOUR_YOUTUBE_API_KEY'

# API connect
youtube = googleapiclient.discovery.build(
                                        api_service_name, 
                                        api_version, 
                                        developerKey = api_key
                                        )

# Set-up function to search Youtube content
def youtube_videodetails(keyword):
    query = youtube.search().list(
                part="id, snippet",
                type='video',
                # Here you can specify the key words for your search
                q=keyword,
                # The order parameter specifies the method that will be used to order resources in the API response.
                # Allowed values are:
                    # date
                    # rating
                    # relevance
                    # title
                    # videoCount
                    # viewCount
                order="viewCount",
                videoDefinition='high' or 'medium',
                maxResults=100,
                fields="items(id(videoId), snippet(title), snippet(description))"
                )
    return query.execute()


# Query execution for data science
request_data_science = youtube_videodetails('data science')
# Query execution for data engineering
request_data_engineering = youtube_videodetails('data engineering')
# Print the results in .json format to check the structure
print(request_data_science)

# Function that we will need to format how the video duration looks like
def duration_formatted(duration):
    def to_digit(string):
        return(
                int(''.join(
                            [x for x in string if x.isdigit()]
                            )
                    )
                )
    formatted = re.match('PT(\d+H)?(\d+M)?(\d+S)?', duration).groups()
    hours = to_digit(formatted[0]) if formatted[0] else 0
    minutes = to_digit(formatted[1]) if formatted[1] else 0
    seconds = to_digit(formatted[2]) if formatted[2] else 0

    return str(hours) + ':' + str(minutes) + ':' + str(seconds)

# Function to get and structure the information of each video
def extract_and_store_info(json_output):

    # Dictionaries to store the video info
    dictionary = {
        'id':[],
        'title':[],
        'description':[],
        'duration':[],
        'views':[],
        'likes':[]
    }

    for item in json_output['items']:
        # Getting the id and title
        vidId = item['id']['videoId']
        title = item['snippet']['title']
        description = item['snippet']['description']
        # Getting stats of the video
        r = youtube.videos().list(
                                    part="statistics,contentDetails",
                                    id=vidId,
                                    fields="items(statistics," + "contentDetails(duration))"
                                    ).execute()
        duration = duration_formatted(
                                    r['items'][0]['contentDetails']['duration']
                                    )
        try:
            views = r['items'][0]['statistics']['viewCount']
            likes = r['items'][0]['statistics']['likeCount']
            dictionary['id'].append(vidId)
            dictionary['title'].append(title)
            dictionary['description'].append(description)
            dictionary['duration'].append(duration)
            dictionary['views'].append(views)
            dictionary['likes'].append(likes)
        except KeyError:
            print(vidId + ': This video contains missing information')       
    return pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in dictionary.items() ]))

data_engineering_videos = extract_and_store_info(request_data_engineering)
data_science_videos = extract_and_store_info(request_data_science)



# 2. Push the output to a hyper file so that it can be open straight in Tableau

## Save the files locally
print('The .json files will be saved here: ' + os.getcwd())

# Files
hyper_filename_1 = "data_science_info.hyper"
hyper_filename_2 = "data_eng_info.hyper"
hyper_filename_3 = "data_video.hyper"

#######################################################################################################
### WAY 1 (2a): Creating two .hyper output and Publish two separate datasources on the Server
#######################################################################################################

# Create the .hyper file
pantab.frame_to_hyper(data_science_videos, hyper_filename_1, table = 'Data science videos')
pantab.frame_to_hyper(data_engineering_videos, hyper_filename_2, table = 'Data engineering videos')

#######################################################################################################
### WAY 2 (2b): Creating a single .hyper file with two tables and Publish it on the Server
#######################################################################################################

dict_df = { "Data engineering videos": data_engineering_videos, 
            "Data science videos": data_science_videos}

pantab.frames_to_hyper(dict_df, hyper_filename_3)




# 3. Push the hyper files to Tableau Server as Published DataSources so they can be accessed from there straight  

# Specify the login variables and Project/DS Info
token_name = 'YOUR_TOKEN_NAME'
personal_access_token = 'YOUR_TOKEN_VALUE'
site_id = "YOUR_SITE_ID"
server_name = "YOUR_SERVER_NAME"
project_name = "YOUR_PROJECT_NAME"

# Sign in to server
tableau_auth = TSC.PersonalAccessTokenAuth( token_name = token_name, 
                                            personal_access_token = personal_access_token, 
                                            site_id = site_id )
server = TSC.Server(server_name, use_server_version=True)

print(f"Signing into Tableau Server and publishing based on " + hyper_filename_1)

with server.auth.sign_in(tableau_auth):
    # Get project_id from project_name
    all_projects, pagination_item = server.projects.get()
    for project in TSC.Pager(server.projects):
        # Specify the project where you want the datasource to be published
        if project.name == project_name:
            project_id = project.id

    # Create the datasource object with the project_id
    new_datasource = TSC.DatasourceItem(project_id)

    # Publish datasource (here based on the first hyper file we created: data_science_info.hyper stored in hyper_filename_1)
    try:
        datasource = server.datasources.publish(new_datasource, hyper_filename_1,  mode = 'CreateNew')
        print("Datasource published. Datasource ID: {0}".format(datasource.id))
        message = "Datasource " + hyper_filename_1 + " published. Datasource ID: {0}".format(datasource.id) + " successfull\n"
    except:
        message = hyper_filename_1 + " FAILED\n"