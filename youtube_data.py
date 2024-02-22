
import pandas as pd
import googleapiclient.discovery       
import streamlit as st
import pymongo
import mysql.connector
import datetime 

# Define YouTube API configuration
api_key = 'AIzaSyCNu5pqZvg3zoLk5lWYlzPhLLN1Gn706gQ'
api_service_name = "youtube"
api_version = "v3"

# Function to fetch channel details from YouTube API
def fetch_channel_details(channel_id):
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    request = youtube.channels().list(
        part="id,snippet,statistics",
        id=channel_id
    )
    response = request.execute()

    if 'items' in response and response['items']:
        channel_info = response['items'][0]
        published_at_str = channel_info['snippet'].get('publishedAt', 'N/A')
        # Parse the datetime string and convert it to MySQL format
        published_at = datetime.datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        
        channel_details = {
            'Channel ID': channel_info['id'],
            'Channel Name': channel_info['snippet']['title'],
            'Subscription Count': channel_info['statistics'].get('subscriberCount', 'N/A'),
            'Channel Views': channel_info['statistics'].get('viewCount', 'N/A'),
            'Total Videos': channel_info['statistics'].get('videoCount', 'N/A'),
            'Channel Description': channel_info['snippet'].get('description', 'N/A'),
            'Published At': published_at
        }
        return channel_details
    else:
        return None

# Function to establish connection to MongoDB
def establish_mongodb_connection():
    # Establish a connection to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    # Select or create a database
    db = client["youtube_data"]
    # Select or create a collection
    collection = db["channel_data"]
    return collection

# Function to establish connection to MySQL database
def establish_mysql_connection():
    return mysql.connector.connect(
        host='127.0.0.1',
        port=3306,
        user="root",
        password="1234",
        database="youtube_data"
    )

def create_mysql_table():
    mysql_connection = establish_mysql_connection()
    cursor = mysql_connection.cursor()

    # Define the SQL statement to create a new table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS channel_details (
        Channel_ID VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Subscription_Count INT,
        Channel_Views INT,
        Total_Videos INT,
        Channel_Description TEXT
    )
    """

    # Execute the SQL statement to create the new table
    cursor.execute(create_table_sql)

    cursor.close()
    mysql_connection.close()

# Function to migrate selected channels from MongoDB to MySQL
def migrate_selected_channels(selected_channels):
    try:
        # Establish connection to MongoDB
        mongo_collection = establish_mongodb_connection()
        # Establish connection to MySQL
        mysql_connection = establish_mysql_connection()
        cursor = mysql_connection.cursor()

        # Create MySQL table if it doesn't exist
        create_mysql_table()

        # Migrate selected channels to MySQL
        for channel_id in selected_channels:
            # Fetch channel details from MongoDB
            channel_details = mongo_collection.find_one({'Channel ID': channel_id})
            if channel_details:
                # Insert channel details into MySQL
                sql = """INSERT INTO channel_details 
                         (Channel_ID, Channel_Name, Subscription_Count, Channel_Views, Total_Videos, Channel_Description) 
                         VALUES (%s, %s, %s, %s, %s, %s)"""
                val = (
                    channel_details['Channel ID'],
                    channel_details['Channel Name'],
                    channel_details['Subscription Count'],
                    channel_details['Channel Views'],
                    channel_details['Total Videos'],
                    channel_details['Channel Description']
                )
                cursor.execute(sql, val)
        
        mysql_connection.commit()
        print("Data migration from MongoDB to MySQL completed successfully!")
    except Exception as e:
        print(f"Error occurred during data migration: {e}")
        # Rollback the transaction if an error occurs
        mysql_connection.rollback()
    finally:
        # Close cursor and connection
        cursor.close()
        mysql_connection.close()


# Define Streamlit app
def main():
    st.markdown("<h1 style='color:green;'>YOUTUBE DATA HARVESTING</h1>", unsafe_allow_html=True)
    st.header('YouTube Channel Migration')

    # Input for YouTube channel ID
    channel_id = st.text_input('Enter YouTube Channel ID:')

    # Button to view channel details
    if st.button('View Channel Details'):
        if channel_id:
            # Fetch channel details from YouTube API
            channel_details = fetch_channel_details(channel_id)
            if channel_details:
                # Display channel details
                st.write('Channel Details:')
                for key, value in channel_details.items():
                    st.write(f"{key}: {value}")
            else:
                st.write('Channel details not found.')

    # Button to insert channel details into MongoDB
    if st.button('Insert Channel Details into MongoDB'):
        if channel_id:
            # Fetch channel details from YouTube API
            channel_details = fetch_channel_details(channel_id)
            if channel_details:
                # Establish connection to MongoDB
                collection = establish_mongodb_connection()
                # Insert channel details into MongoDB
                collection.insert_one(channel_details)
                st.success('Channel details inserted into MongoDB successfully!')
            else:
                st.error('Failed to fetch channel details.')

    # Button to migrate data from MongoDB to MySQL
    if st.button('Migrate Data from MongoDB to MySQL'):
        selected_channels = []  # Provide selected channels here
        migrate_selected_channels(selected_channels)
        st.success('Data migration from MongoDB to MySQL completed successfully!')
    

    # Display questions and execute SQL queries based on selection
    questions = [
        ("Question 1: What are the names of all the videos and their corresponding channels?",
         "SELECT title, channel_id FROM video_details ORDER BY views DESC LIMIT 10;"),
        ("Question 2: Which channels have the most number of videos, and how many videos do they have?",
         "SELECT channel_name, total_videos FROM channel_details ORDER BY total_videos DESC LIMIT 5"),
        ("Question 3: What are the top 10 most viewed videos and their respective channels?",
         "SELECT title, channel_id FROM video_details ORDER BY views DESC LIMIT 10;"),
        ("Question 4: How many comments were made on each video, and what are their corresponding video names?",
         "SELECT title, comments_count FROM video_details"),
        ("Question 5: Which videos have the highest number of likes, and what are their corresponding channel names?",
         "SELECT title, likes, channel_id FROM video_details ORDER BY likes DESC"),
        ("Question 6: What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
         "SELECT title, likes, dislikes FROM video_details"),
        ("Question 7: What is the total number of views for each channel, and what are their corresponding channel names?",
         "SELECT channel_id, SUM(views) AS total_views FROM video_details GROUP BY channel_id"),
        ("Question 8: What are the names of all the channels that have published videos in the year 2022?",
         "SELECT DISTINCT channel_id FROM video_details WHERE YEAR(published_at) = 2022"),
        ("Question 9: What is the average duration of all videos in each channel, and what are their corresponding channel names?",
         "SELECT channel_id, AVG(duration) AS average_duration FROM video_details GROUP BY channel_id"),
        ("Question 10: Which videos have the highest number of comments, and what are their corresponding channel names?",
         "SELECT title, comments_count, channel_id FROM video_details ORDER BY comments_count DESC LIMIT 100")
    ]

    selected_question_index = st.selectbox("Select a question:", range(len(questions)))
    if selected_question_index is not None:
        question, query = questions[selected_question_index]
        st.subheader(question)
        st.subheader("Query:")
        st.code(query)

        # Execute the SQL query and display the result
        connection = establish_mysql_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        if result:
            df = pd.DataFrame(result, columns=[col[0] for col in cursor.description])
            st.subheader("Query Result:")
            st.write(df)
        else:
            st.write("No results found.")

        cursor.close()
        connection.close()

# Run the Streamlit app
if __name__ == '__main__':
    main()





