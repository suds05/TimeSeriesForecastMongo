import debugpy
import os
import statsmodels.api as sm
import pandas as pd
import matplotlib.pyplot as plt
from AtlasClient import AtlasClient
from dotenv import load_dotenv

def debugger_attach():
    debugpy.listen(("0.0.0.0", 5678))
    print("Waiting for debugger attach...")
    debugpy.wait_for_client()
    print("Debugger attached")

# Plot the aggregation results for movies by year

def plot_movies_by_quarter(aggs):
    # Convert the aggregation results to a DataFrame
    df = pd.DataFrame(aggs)

    # Add a new field combining the year and month as a string
    df['year_window'] = df['_id'].apply(
        lambda x: f"{x['year']}-{x['quarter']:02d}")
    
    # # Filter the DataFrame to only include records between 2010 and 2015
    df = df[(df['_id'].apply(lambda x: x['year']) >= 2000) & (df['_id'].apply(lambda x: x['year']) <= 2003)]

    # Plot the histogram
    plt.figure(figsize=(30, 18))
    plt.plot(df['year_window'], df['movies_in_window'])
    plt.xlabel('Time')
    plt.ylabel('Movies')
    plt.title('Total Movies by Time window')
    plt.xticks(rotation=90)
    plt.tight_layout()

     # plt.show()
    # Save the plot to a file
    plt.savefig('plot_movies_by_quarter.png')
    print('Plot saved as plot_movies_by_quarter.png')

    print('Done!')


def plot_movies_by_year(aggs):
    # Convert the aggregation results to a DataFrame
    df = pd.DataFrame(aggs)

    # Add a new field combining the year and month as a string
    df['year_window'] = df['_id']
    
    # Plot the histogram
    plt.figure(figsize=(30, 18))
    plt.bar(df['year_window'], df['movies_in_window'])
    plt.xlabel('Time')
    plt.ylabel('Movies')
    plt.title('Total Movies by Time window')
    plt.xticks(rotation=90)
    plt.tight_layout()

    # plt.show()
    # Save the plot to a file
    plt.savefig('plot_movies_by_year.png')
    print('Plot saved as plot_movies_by_year.png')

    print('Done!')

# Plot the aggregation results for movies by director
def plot_movies_by_director(aggs):
    # Convert the aggregation results to a DataFrame
    df = pd.DataFrame(aggs)

    # Sort the DataFrame by totalMovies in descending order and select the top 10
    top_10_df = df.sort_values(by='totalMovies', ascending=False).head(10)

    # Plot the histogram
    plt.figure(figsize=(20, 12))
    plt.bar(top_10_df['_id'], top_10_df['totalMovies'])
    plt.xlabel('Directors')
    plt.ylabel('Total Movies')
    plt.title('Total Movies by Director')
    plt.xticks(rotation=90)
    plt.tight_layout()

    # plt.show()
    # Save the plot to a file
    plt.savefig('plot.png')
    print('Plot saved as plot.png')

    print('Done!')

# graphs to show seasonal_decompose
def seasonal_decompose(aggs):
    # Convert the aggregation results to a DataFrame
    df = pd.DataFrame(aggs)

    # Filter to years 2004 to 2019
    df = df[(df['_id'].apply(lambda x: x['year']) >= 2004) & (df['_id'].apply(lambda x: x['year']) <= 2008)]

    # Combine the year and quarter fields inside _id as a string in the format 'YYYYQX'. 
    # This format aids in building a datetime index with pandas
    df['year_quarter'] = df['_id'].apply(lambda x: f"{x['year']}-Q{x['quarter']}")
    df.drop(columns=['_id'], inplace=True)

    # Drop any rows with missing year_quarter values
    df = df.dropna(subset=['year_quarter'])

    # Drop any rows with missing movies_in_window values
    df = df.dropna(subset=['movies_in_window'])

    # Convert year_quarter to a datetime column set to the first day of the quarter
    df['year_quarter'] = pd.PeriodIndex(df['year_quarter'], freq='Q').to_timestamp()

    # Set the year_quarter column as the DataFrame index
    df.set_index('year_quarter', inplace=True)

    # Perform seasonal decomposition    
    decomposition = sm.tsa.seasonal_decompose(df['movies_in_window'], model='additive', extrapolate_trend='freq')
    fig = decomposition.plot()
    fig.set_size_inches(30,14)
    plt.savefig('plot_seasonality_trends.png')
    print('Plot saved as plot_seasonality_trends.png')

    print('Done!')

# debugger_attach()

load_dotenv()  # Load environment variables from .env file

DB_NAME = 'sample_mflix'
COLLECTION_NAME = 'embedded_movies'

# Define the Atlas connection string in a .env file. e.g
# ATLAS_URI=mongodb+srv://USER:PASSWORD@cluster0.utnet.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0
ATLAS_URI = os.environ.get('ATLAS_URI')
print('Connecting to Atlas instance ', ATLAS_URI)
atlas_client = AtlasClient(ATLAS_URI, DB_NAME)
atlas_client.ping()
print('Connected to Atlas instance! We are good to go!')

# # Aggregate movies by director and print them
# aggs = atlas_client.agg_movies(COLLECTION_NAME, 'directors')
# plot_movies_by_director(aggs)

# Aggregate movies by year
aggs1 = atlas_client.agg_movies_by_year(COLLECTION_NAME)

# Plot the aggregation results for movies by year
plot_movies_by_year(aggs1)

# Aggregate movies by quarter
aggs2 = atlas_client.agg_movies_by_quarter(COLLECTION_NAME, 2004, 2014)

# Plot the aggregation results for movies by quarter
plot_movies_by_quarter(aggs2)

# Plot the seasonal decomposition
aggs3 = atlas_client.agg_movies_by_quarter(COLLECTION_NAME, 2004, 2014)
seasonal_decompose(aggs3)