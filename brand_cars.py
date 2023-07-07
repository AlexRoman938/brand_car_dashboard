from pytrends.request import TrendReq
import time
import pandas as pd

def main(search_terms, countries_filt, start_date, end_date, conn_string):


    """
    INPUT: ALL VARIABLES THAT WE WANT TO SEARCH AND FILTER
    
    OUTPUT: LOADING "FINAL_DATAFRAME" TO DATABASE
                      WITH THE BELOW COLUMNS "COUNTRY, BRAND, #NUMBER (NUMBER SEARCH)
    """


    df_list = []
    for search_term in search_terms:
        
        for country in countries_filt: 

            pytrends.build_payload(kw_list=[search_term], cat=12, timeframe= f'{start_date} {end_date}', geo= country) #cat = 12 for Autos & Vehicles
            interest_over_time_df = pytrends.interest_over_time()
            interest_over_time_df = interest_over_time_df.reset_index()
            interest_over_time_df["Country"] = country
            interest_over_time_df["Brand"] = search_term
     
            print(search_term)
            print(country)
            print(interest_over_time_df)

            #Concat all dataframes of df_list
            df_list.append(interest_over_time_df)
            
            print(f'Dataframe for {search_term} and {country} added')
            print('Next Search')

            """
            time.sleep(n) is used to introduce a delay or pause 
            in the execution of a program for a specified number of seconds. 
            In this case, This helps us pause for 3 seconds to avoid 
            overwhelming the Google Trends API.
            """

            time.sleep(3)

    final_df = pd.concat(df_list, axis=0)

    print('GOOGLE TRENDS SEARCHING IS FINISHED')
    print(final_df)
    print(final_df.columns)

    

    """
    Featuring engineering

    """

    #Drop
    final_df.drop(columns=['date', 'isPartial'], index= 0 ,inplace = True)

    #Groupby
    report = final_df.groupby(['Country', 'Brand']).agg({'Kia' : 'sum', 'Mitsubishi' : 'sum', 'Peugeot' : 'sum',
                                                'Fuso' : 'sum', 'Chery' : 'sum', 'MG' : 'sum','GAC Motor' : 'sum'})
    
    #Off index
    report.reset_index(inplace = True)

    #Fixed table
    selected_columns = ['Kia','Mitsubishi', 'Peugeot', 'Fuso', 'Chery',
       'MG', 'GAC Motor']

    row_sum = report.loc[:, selected_columns].sum(axis =1)

    report['#Number'] = row_sum.values

    #Drop columns that I won't use anymore

    report.drop(columns = selected_columns, axis = 0, inplace = True)

    #Change name of variables in the dataframe
    report['Country'] = report['Country'].replace({'AR':'Argentina', 'BO' : 'Bolivia', 'CL' : 'Chile', 'CO' : 'Colombia',
                                                 'PE' : 'Peru'})

    # Create a table in the database using the dataframe
    report.to_sql(name = "2021_2022_vehicles", con = conn_string, if_exists="replace")

    print('Finished')


if __name__ == '__main__':

    """
    conn_string is used to establish a connection to PostgreSQL database.
    Remember to edit by our username and database_name
    
    """

    username = "snow"
    database_name = "car_project"
    conn_string = f"postgresql://postgres:{username}@localhost:5432/{database_name}"

    """
    requests_arg:
    It sets up a custom User-Agent header for making HTTP requests using 
    the requests library in Python.

    The User-Agent header is part of the HTTP protocol and is used to 
    identify the client making the request. It typically includes 
    information about the client's operating system, browser, and version.
    """

    requests_args = {
    'headers': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
            }
    }

    pytrends = TrendReq(requests_args= requests_args)

    search_terms = ["Kia", "Mitsubishi", "Peugeot", "Fuso", "Chery", "MG", "GAC Motor"]

    countries_filt = ["AR", "BO", "CL", "CO", "PE"]

    start_date = '2021-01-01'

    end_date = '2022-12-31'

    main(search_terms, countries_filt, start_date, end_date, conn_string)