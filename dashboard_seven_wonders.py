from pytrends.request import TrendReq
import time
import pandas as pd

def main(search_terms, countries_filt, start_date, end_date, conn_string):

    df_list = []
    for search_term in search_terms:
        
        for country in countries_filt: 

            pytrends.build_payload(kw_list=[search_term], cat=67, timeframe= f'{start_date} {end_date}', geo= country, gprop='') #cat = 12 for Autos & Vehicles
            interest_over_time_df = pytrends.interest_over_time()
            interest_over_time_df = interest_over_time_df.reset_index()
            interest_over_time_df["Country"] = country
            interest_over_time_df["Wonder of world"] = search_term
     
            print(search_term)
            print(country)
            print(interest_over_time_df)
            df_list.append(interest_over_time_df)
            
            print('yasta')
            time.sleep(3)

    final_df = pd.concat(df_list, axis=0)

    print('PROCESS FINISHED')
    print(final_df)
    print(final_df.columns)

    
"""
    #Transform Phase

    #Drop
    final_df.drop(columns=['date', 'isPartial'], index= 0 ,inplace = True)

    #Groupby
    report = final_df.groupby(['Country', 'Wonder of world']).agg({'Great Wall of China' : 'sum', 'Chichen Itza' : 'sum', 'Petra' : 'sum',
                                                'Machu Picchu' : 'sum', 'Christ the Redeemer' : 'sum', 'Colosseum' : 'sum','Taj Mahal' : 'sum'})
    
    #Off index
    report.reset_index(inplace = True)

    #Fixed table
    selected_columns = ["Great Wall of China", "Chichen Itza", "Petra", "Machu Picchu", "Christ the Redeemer", 
                        "Colosseum", "Taj Mahal"]

    row_sum = report.loc[:, selected_columns].sum(axis =1)

    report['#Number'] = row_sum.values

    #Drop columns that I won't use anymore

    report.drop(columns = selected_columns, axis = 0, inplace = True)

    #Change name of variables in the dataframe
    report['Country'] = report['Country'].replace({'US':'United States', 'CA' : 'Canada'})

    # Create a table in the database using the dataframe
    report.to_sql(name = "2021_2022_vehicles", con = conn_string, if_exists="replace")

    print('ya esta')

"""
if __name__ == '__main__':

    conn_string = "postgresql://postgres:snow@localhost:5432/seven_wonders"

    requests_args = {
    'headers': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
            }
    }

    pytrends = TrendReq(requests_args= requests_args)

    search_terms = ["Great Wall of China", "Chichen Itza", "Petra", "Machu Picchu", "Christ the Redeemer", "Colosseum", "Taj Mahal"]

    countries_filt = ["US" , "CA"]

    start_date = '2021-01-01'

    end_date = '2022-12-31'

    main(search_terms, countries_filt, start_date, end_date, conn_string)