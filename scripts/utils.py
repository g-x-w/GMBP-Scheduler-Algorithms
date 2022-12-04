import pandas as pd
from sqlalchemy import create_engine

def write_to_sql(df):
    server = "localhost"
    database = "master"
    driver = "ODBC Driver 17 for SQL Server"
    db_con = f'mssql://@{server}/{database}?driver={driver}'
    engine = create_engine(db_con)
    df.to_sql('cleaned_data', engine, if_exists='replace')


def read_from_sql(table_name):
    server = "localhost"
    database = "master"
    driver = "ODBC Driver 17 for SQL Server"
    db_con = f'mssql://@{server}/{database}?driver={driver}'
    engine = create_engine(db_con)
    df = pd.read_sql_table(table_name, engine)
    return df


def load_csv(filepath, index_col=None):
    # Load CSV file as Pandas DataFrame
    # NOTE: There seems to be a bullet point character that can't be parsed. Loading with replacement character for now.
    # TODO: Column type conversion?
    df = pd.read_csv(filepath, index_col=index_col, encoding_errors="replace")
    return df

def split_by_trade(df):
    UniqueNames = df.Trade.unique()

    #create a data frame dictionary to store data frames
    DataFrameDict = {elem : pd.DataFrame() for elem in UniqueNames}

    for key in DataFrameDict.keys():
        if 'Contractor Name' in df.keys():
            DataFrameDict[key] = df[:][(df.Trade == key) & (df.ContractorName == "")]
        else:
            DataFrameDict[key] = df[:][df.Trade == key]
    
    return DataFrameDict

def clean_dataframe(raw_df, max_allowed_hours=80):
    # Specify columns to extract
    columns = [
        "Index", "Data Source", "Task Description", "Task Sequence", "Task Sequence (Weeks)",
        "Trade", "Hrs", "Consolidated Dates"
    ]

    # Extract relevant columns for processed table
    df = raw_df[columns]

    # Convert Consolidated Dates to datetime
    # TODO: Avoid hardcoding the name?
    df["Consolidated Dates"] = df["Consolidated Dates"].apply(pd.to_datetime)

    # Convert Task Sequence (Weeks) to a timeDelta so that we can manipulate dates in a vectorized manner

    df["Task Sequence (Weeks)"] = pd.to_timedelta(df["Task Sequence (Weeks)"], unit='w')  # this gives days instead of weeks (which is probably fine anyway)

    # Create relevant columns (Year, Week, Last Service Date) from Consolidated Dates column
    df["Year"] = df["Consolidated Dates"].dt.isocalendar().year
    df["Week"] = df["Consolidated Dates"].dt.isocalendar().week
    df["Estimated Last Service Date"] = df["Consolidated Dates"] - df["Task Sequence (Weeks)"]

    # Rename columns
    df = df.rename(columns={
        "Data Source": "DataSource",
        "Task Description": "TaskDescription",
        "Task Sequence": "TaskSequence",
        "Task Sequence (Weeks)": "TaskSequence_Weeks",
        "Trade": "Trade",
        "Hrs": "Hrs",
        "Year": "Year",
        "Week": "Week",
        "Consolidated Dates": "ConsolidatedDates",
        "Estimated Last Service Date": "EstimatedLastServiceDate",
        "Index": "Key",
    })

    # Reorder columns
    df = df[[
        "Key",
        "DataSource",
        "TaskDescription",
        "TaskSequence",
        "TaskSequence_Weeks",
        "Trade",
        "Hrs",
        "Year",
        "Week",
        "ConsolidatedDates",
        "EstimatedLastServiceDate",
    ]]
    df["TaskSequence_Weeks"] = df["TaskSequence_Weeks"].dt.days.astype(int) // 7  # Transform into # of weeks integer
    df = df.sort_values(by='Key')

    over_length_tasks = df[df["Hrs"] > max_allowed_hours]
    assert over_length_tasks.empty, \
        f"Task(s) {over_length_tasks['DataSource'].values} takes more than the max allowed_hours of {max_allowed_hours}!"

    return df

def split_by_trade(df):
    UniqueNames = df.Trade.unique()
    #create a data frame dictionary to store data frames
    DataFrameDict = {elem : pd.DataFrame() for elem in UniqueNames}
    for key in DataFrameDict.keys():
        if 'Contractor Name' in df.keys():
            DataFrameDict[key] = df[:][(df.Trade == key) & (df.ContractorName == "")]
        else:
            DataFrameDict[key] = df[:][df.Trade == key]
    
    return DataFrameDict
def produce_final_schedule(df, original_csv, out_link):
    df_original = load_csv(original_csv)
    original_keys = list(df_original.columns.values)
    cols = [
        "Key", "Data Source", "Task Description", "Task Sequence", "Task Sequence (Weeks)",
        "Trade", "Hrs", "Consolidated Dates"
    ]
    other_cols = []
    for i in original_keys:
        if i not in cols:
            other_cols.append(i)
    df_original_new = df_original[other_cols].copy()
    df_original_new.rename(columns={'Index':'Key'}, inplace=True)
    df_original_new['Long Text'] = df_original_new['Long Text'].apply(lambda x: re.sub(r'[^A-Za-z0-9 \n.,_:;-]+', '', x))
    df = df.iloc[:, 1:]
    df_final = pd.merge(df, df_original_new, on=['Key'])
    df_final.to_csv(out_link, encoding='UTF-8')