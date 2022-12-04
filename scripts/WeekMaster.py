import pandas as pd
import datetime as dt


def WeekMasterGenerator(start_year, end_year, reduced_hours=None, allowed_hours=80, allowed_tasks=12, output_dir=None):
    # reduced_hours: list of (star_date, end_date, hours, repetition, notes) tuples

    # Get the mondays for every week in the range(star_year - end_year)
    start_dates = pd.date_range(start=str(start_year), end=str(end_year),
                                freq='W-MON', inclusive='left')

    # Create first dataframe
    weeks_master = pd.DataFrame(start_dates.isocalendar().to_dict('records'))
    weeks_master['ScheduledWeek'] = start_dates
    weeks_master['AllowedHours'] = allowed_hours
    weeks_master['AllowedTasks'] = allowed_tasks

    # Add reduced hours
    if reduced_hours:
        for start_date, end_date, hours, repetition, notes in reduced_hours:

            date_range = (weeks_master['ScheduledWeek'] <= end_date) & (weeks_master['ScheduledWeek'] >= start_date)

            if repetition == 'O':
                weeks_master.loc[date_range, "AllowedHours"] = hours
                weeks_master.loc[date_range, "NotesReducedHours"] = notes
            elif repetition == 'Y':
                # Iterating through the years
                while start_date.year < end_year:
                    weeks_master.loc[date_range, "AllowedHours"] = hours
                    weeks_master.loc[date_range, "NotesReducedHours"] = notes
                    start_date = dt.datetime(start_date.year + 1, start_date.month, start_date.day)
                    end_date = dt.datetime(end_date.year + 1, end_date.month, end_date.day)

    # Create a csv if required
    if not output_dir == None:
        weeks_master.to_csv(output_dir + "/WeeksMaster.csv", index=False)

    return weeks_master


def BlackoutDatesGenerator(end_year, dates, output_dir=None):
    # dates: List of (start date, end date, repetition) tuples

    # Empty lists for date collection
    date_list = []
    iso_info = []
    info = []

    for start_date, end_date, repetition, notes in dates:

        # Add yearly repeating blackout dates
        if repetition == "Y":
            while start_date.year < end_year:
                # Add dates to final lists
                temp_dates = pd.date_range(start=start_date, end=end_date, freq='D')
                date_list += temp_dates.to_list()
                iso_info += temp_dates.isocalendar().to_dict('records')
                info += [notes] * len(temp_dates)

                # Iterate through years
                start_date = dt.datetime(start_date.year + 1, start_date.month, start_date.day)
                end_date = dt.datetime(end_date.year + 1, end_date.month, end_date.day)

        # Add once off blackout dates
        elif repetition == "O":
            temp_dates = pd.date_range(start=start_date, end=end_date, freq='D')
            date_list += temp_dates.to_list()
            iso_info += temp_dates.isocalendar().to_dict('records')
            info += [notes] * len(temp_dates)

            # Create blackout date data frame
    blackout_dates = pd.DataFrame(iso_info)
    blackout_dates["Date"] = date_list
    blackout_dates["NotesBlackout"] = info

    # Create a csv if required
    if not output_dir == None:
        blackout_dates.to_csv(output_dir + "/BlackoutDates.csv", index=False)

    return blackout_dates


def WeeksMinusBlackout(weeks_master, blackout_dates, output_dir=None):
    # If note blackout dates return the original weeks_master
    if not blackout_dates.empty:

        # Initialize
        updated_weeks_master = weeks_master.copy()

        # Join on weeks
        joined_table = pd.merge(weeks_master, blackout_dates, on="week", suffixes=("_wm", "_bd"))

        # Find the weeks with blackout dates
        weeks_with_reduced_hours = joined_table.loc[
            (joined_table['year_bd'] == joined_table['year_wm']),
            ['year_wm', 'week', "NotesBlackout"]]

        # Update hours
        for index, row in weeks_with_reduced_hours.iterrows():
            year = updated_weeks_master['year'] == row['year_wm']
            week = updated_weeks_master['week'] == row['week']
            daily_hours = weeks_master.loc[year & week, "AllowedHours"] / 5  # 5 days per week
            greater_than_zero = updated_weeks_master['AllowedHours'] > 0

            updated_weeks_master.loc[year & week & greater_than_zero, "AllowedHours"] -= daily_hours
            updated_weeks_master.loc[year & week, "NotesBlackout"] = row['NotesBlackout']
    else:
        updated_weeks_master = weeks_master

    # Create a csv if required
    if not output_dir == None:
        updated_weeks_master.to_csv(output_dir + "/WeeksMasterFinal.csv", index=False)

    return updated_weeks_master


def CSVDecoder(filepath, file=None):
    df = pd.read_csv(filepath)

    if df.empty:
        return []

    df["Start_Date"] = df["Start_Date"].apply(pd.to_datetime)
    df["End_Date"] = df['End_Date'].apply(pd.to_datetime)

    # Only take rows where Start_Date year and End_Date year are the same
    # and Start_Date <= End_Date. Repetition must be O or Y. 
    same_years = df["Start_Date"].dt.year == df["End_Date"].dt.year
    sd_less_ed = df["Start_Date"] <= df["End_Date"]
    rep_check = (df['Repetition'] == 'O') | (df['Repetition'] == 'Y')

    df = df.loc[same_years & sd_less_ed & rep_check]

    # Create list of tuples
    dates = list(df.itertuples(index=False, name=None))

    return dates


# TODO: Documentation on how reduced_hours and blackoutdates work. With EXAMPLES.
### Talk about Once and Yearly diff. Talk about yearly starting at dates first year.
# Can't change names of files
# If they dont put a date it will break
# Yearly holiday. Date and then date that falls onto the next Monday or Friday. 
# If there are rules exceeding the final year they simple won't be passed in. 
# Remove hours.
# Same start and end date

def CreateWeeksMaster(start_year, end_year, allowed_hours, allowed_tasks, blackouts_directory, output_directory,
                      reducedhrs_directory=None):
    dates = CSVDecoder(blackouts_directory, file="Blackouts")

    if reducedhrs_directory is not None and reducedhrs_directory != '':
        reduced_hours = CSVDecoder(reducedhrs_directory, file="ReducedHours")
    else:
        reduced_hours = None

    blackout_dates = BlackoutDatesGenerator(end_year, dates, output_dir=output_directory)

    weeks_master = WeekMasterGenerator(start_year, end_year, allowed_hours=allowed_hours,
                                       allowed_tasks=allowed_tasks, reduced_hours=reduced_hours,
                                       output_dir=output_directory)

    weeks_master_final = WeeksMinusBlackout(weeks_master, blackout_dates, output_dir=output_directory)

    weeks_master_final['HardCapped'] = 0

    return weeks_master_final


if __name__ == '__main__':
    # dates = [(dt.datetime(2022, 3, 8), dt.datetime(2022, 3, 8), "O"), 
    #             (dt.datetime(2022, 12, 25), dt.datetime(2022, 12, 26), "Y"), 
    #             (dt.datetime(2023, 8, 25), dt.datetime(2023, 8, 26), "O"), 
    #             (dt.datetime(2022, 1, 3), dt.datetime(2022, 1, 3), "Y"),
    #             (dt.datetime(2024, 12, 30), dt.datetime(2024, 12, 30), "O")
    #         ]

    # reduced_hours = [(dt.datetime(2022, 6, 30), dt.datetime(2022, 9, 11), 60, "O")]

    # dates = CSVDecoder("csv/Blackouts.csv", file="Blackouts")

    # reduced_hours = CSVDecoder("csv/ReducedHours.csv", file="ReducedHours")

    # blackout_dates = BlackoutDatesGenerator(2033, dates, output_dir="csv")

    # weeks_master = WeekMasterGenerator(2023, 2033, reduced_hours=reduced_hours, output_dir="csv")

    # WeeksMinusBlackout(weeks_master, blackout_dates, output_dir="csv")

    CreateWeeksMaster(2023, 2033, 80, 12, "csv/Blackouts.csv", "csv/ReducedHours.csv", "csv")
