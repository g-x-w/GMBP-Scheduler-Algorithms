import pandas as pd
from .AbstractScheduleAlgorithm import AbstractScheduleAlgorithm


class TopDownBackScheduler(AbstractScheduleAlgorithm):
    def create_schedule(self, clean_df, wm_df, forecast_years, hardcap):
        base_sched = AbstractScheduleAlgorithm.build_base_top_down_schedule(clean_df, forecast_years=forecast_years)
        sched_df = self.do_weekly_hour_cap(base_sched, wm_df, hardcap=hardcap)
        AbstractScheduleAlgorithm.check_valid_schedule(sched_df, wm_df, type(self).__name__)
        AbstractScheduleAlgorithm.check_complete_task_list(clean_df, sched_df, type(self).__name__)
        return sched_df


    def do_weekly_hour_cap(self, df, weekly_hours, scale=0.25, hardcap={}, **kwargs):
        """
            Add fields to base schedule: week_priority_score inferred from task_sequence_weeks/(deltaweeks+1), deltaDays
            Logic:
                for each week in weeks_minus_blackout, pull all task rows from base schedule in that week
                sum the hours of all those tasks
                if there's more hours of tasks than is available in the week
                then move the tasks to the next week until the sum of hours is enough
                tasks that get moved are those with highest week_priority_score (i.e. infrequent things get moved first)
                ++ 1 to the deltaweeks for moved tasks
                ++ 7 to the deltadays for moved tasks
                modify the scheduled Date field for moved tasks to same weekday of following week
                modify to week field value for moved tasks
        """

        # TODO: maybe augment priority scoring metric with a hard cap on DeltaWeeks and/or DeltaDays?
        # Monthly or 3-monthly should have hard caps
        # little more flexibility with longer-term frequency tasks beyond that, but hard caps still good idea
        # maybe soft-code as arguments/dropdowns in the gui

        df['WeekPriorityScore'] = pd.eval(f"(df.TaskSequence_Weeks)//(df.DeltaWeeks+1) + {scale}*df.Hrs")

        weekly_hours['HardCapped'] = 0

        for week in weekly_hours.to_dict('records'):
            week_tasks = df.loc[df['ScheduledWeek'] == week['ScheduledWeek']]
            next_week = pd.to_datetime(week['ScheduledWeek']) + pd.DateOffset(days=7)
            if week['AllowedHours'] == 0 or week['AllowedHours'] == 0:
                for (i, row) in week_tasks.iterrows():
                    df.at[i, 'Week'] = AbstractScheduleAlgorithm.week_helper(df.at[i, 'Week'])
                    df.at[i, 'Scheduled_Date'] = df.at[i, 'Scheduled_Date'] + pd.DateOffset(days=7)
                    df.at[i, 'ScheduledWeek'] = next_week
                    df.at[i, 'DeltaWeeks'] += 1
                    df.at[i, 'Year'] = df.at[i, 'ScheduledWeek'].year
                    df['WeekPriorityScore'] = pd.eval(f"(df.TaskSequence_Weeks)//(df.DeltaWeeks+1)+ {scale}*df.Hrs")

            else:
                while week_tasks['Hrs'].sum() > week['AllowedHours'] or len(week_tasks) > week['AllowedTasks']:

                    priority = week_tasks.loc[week_tasks['HardCapped'] == 0]['WeekPriorityScore'].idxmax()
                    df.at[priority, 'Week'] = AbstractScheduleAlgorithm.week_helper(df.at[priority, 'Week'])
                    df.at[priority, 'Scheduled_Date'] = df.at[priority, 'Scheduled_Date'] + pd.DateOffset(days=7)
                    df.at[priority, 'ScheduledWeek'] = next_week
                    df.at[priority, 'DeltaWeeks'] += 1
                    if (freq := df.at[priority, 'TaskSequence_Weeks']) in hardcap.keys():
                        if df.at[priority, 'DeltaWeeks'] >= hardcap[freq]:
                            df.at[priority, 'HardCapped'] = 1
                    df.at[priority, 'Year'] = df.at[priority, 'ScheduledWeek'].year
                    df['WeekPriorityScore'] = pd.eval(f"(df.TaskSequence_Weeks)//(df.DeltaWeeks+1)+ {scale}*df.Hrs")
                    week_tasks = df.loc[df['ScheduledWeek'] == week['ScheduledWeek']]

        df['WeekPriorityScore'].astype(int)
        df['DeltaDays'] = pd.eval('df.DeltaWeeks*7')

        return df
