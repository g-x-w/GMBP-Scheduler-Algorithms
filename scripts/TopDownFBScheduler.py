import random
import pandas as pd
from .AbstractScheduleAlgorithm import AbstractScheduleAlgorithm


class TopDownFBScheduler(AbstractScheduleAlgorithm):
    def create_schedule(self, clean_df, wm_df, forecast_years, hardcap):
        base_sched = AbstractScheduleAlgorithm.build_base_top_down_schedule(clean_df, forecast_years=forecast_years)
        sched_df = self.weekly_fba(base_sched, wm_df, hardcap=hardcap)
        AbstractScheduleAlgorithm.check_valid_schedule(sched_df, wm_df, type(self).__name__)
        AbstractScheduleAlgorithm.check_complete_task_list(clean_df, sched_df, type(self).__name__)
        return sched_df


    def weekly_fba(self, sched, week_master_df, scale=0.25, hardcap={}, **kwargs):
        """
            Logic:
                # 1. augment weeks master with AvailableHours column = AllowedHours - AssignedHours
                # (calculated from schedule df, maybe also consider storing as column for minimal computation/df access)
                # 2. take the first week with negative AvailableHours (overbooked)
                # 3. group Tasks To Shift idx's until the sum of hours of TTS set offsets the AvailableHours overbooking
                # ^^ take tasks in order of priority score
                # 4. check if n-1 and n+1 week window has enough AvailableHours to house all TTS
                # ^^ this step risks bc task hrs are quantized
                # 5. if not enough AvailableHours, expand window size +1
                # 6. otherwise, check if hour quanta are sufficient
                # ^^ requires checking if tasks can actually be inserted, which can be one step with intermediate storage
                # 7. take highest priority score -> check n-w or n+w if it fits (least sensitive task gets moved furthest in window)
                # ^^ if it can, insert in the one with more free hours; if equal do a coin toss
                # 8. re-update all AvailableHours column values again
                # 9. loop through 2-8 until no weeks with negative AvailableHours
        """

        # NOTE: issue here at start/end date boundary conditions;
        # sched df comes from schedule csv, weeksmaster is generated with defined bounds

        sched['WeekPriorityScore'] = pd.eval(
            f"(sched.TaskSequence_Weeks)//(sched.DeltaWeeks+1) + {scale}*sched.Hrs")
        sched['WeekTasks'] = 1

        week_tasks = sched.groupby(['ScheduledWeek']).sum(numeric_only=True)

        wm_df = week_master_df.copy(deep="True")
        wm_df['HardCapped'] = 0
        wm_df = week_tasks[['Hrs', 'WeekTasks']].join(wm_df.set_index('ScheduledWeek'), on='ScheduledWeek', how='outer')
        wm_df = wm_df.set_index('ScheduledWeek')
        wm_df = wm_df.fillna(0)
        wm_df.rename(columns={'Hrs': 'AssignedHours', 'WeekTasks': 'AssignedTasks'}, inplace=True)
        wm_df['AvailableHours'] = pd.eval("wm_df.AllowedHours - wm_df.AssignedHours")
        wm_df['AvailableTasks'] = pd.eval("wm_df.AllowedTasks - wm_df.AssignedTasks")

        overbooked = wm_df.loc[(wm_df['AvailableHours'] < 0) | (wm_df['AvailableTasks'] < 0)]
        while len(overbooked) > 0:

            hours = overbooked.iloc[0]['AvailableHours']
            num_tasks = overbooked.iloc[0]['AvailableTasks']
            week = overbooked.index[0]
            tasks = sched.loc[sched['ScheduledWeek'] == week]
            range_start = wm_df.iloc[0]._name
            range_end = wm_df.iloc[-1]._name

            for i in range(len(tasks)):
                if tasks.nlargest(i+1, 'WeekPriorityScore')['Hrs'].sum() >= abs(hours) or \
                        tasks.nlargest(i+1, 'WeekPriorityScore')['WeekTasks'].sum() >= abs(num_tasks):
                    tts = tasks.nlargest(i+1, 'WeekPriorityScore').sort_values(by='WeekPriorityScore', ascending=False)
                    break

            for (i, row) in tts.iterrows():
                window = 1
                adjacents = {}

                if (freq := row['TaskSequence_Weeks']) in hardcap.keys():
                    if row['DeltaWeeks'] >= hardcap[freq]:
                        row['HardCapped'] = 1
                        print('hardcap breaK')
                        break

                if not row['HardCapped']:
                    while len(adjacents) == 0:
                        offset = pd.DateOffset(days=7 * window)
                        backdate = row['ScheduledWeek'] + offset
                        fwddate = row['ScheduledWeek'] - offset
                        try:
                            if row['ScheduledWeek'] == range_start:
                                back = wm_df.loc[backdate, 'AvailableHours']
                                back_tasks = wm_df.loc[backdate, 'AvailableTasks']
                                if back >= row['Hrs'] and back_tasks >= row['WeekTasks']:
                                    adjacents[backdate] = window
                                else:
                                    window += 1
                                    if (freq := row['TaskSequence_Weeks']) in hardcap.keys():
                                        if window > hardcap[freq]:
                                            print('hardcap break 1')
                                            break

                            elif row['ScheduledWeek'] == range_end:
                                fwd = wm_df.loc[fwddate, 'AvailableHours']
                                fwd_tasks = wm_df.loc[backdate, 'AvailableTasks']
                                if fwd >= row['Hrs'] and fwd_tasks >= row['WeekTasks']:
                                    adjacents[fwddate] = -window
                                else:
                                    window += 1
                                    if (freq := row['TaskSequence_Weeks']) in hardcap.keys():
                                        if window > hardcap[freq]:
                                            print('hardcap break 2')
                                            break

                            else:
                                back = wm_df.loc[backdate, 'AvailableHours']
                                fwd = wm_df.loc[fwddate, 'AvailableHours']
                                back_tasks = wm_df.loc[backdate, 'AvailableTasks']
                                fwd_tasks = wm_df.loc[backdate, 'AvailableTasks']
                                if back == fwd and fwd >= row['Hrs'] and fwd_tasks >= row['WeekTasks']:
                                    # adjacents[backdate] = window
                                    adjacents[fwddate] = -window
                                elif back >= row['Hrs'] and fwd >= row['Hrs'] and fwd_tasks >= row['WeekTasks'] and back_tasks >= row['WeekTasks']:
                                    tmp_entry = max([backdate, fwddate], key=lambda x: wm_df.loc[x, 'AvailableHours'])
                                    adjacents[tmp_entry] = window if tmp_entry == backdate else -window
                                elif fwd >= row['Hrs'] and fwd_tasks >= row['WeekTasks']:
                                    adjacents[fwddate] = -window
                                elif back >= row['Hrs'] and back_tasks >= row['WeekTasks']:
                                    adjacents[backdate] = window
                                else:
                                    # print(fwd, row['Hrs'], fwd_tasks, row['WeekTasks'])
                                    window += 1
                                    if (freq := row['TaskSequence_Weeks']) in hardcap.keys():
                                        if window > hardcap[freq]:
                                            print('hardcap break 3')
                                            break
                        except KeyError:
                            print('KeyError: boundary week may be out of range')
                            break

                    # print(adjacents)
                    assert adjacents, f"Hard cap constraint too strict for task sequence week frequency {freq}!"
                    shift = random.choice(list(adjacents.keys()))

                    assert isinstance(row, object)
                    priority = row._name

                    sched.at[priority, 'Week'] = AbstractScheduleAlgorithm.week_helper(sched.at[priority, 'Week'], adjacents[shift])
                    sched.at[priority, 'Scheduled_Date'] = sched.at[priority, 'Scheduled_Date'] \
                                                        + pd.DateOffset(days=7*adjacents[shift])
                    sched.at[priority, 'ScheduledWeek'] = shift
                    sched.at[priority, 'DeltaWeeks'] += adjacents[shift]
                    if (freq := sched.at[priority, 'TaskSequence_Weeks']) in hardcap.keys():
                        if abs(sched.at[priority, 'DeltaWeeks']) >= hardcap[freq]:
                            sched.at[priority, 'HardCapped'] = 1
                    sched.at[priority, 'Year'] = sched.at[priority, 'ScheduledWeek'].year

                    sched['WeekPriorityScore'] = pd.eval(
                        f"(sched.TaskSequence_Weeks)//(sched.DeltaWeeks.abs()+1) + {scale}*sched.Hrs")

                    week_tasks = sched.groupby(['ScheduledWeek']).sum(numeric_only=True).fillna(0)

                    wm_df.drop(['AssignedHours', 'AssignedTasks'], axis=1, inplace=True)
                    wm_df = week_tasks[['Hrs', 'WeekTasks']].join(wm_df, on='ScheduledWeek', how='outer')
                    wm_df = wm_df.set_index('ScheduledWeek')
                    wm_df = wm_df.fillna(0)
                    wm_df.rename(columns={'Hrs': 'AssignedHours', 'WeekTasks': 'AssignedTasks'}, inplace=True)
                    wm_df['AvailableHours'] = pd.eval("wm_df.AllowedHours - wm_df.AssignedHours")
                    wm_df['AvailableTasks'] = pd.eval("wm_df.AllowedTasks - wm_df.AssignedTasks")

                    overbooked = wm_df.loc[(wm_df['AvailableHours'] < 0) | (wm_df['AvailableTasks'] < 0)]

                else:
                    print("Uncaught Error")

        sched['WeekPriorityScore'].astype(float)
        sched['DeltaDays'] = pd.eval('sched.DeltaWeeks*7')

        return sched