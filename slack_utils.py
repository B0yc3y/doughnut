import math
import random
from typing import Any
from datetime import datetime as dt
from os import path
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from typing import List, Dict
import sys

from pandas import DataFrame
from slack_sdk import WebClient

SLACK_USER = '@doughnut-bot'


def get_channel_dict(session):
    channel_dict = {}
    response = session.conversations_list(limit=5000, exclude_archived=True)
    channels = response['channels']
    for channel in channels:
        channel_dict[channel['name']] = channel['id']
    return channel_dict


def get_user_df(session, channel_id):
    user_info_list: Any = []

    response = session.conversations_members(channel=channel_id, limit=200)
    user_list = response['members']

    user_detail_responses = get_all_user_data(user_list, session)

    for resp in user_detail_responses:
        if (resp["user"] is not None
                and not resp["user"]["deleted"]
                and not resp["user"]["is_restricted"]
                and not resp["user"]["is_bot"]
        ):
            user_info_list += [resp['user']]

    if len(user_info_list) > 0:
        user_df = pd.DataFrame(user_info_list)[['id', 'name', 'real_name', 'tz']]
        user_df = user_df[(~user_df.name.str.contains('donut')) &
                          (~user_df.name.str.contains('doughnut'))].reset_index(drop=True)

        return user_df

    print(f"No Suitable users found in channel: {channel_id}")
    sys.exit(1)


def get_user_wrapper(user, session):
    return session.users_info(user=user, include_locale=True)


def get_all_user_data(users, session) -> List[Dict]:
    user_details = []
    with ThreadPoolExecutor() as executor:
        running_tasks = [executor.submit(get_user_wrapper, user, session) for user in users]
        for running_task in running_tasks:
            result = running_task.result()
            user_details.append(result)
    return user_details


def create_matches(user_df, history_df):
    # Match across timezones and with those they haven't matched with yet
    possible_cases_df = pd.DataFrame(columns=['name1', 'name2', 'times_paired', 'is_diff_tz'])
    user_list = user_df['name'].tolist()

    for i in range(len(user_list)):
        name1 = user_df['name'][i]
        for j in range(i + 1, len(user_list)):
            name2 = user_df['name'][j]

            if len(history_df) > 0:
                tmp_hist_df = history_df[((history_df['name1'] == name1) &
                                          (history_df['name2'] == name2)) |
                                         ((history_df['name2'] == name1) &
                                          (history_df['name1'] == name2))]
                times_paired = len(tmp_hist_df)
            else:
                times_paired = 0

            name1_mask = user_df['name'].values == name1
            name2_mask = user_df['name'].values == name2

            name1_tz = user_df[name1_mask]['tz'].values[0]
            name2_tz = user_df[name2_mask]['tz'].values[0]

            is_diff_tz = (name1_tz != name2_tz)

            possible_cases_df = possible_cases_df.append({'name1': name1,
                                                          'name2': name2,
                                                          'times_paired': times_paired,
                                                          'is_diff_tz': is_diff_tz}, ignore_index=True)

    possible_cases_df['match_strength'] = (possible_cases_df['is_diff_tz'] * 2) - possible_cases_df['times_paired']
    filter_cases_df = possible_cases_df.copy(deep=True)

    match_df = pd.DataFrame(columns=['name1', 'name2'])
    ind = 0
    for user in user_df['name'].tolist():
        top_user_match = filter_cases_df[(filter_cases_df['name1'] == user) |
                                         (filter_cases_df['name2'] == user)].sort_values('match_strength',
                                                                                         ascending=False).reset_index(
            drop=True)[['name1', 'name2']].head(1).reset_index(drop=True)
        if len(top_user_match.index) > 0:
            name1 = top_user_match.name1.values[0]
            name2 = top_user_match.name2.values[0]
            match_df.loc[ind] = [name1, name2]
            filter_cases_df = filter_cases_df[(filter_cases_df['name1'] != name1) &
                                              (filter_cases_df['name2'] != name1)]
            filter_cases_df = filter_cases_df[(filter_cases_df['name1'] != name2) &
                                              (filter_cases_df['name2'] != name2)]
            ind += 1

    # Find if anyone wasn't matched, make a second match with their top option
    for user in user_df['name'].tolist():
        tmp_match_df = match_df[(match_df['name1'] == user) |
                                (match_df['name2'] == user)]
        if len(tmp_match_df.index) == 0:
            print(f'User: {user} was not matched. Setting a second match up for them...')
            top_user_match = possible_cases_df[(possible_cases_df['name1'] == user) |
                                               (possible_cases_df['name2'] == user)].sort_values('match_strength',
                                                                                                 ascending=False).reset_index(
                drop=True)[['name1', 'name2']].head(1).reset_index(drop=True)

            name1 = top_user_match.name1.values[0]
            name2 = top_user_match.name2.values[0]
            match_df.loc[ind] = [name1, name2]

    today = dt.strftime(dt.now(), "%Y-%m-%d")
    match_df['match_date'] = today
    match_df['prompted'] = 0

    return match_df


def update_history(match_df, history_file, concat_df:bool = True):
    if concat_df:
        history_df: pd.DataFrame = pd.read_csv(history_file) if path.exists(history_file) else pd.DataFrame()
        match_df = pd.concat([history_df, match_df])

    match_df.to_csv(history_file, index=False)


def get_user_id_from_name(user_df: DataFrame, name: str) -> str:
    return user_df[user_df['name'] == name]['id'].values[0]


def create_match_dms(match_df, user_df, session):
    with ThreadPoolExecutor() as executor:
        for i in range(len(match_df)):
            user1: str = match_df[match_df.index == i].name1.values[0]
            user2: str = match_df[match_df.index == i].name2.values[0]
            user1_id: str = get_user_id_from_name(user_df, user1)
            user2_id: str = get_user_id_from_name(user_df, user2)
            conversation_id_future = executor.submit(get_match_conversation_id, [user1_id, user2_id], session)
            executor.submit(create_match_dm, conversation_id_future.result(), user1_id, user2_id, session)


def get_match_conversation_id(user_ids: List[str], session: WebClient) -> str:
    response = session.conversations_open(users=user_ids, return_im=True)
    return response['channel']['id']


def direct_message_match(user1_name: str, user2_name: str, user_df: DataFrame, message: str, session: WebClient,):
    user1_id: str = get_user_id_from_name(user_df, user1_name)
    user2_id: str = get_user_id_from_name(user_df, user2_name)
    conv_id: str = get_match_conversation_id([user1_id,user2_id], session)
    session.chat_postMessage(
        channel=conv_id,
        as_user=SLACK_USER,
        text=message
    )


def create_match_dm(conv_id: str, user1_id:str, user2_id:str, session):
    ids: List[str] = [user1_id, user2_id]

    organiser = ids[random.randint(0, 1)]

    session.chat_postMessage(channel=conv_id,
                             text=f'Hello <@{user1_id}> and <@{user2_id}>! Welcome to a new round of doughnuts! Please use this DM channel to set up time to connect!',
                             as_user=SLACK_USER)
    session.chat_postMessage(channel=conv_id,
                             text=f'<@{organiser}> you have been selected to organise the meeting',
                             as_user=SLACK_USER)


def post_matches(session, user_df, match_df, my_channel_id):
    create_match_dms(match_df, user_df, session)

    message: str = 'The new round of pairings are in! You should have received a DM from _doughnut with your new doughnut partner. Please post any feedback here. (If there are an odd number of participants someone will get two matches)'
    for i in range(0, len(match_df.index)):
        user1 = match_df[match_df.index == i].name1.values[0]
        user2 = match_df[match_df.index == i].name2.values[0]
        user1_id = user_df[user_df['name'] == user1]['id'].values[0]
        user2_id = user_df[user_df['name'] == user2]['id'].values[0]
        message += f'\n<@{user1_id}> and <@{user2_id}>'

    message += f"\nThats {len(match_df.index)} donuts this time around!"

    # Send pairings to the ds_donut channel
    response = session.chat_postMessage(channel=my_channel_id,
                                        text=message,
                                        as_user='@doughnut_bot')
