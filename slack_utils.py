import random
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict

from slack_sdk import WebClient

SLACK_USER = '@doughnut-bot'


def get_channel_dict(session):
    channel_dict = {}
    response = session.conversations_list(limit=5000, exclude_archived=True)
    channels = response['channels']
    for channel in channels:
        channel_dict[channel['name']] = channel['id']
    return channel_dict


def get_user_df(session, channel_id) -> List[dict]:
    """
    Fetch basic details for all active, non-bot users in this channel
    :param session: a current Slack API session
    :param channel_id: Slack channel unique ID
    :return: A list with an {id, name, real_name, timezone} entry for each active, non-bot user in this channel
    """
    user_info_list = []

    response = session.conversations_members(channel=channel_id, limit=200)
    user_list = response['members']

    user_detail_responses = get_all_user_data(user_list, session)

    for resp in user_detail_responses:
        user = resp['user']
        if (user is not None
                and not user['deleted']
                and not user['is_restricted']
                and not user['is_bot']
                and 'donut' not in user['name']
                and 'doughnut' not in user['name']):

            user_info_list.append({
                'id': user['id'],
                'name': user['name'],
                'real_name': user['real_name'],
                'tz': user['tz']
            })

    if len(user_info_list) == 0:
        print(f"No Suitable users found in channel: {channel_id}")

    return user_info_list


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


def create_match_dms(matches: List[dict], session: WebClient):
    with ThreadPoolExecutor() as executor:
        for match in matches:
            user1_id = match['user1']['id']
            user2_id = match['user2']['id']
            conversation_id_future = executor.submit(get_match_conversation_id, [user1_id, user2_id], session)
            executor.submit(create_match_dm, conversation_id_future.result(), user1_id, user2_id, session)


def get_match_conversation_id(user_ids: List[str], session: WebClient) -> str:
    response = session.conversations_open(users=user_ids, return_im=True)
    return response['channel']['id']


def direct_message_match(user1_name: str, user2_name: str, user_id_lookup: Dict[str, str], message: str, session: WebClient):
    user1_id = user_id_lookup[user1_name]
    user2_id = user_id_lookup[user2_name]
    conv_id = get_match_conversation_id([user1_id, user2_id], session)
    session.chat_postMessage(
        channel=conv_id,
        as_user=SLACK_USER,
        text=message
    )


def create_match_dm(conv_id: str, user1_id: str, user2_id: str, session: WebClient):
    ids: List[str] = [user1_id, user2_id]

    organiser = ids[random.randint(0, 1)]

    session.chat_postMessage(channel=conv_id,
                             text=f'Hello <@{user1_id}> and <@{user2_id}>! Welcome to a new round of doughnuts! '
                                  f'Please use this DM channel to set up time to connect!',
                             as_user=SLACK_USER)
    session.chat_postMessage(channel=conv_id,
                             text=f'<@{organiser}> you have been selected to organise the meeting.',
                             as_user=SLACK_USER)


def post_matches(session: WebClient, matches: List[dict], my_channel_id: str):
    """
    Creates a new DM for each pair of users to introduce them,
    and also posts a list of all pairings to the channel
    """
    create_match_dms(matches, session)

    message: str = 'The new round of pairings are in! You should have received a DM from _doughnut with your new ' \
                   'doughnut partner. If there are an odd number of participants, ' \
                   'someone will get two matches.\n' \
                   'Please post any issues to https://github.com/B0yc3y/doughnut/issues'
    for match in matches:
        user1_id = match['user1']['id']
        user2_id = match['user2']['id']
        message += f'\n<@{user1_id}> and <@{user2_id}>'

    message += f"\nThat's {len(matches)} donuts this time around!"

    # Send pairings to the ds_donut channel
    session.chat_postMessage(channel=my_channel_id, text=message, as_user='@doughnut_bot')
