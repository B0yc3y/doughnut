import random
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.models.blocks import Block
from slack_sdk.web import SlackResponse

SLACK_USER = '@doughnut-bot'


def get_user_list(
        channel_id: str,
        session: WebClient,
        limit: int,
) -> List[Dict[str, str]]:
    """
    Fetch basic details for all active, non-bot users in this channel
    :param channel_id: Slack channel unique ID
    :param session: a current Slack API session
    :param limit: The limit of the number of users to pull out of the slack team
    :return: A list with an {id, name, real_name, timezone} entry for each active, non-bot user in this channel
    """
    users: List[Dict[str, str]] = get_channel_users(
        channel_id=channel_id,
        session=session,
        limit=limit
    )

    if len(users) > 0:
        # only get the summary fields needed for matching
        users = [{
            'id': user['id'],
            'name': user['name'],
            'real_name': user['real_name'],
            'tz': user['tz'],
            'tzOffset': user['tz_offset']
        } for user in users]

    return users


def get_channel_users(channel_id: str, session: WebClient, limit: int) -> List[Dict]:
    """
    Fetch all details for users in a given channel
    :param channel_id: the channel we are looking for users in
    :param session: the slack client session
    :param limit: the maximum number of users to pull
    :return: a list of user details as a dict
    """
    try:
        # Get all ids of users in the channel
        channel_users_response: SlackResponse = session.conversations_members(
            channel=channel_id,
            limit=limit
        )

        # Get user details for all users in the slack team
        team_users_response: SlackResponse = session.users_list()

    except SlackApiError as e:
        print(f"Error fetching data from Slack API: {e}")
        raise SlackApiError

    channel_user_ids: List[str] = channel_users_response['members']
    slack_team_users: List[Dict] = team_users_response['members']

    # todo add filtering here for match aversion/temporarily excluded users.
    slack_team_users = [user for user in slack_team_users if is_active_user(user)]

    # Return all the user details for users in the channel
    return [user for user in slack_team_users if user["id"] in set(channel_user_ids)]


def is_active_user(user: Dict) -> bool:
    """
    Returns true if the user is active and not the bot
    :param user: the user to check
    :return: boolean if user is active
    """
    return (user is not None
            and not user['deleted']
            and not user['is_restricted']
            and not user['is_bot']
            and 'donut' not in user['name']
            and 'doughnut' not in user['name'])


def create_match_dms(matches: List[Dict], session: WebClient) -> List[Dict]:
    """
    Create many dms, one for each match, this is done using multithreading
    :param matches: the list of matches to message
    :param session: The slack client session.
    :return:
    """
    with ThreadPoolExecutor() as executor:
        for match in matches:
            user1_id: str = match['user1']['id']
            user2_id: str = match['user2']['id']
            conversation_id_future = executor.submit(get_match_conversation_id, [user1_id, user2_id], session)
            executor.submit(match_opening_message, conversation_id_future.result(), user1_id, user2_id, session)
            match["conversation_id"] = conversation_id_future.result()

    return matches


def get_match_conversation_id(user_ids: List[str], session: WebClient) -> str:
    """
    Get the slack conversation id for this match's DM
    :param user_ids: the users int he conversation
    :param session: the slack client session
    :return: the string id of the conversation
    """
    response: SlackResponse = session.conversations_open(users=user_ids, return_im=True)
    return response['channel']['id']


def direct_message_match(
        conversation_id: str,
        preview_message: str,
        messages: [str],
        session: WebClient
) -> SlackResponse:
    """
    Send a message to the provided conversation
    :param conversation_id: the id of the message to send
    :param preview_message: the notification display message
    :param messages: the content of the message(s) to send
    :param session: the slack client session
    :return:
    """
    try:
        return session.chat_postMessage(
            channel=conversation_id,
            text=preview_message,
            blocks=Block.parse_all([
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message
                    }
                } for message in messages
            ])
        )
    except SlackApiError as e:
        print(f"Error posting message to Slack API: {e}")
        print(f"Unable to message conversation: {conversation_id}")
        raise SlackApiError


def match_opening_message(conversation_id: str, user1_id: str, user2_id: str, session: WebClient) -> SlackResponse:
    """
    Send the opening message to the match
    :param conversation_id: the conversation_id of the match
    :param user1_id: the userId of user 1 used to tag the user
    :param user2_id: the userId of user 2 used to tag the user
    :param session: the slack client session
    :return: the response from slack
    """
    ids: List[str] = [user1_id, user2_id]
    organiser_id: str = ids[random.randint(0, 1)]

    try:
        # Get all ids of users in the channel
        return session.chat_postMessage(
            channel=conversation_id,
            text=f':doughnut: doughnut Time! :doughnut: ',
            blocks=Block.parse_all(
                [{
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f'Hello <@{user1_id}> and <@{user2_id}>!'
                                f' Welcome to a new round of doughnuts!'
                                f' Please use this DM channel to set up time to connect!'
                                f'<@{organiser_id}> you have been selected to organise the meeting.'
                    }
                }]
            ),
        )

    except SlackApiError as e:
        print(f"Error posting message to Slack API: {e}")
        print(f"Unable to message {user1_id} & {user2_id}")
        raise SlackApiError


def post_matches(session: WebClient, matches: List[Dict], channel_id: str) -> SlackResponse:
    """
    Posts a list of all pairings to the channel
    :param session: the slack client session
    :param matches: the list of matches
    :param channel_id: the channel to post the matches to
    :return: the slack api response
    """
    preview_message: str = ":doughnut: Matches are in! :doughnut:"

    match_messages = []
    msg: str = "The matches for this round:"
    for match in matches:
        user1_id: str = match['user1']['id']
        user2_id: str = match['user2']['id']
        msg += f'\n<@{user1_id}> and <@{user2_id}>'
        if len(msg) > 2500:
            match_messages.append(msg)
            msg = ""
    
    # Don't forget to capture the last one!
    # ...or the first one if it's less than 2500 chars
    match_messages.append(msg)

    blocks: List[Block] = Block.parse_all([
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": 'The new round of pairings are in!\n'
                        'You should have received a DM from _doughnut with your new doughnut partner.\n'
                        'If there are an odd number of participants, someone will get two matches.'
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": 'Please post any issues to https://github.com/B0yc3y/doughnut/issues'
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message
            }
        } for message in match_messages,
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"\nThat's {len(matches)} doughnut(s) this time around!"
            }
        }
    ])

    try:
        # Send pairings to the ds_doughnut channel
        return session.chat_postMessage(
            channel=channel_id,
            text=preview_message,
            blocks=blocks
        )

    except SlackApiError as e:
        print(f"Error posting channel message to Slack API: {e}")
        raise SlackApiError
