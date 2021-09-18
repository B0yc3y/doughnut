import random
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.models.blocks import Block
from slack_sdk.web import SlackResponse

SLACK_USER = '@doughnut-bot'


def get_user_list(channel_id: str, session: WebClient, summary_only: bool) -> List[Dict[str, str]]:
    """
    Fetch basic details for all active, non-bot users in this channel
    :param channel_id: Slack channel unique ID
    :param session: a current Slack API session
    :param summary_only: return only a summary of user data instead of all details
    :return: A list with an {id, name, real_name, timezone} entry for each active, non-bot user in this channel
    """
    users: List[Dict[str, str]] = get_channel_users(
        channel_id=channel_id,
        session=session,
        active_users_only=True
    )

    if summary_only:
        # only get the summary fields needed for matching
        users = [{
            'id': user['id'],
            'name': user['name'],
            'real_name': user['real_name'],
            'tz': user['tz'],
            'tzOffset': user['tz_offset']
        } for user in users]

    if len(users) == 0:
        print(f"No Suitable users found in channel: {channel_id}")

    return users


def get_user_wrapper(user: str, session: WebClient) -> SlackResponse:
    return session.users_info(user=user, include_locale=True)


def get_channel_users(channel_id: str, session: WebClient, active_users_only: bool) -> List[Dict]:
    try:
        # Get all ids of users in the channel
        channel_users_response: SlackResponse = session.conversations_members(channel=channel_id)

        # Get user details for all users in the slack team
        team_users_response: SlackResponse = session.users_list()

    except SlackApiError as e:
        print(f"Error fetching data from Slack API: {e}")
        raise SlackApiError

    channel_user_ids: List[str] = channel_users_response['members']
    slack_team_users: List[Dict] = team_users_response['members']

    # todo add filtering here for match aversion/temporarily excluded users.
    # filter out inactive users.
    if active_users_only:
        slack_team_users = [user for user in slack_team_users if is_user_active(user)]

    # Return all the user details for users in the channel
    return [user for user in slack_team_users if user["id"] in channel_user_ids]


def is_user_active(user: Dict) -> bool:
    return (user is not None
            and not user['deleted']
            and not user['is_restricted']
            and not user['is_bot']
            and 'donut' not in user['name']
            and 'doughnut' not in user['name'])


def create_match_dms(matches: List[Dict], session: WebClient):
    with ThreadPoolExecutor() as executor:
        for match in matches:
            user1_id: str = match['user1']['id']
            user2_id: str = match['user2']['id']
            conversation_id_future = executor.submit(get_match_conversation_id, [user1_id, user2_id], session)
            executor.submit(create_match_dm, conversation_id_future.result(), user1_id, user2_id, session)
            match["conv_id"] = conversation_id_future.result()


def get_match_conversation_id(user_ids: List[str], session: WebClient) -> str:
    response: SlackResponse = session.conversations_open(users=user_ids, return_im=True)
    return response['channel']['id']


def direct_message_match(
        user1_name: str,
        user2_name: str,
        user_id_lookup: Dict[str, str],
        preview_message: str,
        messages: [str],
        session: WebClient
) -> SlackResponse:
    user1_id: str = user_id_lookup[user1_name]
    user2_id: str = user_id_lookup[user2_name]
    conv_id: str = get_match_conversation_id([user1_id, user2_id], session)

    return session.chat_postMessage(
        channel=conv_id,
        text=preview_message,
        blocks=Block.parse_all([
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            } for message in messages
        ]),
    )


def create_match_dm(conv_id: str, user1_id: str, user2_id: str, session: WebClient) -> str:
    ids: List[str] = [user1_id, user2_id]
    organiser_id: str = ids[random.randint(0, 1)]

    response: SlackResponse = session.chat_postMessage(
        channel=conv_id,
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

    if not response.status_code == 200:
        print(f"Unable to message {user1_id} & {user2_id}")

    return conv_id


def post_matches(session: WebClient, matches: List[Dict], my_channel_id: str) -> SlackResponse:
    """
    Creates a new DM for each pair of users to introduce them,
    and also posts a list of all pairings to the channel
    """
    create_match_dms(matches, session)
    preview_message: str = ":doughnut: Matches are in! :doughnut:"

    match_message: str = "The matches for this round:"
    for match in matches:
        user1_id: str = match['user1']['id']
        user2_id: str = match['user2']['id']
        match_message += f'\n<@{user1_id}> and <@{user2_id}>'

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
                "text": match_message
            }
        },
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

    # Send pairings to the ds_doughnut channel
    return session.chat_postMessage(
        channel=my_channel_id,
        text=preview_message,
        blocks=blocks
    )
