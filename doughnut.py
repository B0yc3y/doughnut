import csv
import random
import sys
from concurrent.futures import ThreadPoolExecutor

from slack_sdk.web import SlackResponse

import slack_utils as su
import os
import boto3
from typing import List, Dict
from datetime import date
from datetime import datetime as dt
from os import path
from slack_sdk import WebClient
from botocore.exceptions import ClientError

HISTORY_DIR = "./doughnut_history/"
DAYS_BETWEEN_RUNS = 14
PROMPT_DAYS = DAYS_BETWEEN_RUNS / 2
CSV_FIELD_NAMES = ['name1', 'name2', 'match_date', 'prompted']

CHANNELS = os.environ.get("SLACK_CHANNELS", "donuts:C015239UFM2")
POST_MATCHES = os.environ.get("POST_MATCHES", False)
API_TOKEN = os.environ.get("SLACK_API_TOKEN", 'TOKEN HERE')
S3_BUCKET_NAME = os.environ.get("S3_BUCKET", None)

SESSION = WebClient(token=API_TOKEN)
S3_CLIENT = boto3.resource('s3')


def main():
    if not POST_MATCHES:
        print("--------------------------------------------")
        print("---    Publishing to slack is disabled   ---")
        print("--------------------------------------------")
        print("--- Set `POST_MATCHES` env var to enable ---")
        print("--------------------------------------------")

    # Pull all history from s3 if backed by s3
    if S3_BUCKET_NAME is not None:
        pull_history_from_s3(S3_BUCKET_NAME, HISTORY_DIR)
    else:
        print("No S3 bucket configured. Using local history")

    # for each channel, execute matches
    channels: List[str] = CHANNELS.split(",")
    for channel in channels:
        channel_name, channel_id = channel.split(":")
        channel_history_file: str = get_history_file_path(channel_id, channel_name, HISTORY_DIR)
        channel_history: List[dict] = parse_history_file(channel_history_file)
        last_run_date: date = get_last_run_date(channel_history)
        days_since_last_run: int = abs(date.today() - last_run_date).days

        print(f"Days since last run: {days_since_last_run}")

        # if it's been less than the minimum number of days needed to do more work exit.
        if days_since_last_run < PROMPT_DAYS:
            print(f"It has only been {days_since_last_run} days since last run.")
            print("Nothing to do. Goodbye!")
            sys.exit(1)

        print(f"Fetching users in channel: {channel_id}")
        channel_users = su.get_user_list(
            channel_id=channel_id,
            session=SESSION,
            summary_only=True
        )
        print(f"Successfully found: {len(channel_users)} users")

        # if it's been more than enough days, run more matches.
        if days_since_last_run >= DAYS_BETWEEN_RUNS:
            matches = execute_channel_matches(channel_id, channel_users, channel_history, POST_MATCHES, SESSION)
            print("Updating history with new matches.")
            channel_history += matches
            write_history(channel_history, channel_history_file)

        # if it's been more than match days/2, prompt people to check if they've made a time.
        else:
            user_id_lookup = {u['name']: u['id'] for u in channel_users}
            users_prompted = execute_channel_match_prompts(channel_id, user_id_lookup, channel_history, POST_MATCHES, SESSION)
            if users_prompted > 0:
                print("Updating history with new prompts.")
                write_history(channel_history, channel_history_file)

    # push updated history to s3 if backed by s3
    if S3_BUCKET_NAME is not None and POST_MATCHES:
        push_history_to_s3(S3_BUCKET_NAME, channels, HISTORY_DIR)

    print("Done!")
    print("Thanks for using doughnut! Goodbye!")


def get_last_run_date(channel_history: List[dict]) -> date:
    if len(channel_history) == 0:
        return date.min
    else:
        # Assumed sorted by date
        return date.fromisoformat(channel_history[-1]['match_date'])


def parse_history_file(history_file: str) -> List[dict]:
    """
    Parse a CSV match history file

    Example CSV:
    name1, name2, match_date, prompted
    alice, bob, 2021-08-31, 1
    bob, charlie, 2021-09-14, 0

    Example parsed output:
    [
        {"name1": "alice", "name2": "bob", "match_date": "2021-08-31", "prompted": "1",
        {"name1": "bob", "name2": "charlie", "match_date": "2021-09-14", "prompted": "0"
    ]

    :param history_file: filepath to read from
    :return: A list where each item is a single previously-held match
    """
    if path.exists(history_file):
        with open(history_file, 'r', newline='') as csv_file:
            return [{k: v for k, v in row.items()} for row in csv.DictReader(csv_file, skipinitialspace=True)]
    return []


def execute_channel_match_prompts(
    channel_id: str,
    user_id_lookup: Dict[str, str],
    match_history: List[dict],
    post_to_slack: bool,
    session: WebClient
) -> int:
    """
    Send a message to matched users checking up on them, and update history to show
    that this has happened.
    :return: count of users prompted
    """
    print(f"Checking for matches to prompt in channel: {channel_id}")
    matches_to_prompt: List[dict] = []
    for match in match_history:
        if match['prompted'] != '1':
            days_since_last_run: int = abs(date.today() - date.fromisoformat(match['match_date'])).days
            if days_since_last_run >= PROMPT_DAYS:
                match['prompted'] = '1'
                matches_to_prompt.append(match)

    if len(matches_to_prompt) > 0:
        print(f"Prompting {len(matches_to_prompt)} matches")
        if post_to_slack:
            prompt_match_list(user_id_lookup, matches_to_prompt, session)
    else:
        print("No matches require prompting.")

    return len(matches_to_prompt)


def prompt_match_list(user_id_lookup: Dict[str, str], matches_to_prompt: List[Dict[str, str]], session: WebClient):
    with ThreadPoolExecutor() as executor:
        for match in matches_to_prompt:
            executor.submit(send_prompt_message, user_id_lookup, match, session)


def send_prompt_message(user_id_lookup: Dict[str, str], match: Dict[str, str], session: WebClient):
    preview_message: str = ":doughnut: Half way! :doughnut:"
    message: str = "It's the halfway point, just checking in to ensure the session has been scheduled or "
    user1_name: str = match['name1']
    user2_name: str = match['name2']
    response: SlackResponse = su.direct_message_match(
        user1_name=user1_name,
        user2_name=user2_name,
        user_id_lookup=user_id_lookup,
        preview_message=preview_message,
        messages=[message],
        session=session
    )

    if not response.status_code == 200:
        print(f"Unable to post message dm with: {user1_name} & {user2_name}")


def execute_channel_matches(channel_id: str, channel_users: List[dict], history: List[dict], post_to_slack: bool, session: WebClient) -> List[dict]:
    """
    Gather user information, calculate best matches, and post those matches to Slack.
    :param channel_id: Slack channel
    :param channel_users: List of user information: names, ids
    :param history: History of previous matches for this channel
    :param post_to_slack: yes/no send messages in Slack channel/DMs
    :param session: Slack API session
    :return: a list of matches made this time
    """
    print("Generating optimal matches, this could take some time...")
    matches = create_matches(channel_users, history)
    print(f"The following matches have been found: {matches}")
    if post_to_slack:
        post_matches_to_slack(channel_id, matches, session)

    today = dt.strftime(dt.now(), "%Y-%m-%d")
    new_match_history = [{
        'name1': m['user1']['name'],
        'name2': m['user2']['name'],
        'match_date': today,
        'prompted': 0
    } for m in matches]
    return new_match_history


def create_matches(channel_users: List[dict], history: List[dict]) -> List[dict]:
    """
    Choose which users should be paired together this time
    :param channel_users: A list of active users in this channel
    :param history: A list of previously matched pairs (names and dates)
    :return: A list of pairings (same format as history)
    """

    """
    Build a record of previous pairings for each user
    eg
    {
      'Alice': {
        'Bob': ['2021-01-01', '2021-03-07'],
        'Charlie': ['2020-12-25']
    """
    match_counts: Dict[str, Dict[str, List[str]]] = dict()
    for match in history:
        person_a = match['name1']
        person_b = match['name2']

        record_match(person_a, person_b, match['match_date'], match_counts)
        record_match(person_b, person_a, match['match_date'], match_counts)

    """
    Build a list of all potential pairings with a score for each:
    {name1, name2, match_strength}
    """
    possible_matches = []
    for i in range(len(channel_users)):
        user1 = channel_users[i]
        user1['matched'] = False
        for j in range(i + 1, len(channel_users)):
            user2 = channel_users[j]

            match_strength = calculate_match_strength(user1, user2, match_counts)
            possible_matches.append({
                'user1': user1,
                'user2': user2,
                'match_strength': match_strength
            })

    """
    Iterate through potential matches from best to worst, marking users as paired off as we go
    """
    chosen_matches = []
    for potential in sorted(possible_matches, key=lambda v: v['match_strength'], reverse=True):
        if not (potential['user1']['matched'] or potential['user2']['matched']):
            chosen_matches.append(potential)
            potential['user1']['matched'] = True
            potential['user2']['matched'] = True

    # Find if anyone wasn't matched, make a second match with their top option
    # This should only happen if we have an odd number of users
    for user in channel_users:
        if not user['matched']:
            max_match: int
            max_match_partner: Dict
            for partner in channel_users:
                if partner['name'] != user['name']:
                    this_match_strength = calculate_match_strength(user, partner, match_counts)
                    if max_match is None or this_match_strength > max_match:
                        max_match = this_match_strength
                        max_match_partner = partner

            user['matched'] = True
            chosen_matches.append({
                'user1': user,
                'user2': max_match_partner,
                'match_strength': max_match
            })

    return chosen_matches


def record_match(host: str, guest: str, meet_date: str, matches: Dict[str, Dict[str, List[str]]]):
    """
    Records a given meeting in the history for the host.
    """
    if host not in matches:
        matches[host] = {}

    if guest not in matches[host]:
        matches[host][guest] = [meet_date]
    else:
        matches[host][guest].append(meet_date)


def calculate_match_strength(user1: Dict[str, str], user2: Dict[str, str], past_matches: Dict[str, Dict[str, List[str]]]) -> int:
    """
    Provides a weighting/metric for how "good" a potential pairing is.
    """
    name1 = user1['name']
    name2 = user2['name']
    if name1 not in past_matches or name2 not in past_matches[name1]:
        times_paired = 0
    else:
        times_paired = len(past_matches[name1][name2])

    is_diff_tz = (user1['tz'] != user2['tz'])

    # Users in different timezones prioritised, but won't match the same person again until you have met everyone else
    # some randomness added for the case when multiple potential matches share a match score so we don't get some
    # unintended default alphabetic order or alike.
    return 100*is_diff_tz - 200*times_paired + random.randint(0, 50)


def get_history_file_path(channel_id, channel_name, history_dir):
    channel_history_file = f"{channel_name}_{channel_id}_history.csv"
    if history_dir is not None:
        channel_history_file = f"{history_dir}{channel_history_file}"
    return channel_history_file


def write_history(history: List[dict], filepath: str):
    with open(filepath, 'w+', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELD_NAMES)
        writer.writeheader()
        writer.writerows(history)


def post_matches_to_slack(channel_id, matches, session):
    print(f"Posting matches to channel: {channel_id}.")
    print("Setting up DM channels for matched pairs.")
    su.post_matches(session, matches, channel_id)


def pull_history_from_s3(bucket_name: str, out_dir: str = "/tmp/"):
    bucket = S3_CLIENT.Bucket(bucket_name)
    print(f"Pulling history from s3://{bucket_name}")
    for s3_object in bucket.objects.all():
        _, filename = os.path.split(s3_object.key)
        print(f"Pulling history for channel {filename}")
        bucket.download_file(s3_object.key, f"{out_dir}{filename}")


def push_history_to_s3(bucket_name: str, channels: List[str], history_dir: str = "/tmp/"):
    for channel in channels:
        channel_name, channel_id = channel.split(":")
        local_file: str = get_history_file_path(channel_id, channel_name, history_dir)
        s3_file_name = local_file.split("/")[-1]
        file_uploaded: bool = upload_file(local_file, bucket_name, s3_file_name)
        if file_uploaded:
            print(f"Uploaded history for channel: {channel} to s3://{bucket_name}/{s3_file_name}")
        else:
            print(f"Unable to upload history for channel: {channel}")

    print(f"Finished updating history")


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True


if __name__ == '__main__':
    main()
