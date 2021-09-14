import sys
from concurrent.futures import ThreadPoolExecutor

import slack_utils as su
import os
import boto3
import pandas as pd
from typing import List
from datetime import date
from pandas import DataFrame
from os import path
from slack_sdk import WebClient
from botocore.exceptions import ClientError

HISTORY_DIR = "./doughnut_history/"
DAYS_BETWEEN_RUNS = 14
PROMPT_DAYS = DAYS_BETWEEN_RUNS / 2

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
        channel_history_df: DataFrame = get_history_df(channel_history_file)
        last_run_date: date = get_last_run_date(channel_history_df)
        days_since_last_run: int = abs(date.today() - last_run_date).days

        print(f"Days since last run: {days_since_last_run}")
        # if it's been more than enough days, run more matches.
        if days_since_last_run >= DAYS_BETWEEN_RUNS:
            matches: DataFrame = execute_channel_matches(channel_id, channel_history_df, POST_MATCHES, SESSION)
            print("Updating history with new matches.")
            su.update_history(matches, channel_history_file)

        # if it's been more than match days/2, prompt people to check if they've made a time.
        elif days_since_last_run >= PROMPT_DAYS:
            matches: DataFrame = execute_channel_match_prompts(channel_id, channel_history_df, POST_MATCHES, SESSION)
            print("Updating history with new prompts")
            su.update_history(matches, channel_history_file, False)

        # if it's been less than the minimum number of days needed to do more work exit.
        else:
            print(f"It has only been {days_since_last_run} days since last run.")
            print("Nothing to do. Goodbye!")
            sys.exit(1)

    # push updated history to s3 if backed by s3
    if S3_BUCKET_NAME is not None and POST_MATCHES:
        push_history_to_s3(S3_BUCKET_NAME, channels, HISTORY_DIR)

    print("Done!")
    print("Thanks for using doughnut! Goodbye!")


def get_last_run_date(channel_history_df: DataFrame) -> date:
    if len(channel_history_df) == 0:
        return date.min
    else:
        return date.fromisoformat(channel_history_df.tail(1)['match_date'].values[0])


def get_history_df(history_file: str) -> DataFrame:
    if path.exists(history_file):
        return pd.read_csv(history_file)
    return pd.DataFrame()


def execute_channel_match_prompts(
    channel_id: str,
    match_history_df: DataFrame,
    post_to_slack: bool,
    session: WebClient
) -> DataFrame:
    print(f"Checking for matches to prompt in channel: {channel_id}")
    matches_to_prompt: List[List[str]] = []
    for index, row in match_history_df.iterrows():
        days_since_last_run: int = abs(date.today() - date.fromisoformat(row['match_date'])).days
        if not row['prompted'] and days_since_last_run >= PROMPT_DAYS:
            match_history_df.at[index, 'prompted'] = 1
            sorted_match: List[str] = sorted([row['name1'], row['name2']])
            if sorted_match not in matches_to_prompt:
                matches_to_prompt.append(sorted_match)
    if len(matches_to_prompt) > 0:
        print(f"Prompting {len(matches_to_prompt)} matches")
        prompt_match_list(channel_id, matches_to_prompt, post_to_slack, session)

    return match_history_df


def prompt_match_list(channel_id: str, matches_to_prompt: List[List[str]], post_to_slack: bool, session: WebClient):
    print(f"Fetching users in channel: {channel_id}")
    channel_users = su.get_user_df(session, channel_id)
    print(f"Successfully found: {len(channel_users)} users in channel: {channel_id}")
    if post_to_slack:
        with ThreadPoolExecutor() as executor:
            for match in matches_to_prompt:
                executor.submit(send_prompt_message, channel_users, match, session)


def send_prompt_message(channel_users: DataFrame, match: List[str], session):
    message = "It's the halfway point, checking in to ensure the session has been scheduled or completed"
    user1_name: str = match[0]
    user2_name: str = match[1]
    su.direct_message_match(
        user1_name=user1_name,
        user2_name=user2_name,
        user_df=channel_users,
        message=message,
        session=session
    )


def execute_channel_matches(channel_id, history_df, post_to_slack, session: WebClient) -> DataFrame:
    print(f"Fetching users in channel: {channel_id}")
    channel_users = su.get_user_df(session, channel_id)
    print(f"Successfully found: {len(channel_users)} users")
    print("Generating optimal matches, `this could take some time...")
    match_df = su.create_matches(channel_users, history_df)
    print(f"The following matches have been found: {match_df}")
    if post_to_slack:
        post_matches_to_slack(channel_id, channel_users, match_df, session)
    return match_df


def get_history_file_path(channel_id, channel_name, history_dir):
    channel_history_file = f"{channel_name}_{channel_id}_history.csv"
    if history_dir is not None:
        channel_history_file = f"{history_dir}{channel_history_file}"
    return channel_history_file


def post_matches_to_slack(channel_id, channel_users, match_df, session):
    print(f"Posting matches to channel: {channel_id}.")
    print("Setting up DM channels for matched pairs.")
    su.post_matches(session, channel_users, match_df, channel_id)


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
