# doughnut_bot
Bare bones, custom version of [Donut Slack App](https://beemit.slack.com/apps/A11MJ51SR-donut?tab=more_info)

This app gets all active, not bot users in a Slack channel and randomly pairs them up for a social catch up, taking into account previous matches and timezones.
The app will create a DM with the matched users, and post the matches to the channel it's pulling from.

It uses the most recent matches to gauge when it last created matches, and if 7 days or more it will prompt the matches to catch up, if 14 days or more has passed it will make new matches with everyone included in the target channel.

### Installation
```shell
virtualenv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

### Environment Setup
Some of these env vars are defaulted, the token and bucket names are the only required env vars.
Post matches is required to be true to execute fully.
```shell
export SLACK_API_TOKEN="SlackTokenHere" # get a slack integration token from your slack admin
export DOUGHNUT_S3_BUCKET="bc-mel-doughnut-store" # replace as required

#defaulted env vars
#enable POST_MATCHES to actually post results to slack, and to write history to s3
export POST_MATCHES=false

#enable override this to run against other/more slack channels 
export SLACK_CHANNELS="channelName:channelId,channelName2:channelId2" 
```

### Run me
```shell
python3 doughnut.py
```
