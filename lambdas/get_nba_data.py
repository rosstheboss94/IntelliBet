import logging
import boto3
from botocore.exceptions import ClientError
import os
import requests
import json


def upload_teams():
    file_contents = ''

    if os.path.exists('./teams.json'):
        os.remove('./teams.json')

    res = requests.get('https://www.balldontlie.io/api/v1/teams')

    teams_dict = res.json()

    teams_arr = [json.dumps(team) for team in teams_dict['data']]

    for team in teams_arr:
        file_contents = file_contents + team + '\n'

    with open('teams.json', 'w') as file:
        file.write(file_contents)

    upload_file(file.name, 'intellibet', 'bronze/teams/teams.json')

    if os.path.exists('./teams.json'):
        os.remove('./teams.json')


def upload_players():
    page = 1
    file_contents = ''

    params = {
        'page': page,
        'per_page': 100
    }

    res = requests.get(
        'https://www.balldontlie.io/api/v1/players', params=params)

    players_response = res.json()
    total_pages = players_response['meta']['total_pages']
    players = [json.dumps(player) for player in players_response['data']]

    for player in players:
        file_contents = file_contents + player + '\n'

    if os.path.exists(f'./player_file{page}.json'):
        os.remove(f'./player_file{page}.json')

    with open(f'player_file{page}.json', 'w') as file:
        file.write(file_contents)

    upload_file(file.name, 'intellibet',
                f'bronze/players/player_file{page}.json')

    if os.path.exists(f'./player_file{page}.json'):
        os.remove(f'./player_file{page}.json')

    page += 1
    while page <= total_pages:
        file_contents = ''
        params = {
            'page': page,
            'per_page': 100
        }

        res = requests.get(
            'https://www.balldontlie.io/api/v1/players', params=params)

        players_response = res.json()
        total_pages = players_response['meta']['total_pages']
        players = [json.dumps(player) for player in players_response['data']]

        for player in players:
            file_contents = file_contents + player + '\n'

        if os.path.exists(f'./player_file{page}.json'):
            os.remove(f'./player_file{page}.json')

        with open(f'player_file{page}.json', 'w') as file:
            file.write(file_contents)

        upload_file(file.name, 'intellibet',
                    f'bronze/players/player_file{page}.json')

        if os.path.exists(f'./player_file{page}.json'):
            os.remove(f'./player_file{page}.json')

        page += 1


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


upload_players()
