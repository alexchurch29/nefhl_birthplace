import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from bs4 import BeautifulSoup
import time
import pandas as pd


states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

provinces = ['Alta.', 'B.C.', 'Man.','N.B.', 'N.L.', 'N.S.', 'N.W.T.', 'Nun.', 'Ont.', 'P.E.I.', 'Que.', 'Sask.', 'Yuk.']


def get_url(url):
    """
    Get the url
    :param url: given url
    :return: raw html
    """
    response = requests.Session()
    retries = Retry(total=10, backoff_factor=.1)
    response.mount('http://', HTTPAdapter(max_retries=retries))

    try:
        response = response.get(url, timeout=5)
        response.raise_for_status()
    except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
        return None

    return response


def get_html(player_id):
    """
    Given a player id returns the forecaster page html
    E.g.: http://sportsforecaster.com/nhl/player/6746
    :param player_id: player id
    :return: raw html for forecaster page
    """
    player_id = str(player_id)
    url = 'http://sportsforecaster.com/nhl/player/{}'.format(player_id)

    return get_url(url)


def scrape_forecaster(player_id):
    """
    For a given player, scrapes the player's birthplace
    :param player_id: player id
    :return: array of players, along with their birthplaces
    """

    try:
        html = get_html(player_id)
        time.sleep(1)
    except Exception as e:
        print('hmtl for player {} is not there'.format(player_id), e)
        raise Exception

    soup = BeautifulSoup(html.content, 'html.parser')
    bio = list()
    for item in soup.find_all(attrs={'class': 'fss_profile_vitals'}):
        bio.append(item.get_text().replace('\n', '').split('\t'))

    try:
        birthplace = [i for i in bio[0] if "in " in i][0][3:].split(',')[1].strip()
        if birthplace in states:
            birthplace = 'USA'
        if birthplace in provinces:
            birthplace = 'CAN'

        return birthplace

    except:
        print('error for player {}'.format(player_id))
    return


def main():
    birthplaces = list()
    df = pd.read_csv('birthplaces.csv')
    for i in df.url:
        player_id = i.split('/')[-1]
        birthplaces.append(scrape_forecaster(player_id))

    df['birthplace'] = birthplaces
    df.to_csv('birthplaces.csv', index=False)

    return


if __name__ == '__main__':
    main()
