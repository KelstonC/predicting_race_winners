import os
import requests
import json
from datetime import datetime
import time
import logging


BASE_DIR = "/Users/kelstonchen/GitRepos/predicting_race_winners"
BASE_URL = "https://api.jolpi.ca/ergast/f1"
SEASONS = [2022, 2023, 2024, 2025]

logging.basicConfig(
    level=logging.INFO,
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M"
    )

class JolpicaFetcher:
    def __init__(
            self, 
            base_url: str, 
            params: dict, 
            seasons: list[int], 
            endpoint: str,
            filepath: str
            ):
        self.base_url = base_url
        self.params = params
        self.seasons = seasons
        self.endpoint = endpoint
        self.filepath = filepath
    
    def _save(
            self, 
            data: json,
            season_id: str,
            file_id: str
            ) -> None:
        """Save JSON file"""
        path = os.path.join(self.filepath, self.endpoint, season_id)
        time = datetime.now().strftime('%Y%m%d%H%M%S')
        output = os.path.join(path, (file_id + '_' + time + '.json'))

        if not os.path.exists(path):
            logging.info(f"Directory created: {path}")
            os.makedirs(path, exist_ok=True)
        
        logging.info(f"Saved to: {output}")
        with open(output, 'w') as w:
            json.dump(data, w, indent=4)

    def _retries(self, url: str, params: dict):
        for i in range(3):
            r = requests.get(url, params=params)
            if r.status_code == 429:
                logging.warning('Too fast, sleep and try again')
                logging.info(f'Retry: {i + 1}')
                time.sleep(3)
                continue
            elif r.status_code == 200:
                logging.info(f'Successful pull after {i + 1} tries')
                return r
        raise Exception(f"Failed to fetch data after 3 retries. Status code: {r.status_code}")

    def fetch(self) -> None:
        """Grab data from Jolpica F1 API"""
        updated_params = self.params.copy()

        for season in self.seasons:
            updated_params['offset'] = 0
            logging.info(f'Fetching for season: {season}')

            while True:
                try:
                    url = os.path.join(
                        self.base_url, 
                        f'{season}',
                        self.endpoint
                        )
                    r = requests.get(url, params=updated_params)

                    if r.status_code == 429:
                        r = self._retries(url, params=updated_params)

                    data = r.json()['MRData']
                    self._save(
                        data,
                        season_id=str(season),
                        file_id=self.endpoint
                        )
                    
                    updated_params['offset'] += updated_params['limit']

                    # if int(updated_params['offset']) >= int(data['total']):
                    #     print('Nothing left to fetch')
                    #     break
                    if int(updated_params['offset']) >= 30:
                        break
                except requests.exceptions.RequestException as e:
                    raise e


# r = requests.get(os.path.join(BASE_URL, '2022', 'results'), params={'limit': 30, 'offset': 0})
# data = r.json()['MRData']

# with open(os.path.join(BASE_DIR, 'data', 'raw', 'test__.json'), 'w') as w:
#     json.dump(data, w, indent=4)

def main():
    fetcher = JolpicaFetcher(
        BASE_URL, 
        params={'limit': 30, 'offset': 0},
        seasons=SEASONS,
        endpoint='results',
        filepath=os.path.join(BASE_DIR, 'data', 'raw')
        )
    fetcher.fetch()

if __name__ == "__main__":
    main()
