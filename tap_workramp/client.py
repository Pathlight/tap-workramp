import requests
import requests.exceptions
import singer
import time

LOGGER = singer.get_logger()


class WorkrampAPI:
    BASE_URL = 'https://app.workramp.com/api/v1'
    MAX_RETRIES = 10
    WAIT_TO_RETRY = 60  # documentation doesn't include errors

    def __init__(self, config):
        self.access_token = config['access_token']
        self.headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

    def get(self, url):
        if not url.startswith('https://'):
            url = f'{self.BASE_URL}/{url}'

        for num_retries in range(self.MAX_RETRIES):
            LOGGER.info(f'workramp get request {url}')
            resp = requests.get(url, headers=self.headers)
            try:
                resp.raise_for_status()
            except requests.exceptions.RequestException:
                if resp.status_code == 429 and num_retries < self.MAX_RETRIES:
                    LOGGER.info('api query workramp rate limit')
                    time.sleep(self.WAIT_TO_RETRY)
                elif resp.status_code >= 500 and num_retries < self.MAX_RETRIES:
                    LOGGER.info('api query workramp 5xx error', extra={
                        'subdomain': self.subdomain
                    })
                    time.sleep(10)
                else:
                    raise Exception(f'workramp query error: {resp.status_code}')

            if resp and resp.status_code == 200:
                break

        return resp.json()
