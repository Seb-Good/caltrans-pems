"""
handler.py
----------
This module provides a class and methods for connecting with and pulling data from the CalTrans-PeMS site.
By: Sebastian D. Goodfellow, Ph.D.
"""

# 3rd party imports
import os
import re
import sys
import json
import time
import locale
import logging
import mechanize
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from http.cookiejar import LWPCookieJar

# Local imports
from pems.settings import DATA_PATH, BASE_URL, DISTRICTS, CLEARING_HOUSE_URL


class PeMSHandler(object):

    def __init__(self, username, password, debug=False):

        # Set parameters
        self.username = username
        self.password = password
        self.debug = debug

        # Set attributes and connect to PeMS
        self.log = self._logger_setup()
        self.browser = self._browser_login()
        self.html_object = self._get_html_object()
        self.label_reference, self.form_data = self._parse_html()

    def get_file_types(self):
        """Collect all available """
        return list(self.form_data.keys())

    def get_districts(self, file_type):
        """Return the available districts for a file type."""
        # Get form_data district keys
        districts = list(self.form_data[file_type].keys())

        return DISTRICTS if districts[0] == 'all' else sorted(districts, key=int)

    def get_files(self, start_year, end_year, districts, file_types, months=None):
        """Return a list of available files for specific query."""
        # Storage for query responses
        responses_json = dict()
        responses_df = list()
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

        # Loop through file types
        for file_type in file_types:
            responses_json[file_type] = dict()
            for district in districts:
                responses_json[file_type][district] = dict()
                for year in [str(x) for x in range(start_year, end_year + 1)]:
                    responses_json[file_type][district][year] = dict()
                    self.browser.open(CLEARING_HOUSE_URL.format(BASE_URL, district, year, file_type))
                    response = json.loads(self.browser.response().read())
                    if len(response) > 0:
                        responses_json[file_type][district][year] = response['data']
                        if months is None:
                            months_to_get = list(responses_json[file_type][district][year].keys())
                        else:
                            months_to_get = [month for month in months if month in
                                             list(responses_json[file_type][district][year].keys())]
                        for month in months_to_get:
                            for file in responses_json[file_type][district][year][month]:
                                responses_df.append({'file_type': file_type,
                                                     'district': district,
                                                     'year': year,
                                                     'month': month,
                                                     'file_name': file['file_name'],
                                                     'file_id': file['file_id'],
                                                     'megabites': np.round(locale.atof(file['bytes']) / 10 ** 6, 1),
                                                     'url': file['url']})
                    else:
                        self.log.info('No data available for filetype: {}, year: {}, district: {}'.format(file_type,
                                                                                                          year,
                                                                                                          district))

        # Configure output
        files_json = responses_json
        files_df = pd.DataFrame(responses_df)

        return files_json, files_df

    def download_files(self, start_year, end_year, districts, file_types, months, save_path=None):
        """Download all text files for specific query."""
        # Get query files
        _, files_new = self.get_files(start_year=start_year, end_year=end_year, districts=districts,
                                      file_types=file_types, months=months)

        if files_new.shape[0] > 0:

            # Create save path
            save_path = self._get_save_path(save_path=save_path)

            # Check for existing downloads
            files, files_new = self._check_for_new_files(files_new=files_new, save_path=save_path)

            if files_new.shape[0] > 0:
                for index, row in files_new.iterrows():
                    success = self._download_file(file_name=row['file_name'], file_url=row['url'], save_path=save_path)
                    if success:
                        files.append(row, ignore_index=True, )
                    time.sleep(5)

                # Save lookup csv
                files.to_csv(os.path.join(save_path, 'saved_files.csv'), index=False)
                self.log.info('Downloads complete, {} files, {} megabites'.format(
                    files_new.shape[0], np.round(files_new['megabites'].sum(), 1)))
            else:
                self.log.info('No new data to download')
        else:
            self.log.info('No data available to download')

    @staticmethod
    def _check_for_new_files(files_new, save_path):
        """Check for files that have already been downloaded and remove them from download list."""
        # Check for existing downloads
        if os.path.exists(os.path.join(save_path, 'saved_files.csv')):
            files = pd.read_csv(os.path.join(save_path, 'saved_files.csv'))
            return files, files_new[~files_new.isin(files)].dropna()
        else:
            return pd.DataFrame(data=[], columns=files_new.columns), files_new

    @staticmethod
    def _get_save_path(save_path):
        """Create and set save path."""
        if save_path is None:
            os.makedirs(DATA_PATH, exist_ok=True)
            return DATA_PATH
        else:
            os.makedirs(save_path, exist_ok=True)
            return save_path

    def _download_file(self, file_name, file_url, save_path):
        """Download single text file."""
        try:
            self.log.info('Start download, {}'.format(file_name))
            self.browser.retrieve('{}{}'.format(BASE_URL, file_url), os.path.join(save_path, file_name))
            self.log.info('Download completed')
            return True
        except Exception:
            self.log.info('Error downloading {}}'.format(file_name))
            return False

    @staticmethod
    def _logger_setup():
        """Setup logger tool."""
        formatter = logging.Formatter('%(asctime)s [%(levelname)-8s] %(message)s')
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        log = logging.Logger('scraper')
        log.addHandler(handler)

        return log

    def _browser_login(self):
        """Log into CalTrans PeMS."""
        # Initialize browser
        browser = mechanize.Browser()

        # Set cookie jar
        browser.set_cookiejar(LWPCookieJar())

        # Set browser options
        browser.set_handle_equiv(True)
        browser.set_handle_referer(True)
        browser.set_handle_robots(False)
        browser.set_handle_redirect(mechanize.HTTPRedirectHandler)

        # Follows refresh 0 but doesn't hang on refresh > 0
        browser.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

        # Want debugging messages?
        browser.set_debug_http(self.debug)
        browser.set_debug_redirects(self.debug)
        browser.set_debug_responses(self.debug)

        self.log.info('Requesting initial page...')

        # User-Agent
        browser.addheaders = [
            ('User-agent',
             'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')
        ]
        browser.open(BASE_URL + "/?dnode=Clearinghouse")

        self.log.info('Initial page opened.')

        # Set authentication
        browser.select_form(nr=0)
        browser.form['username'] = self.username
        browser.form['password'] = self.password

        self.log.info('Logging in...')

        # Submit login
        browser.submit()

        self.log.info('Logged in.')

        return browser

    def _get_html_object(self):
        """Read main page HTML and return bs object."""
        return BeautifulSoup(self.browser.response().read())

    def _parse_html(self):
        """Convert BeautifulSoup html object to JSON."""
        # Extract script containing valid request parameter values
        script = self.html_object.find('script', text=re.compile('YAHOO\.bts\.Data'))
        j = re.search(r'^\s*YAHOO\.bts\.Data\s*=\s*({.*?})\s*$', script.string, flags=re.DOTALL | re.MULTILINE).group(1)

        # Convert to valid JSON
        j = re.sub(r"{\s*(\w)", r'{"\1', j)
        j = re.sub(r",\s*(\w)", r',"\1', j)
        j = re.sub(r"(\w):", r'\1":', j)

        # Convert JSON to dict
        data = json.loads(j)
        assert data['form_data']['reid_raw']['all'] == 'all'

        return data['labels'], data['form_data']
