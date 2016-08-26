#!/usr/bin/env python
"""
sumologic_export.py
~~~~~~~~~~~~~~~~

Export your Sumologic logs easily and quickly.

Usage:
    sumologic_export.py configure
    sumologic_export.py
    sumologic_export.py <start> <stop>
    sumologic_export.py
        (<start> | -s <start> | --start <start>)
        [(<stop> | -t <stop> | --stop <stop>)]
    sumologic_export.py (-h | --help)
    sumologic_export.py (-v | --version)

Written by Randall Degges (http://www.rdegges.com)
"""


from datetime import datetime, timedelta
from json import dumps, loads
from os import chmod, mkdir
from os.path import exists, expanduser
from subprocess import call
from time import sleep
import gzip
import cookielib

from docopt import docopt
import requests


##### GLOBALS
VERSION = '0.0.2'
CONFIG_FILE = expanduser('~/.sumo')


# Pretty print datetime objects.
prettify = lambda x: x.strftime('%Y-%m-%d')

def read_creds():
    with open(CONFIG_FILE, 'rb') as cfg:
        creds = loads(cfg.read())
        return (creds['email'], creds['password'])

class SumologicClient(object):

    # Sumologic API constants.
    SUMOLOGIC_URL = 'https://api.sumologic.com/api/v1/search/jobs'
    SUMOLOGIC_HEADERS = {
        'content-type': 'application/json',
        'accept': 'application/json',
    }

    # Amount of time to wait for API response.
    TIMEOUT = 10

    # Sumologic timezone to specify.
    TIMEZONE = 'PST'

    # The amount of logs to download from Sumologic per page.  The higher this
    # is, the more memory is used, but the faster the exports are.
    MESSAGES_PER_PAGE = 10000

    def __init__(self, email=None, password=None):
        if email is None or password is None:
            email, password = read_creds()
        self.credentials = email, password
        self.session = requests.Session()
        cookies = cookielib.LWPCookieJar('.sumocookie')
        cookies.load(ignore_discard=True)
        self.session.cookies = cookies

    def create_job(self, start, stop):
        """
        Request all Sumologic logs for the specified date range.

        :param datetime start: The date to start.
        :param datetime stop: The date to stop.

        :rtype: string
        :returns: The URL of the job.
        """
        while True:
            try:
                resp = self.post(
                    self.SUMOLOGIC_URL,
                    data = {
                        'query': '_env=prod _app=r101-postoffice _process=web (("at=dlr" and ("relay=twilio" or "relay=bandwidth")) or ("v1/openmarket" and not "openmarket/sms")) | parse "relay=* " as relay nodrop | parse "to=* " as device nodrop | parse "mid=* " as mid nodrop | parse "did=*" as did nodrop | parse "uid=* " as device nodrop | parse "status=* " as status nodrop | parse "/v1/*/*/*/*/" as relay, did, mid, device nodrop',
                        'from': start.replace(second=0, microsecond=0).isoformat(),
                        'to': stop.replace(second=0, microsecond=0).isoformat(),
                        'timeZone': self.TIMEZONE,
                    },
                )
                if resp.status_code == 202:
                    print "Created job: %s" % (resp.json())
                    return '%s/%s' % (self.SUMOLOGIC_URL, resp.json()['id'])

                raise Exception("Unexpected response: %s" % (resp.text))
            except Exception as ex:
                print ex
                sleep(1)

    def get_count(self, job_url):
        """
        Given a Sumologic job URL, figure out how many logs exist.

        :param str job_url: The job URL.

        :rtype: int
        :returns: The amount of logs found in the specified job results.
        """
        while True:
            try:
                resp = self.get(job_url)
                if resp.status_code == 200:
                    json = resp.json()
                    if json['state'] == 'DONE GATHERING RESULTS':
                        return json['messageCount']
                raise Exception("Unexpected response: %s" % resp.text)
            except Exception as ex:
                print ex
                sleep(1)

    def get_logs(self, job_url, count):
        """
        Iterate through all Sumologic logs for the given job.

        :param str job_url: The job URL.
        :param int count: The number of logs to retrieve.

        :rtype: generator
        :returns: A generator which returns a single JSON log until all logs have
            been retrieved.
        """
        for page in xrange(0, (count / self.MESSAGES_PER_PAGE) + 1):
            while True:
                try:
                    resp = self.get(job_url + '/messages',
                        params = {
                            'limit': self.MESSAGES_PER_PAGE,
                            'offset': self.MESSAGES_PER_PAGE * page,
                        }
                    )

                    if resp.status_code == 200:
                        json = resp.json()
                        for log in json['messages']:
                            yield log['map']

                        break
                    raise Exception("Unexpected response: %s" % (resp.text))
                except Exception as ex:
                    print ex
                    sleep(1)

    def get(self, url, params=None):
        return self._resp(self.session.get(
            url,
            auth=self.credentials,
            headers=self.SUMOLOGIC_HEADERS,
            timeout=self.TIMEOUT,
            params=params,
        ))

    def post(self, url, data):
        return self._resp(self.session.post(
            self.SUMOLOGIC_URL,
            auth=self.credentials,
            headers=self.SUMOLOGIC_HEADERS,
            timeout=self.TIMEOUT,
            data=dumps(data),
        ), save_cookie=True)

    def _resp(self, resp, save_cookie=False):
        if save_cookie:
            self.session.cookies.save(ignore_discard=True)
        if resp.status_code >= 300:
            raise Exception("Got status %s, body: %s" % (resp.status_code, resp.text))

        return resp


class Exporter(object):
    """Abstraction for exporting Sumologic logs."""

    # Default time increment to move forward by.
    INCREMENT = timedelta(days=1)

    # Default timerange to use if no dates are specified.
    DEFAULT_TIMERANGE = timedelta(days=30)


    # Amount of time to pause before requesting Sumologic logs.  60 seconds
    # seems to be a good amount of time.
    SLEEP_SECONDS = 60

    def __init__(self):
        """
        Initialize this exporter.

        This includes:

        - Loading credentials.
        - Prepping the environment.
        - Setting up class variables.
        """
        if not exists(CONFIG_FILE):
            print 'No credentials found! Run sumologic_export.py configure'
            raise SystemExit()

        if not exists('exports'):
            mkdir('exports')

        self.client = SumologicClient()

    def init_dates(self, start, stop):
        """
        Validate and initialize the date inputs we get from the user.

        We'll:

        - Ensure the dates are valid.
        - Perform cleanup.
        - If no dates are specified, we'll set defaults.
        """
        if start:
            try:
                self.start = datetime.strptime(start, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)
            except:
                print 'Invalid date format. Format must be YYYY-MM-DD.'
                raise SystemExit(1)

            if self.start > datetime.now():
                print 'Start date must be in the past!'
                raise SystemExit(1)
        else:
            self.start = (datetime.now() - self.DEFAULT_TIMERANGE).replace(hour=0, minute=0, second=0, microsecond=0)

        if stop:
            try:
                self.stop = datetime.strptime(stop, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)
            except:
                print 'Invalid date format. Format must be YYYY-MM-DD.'
                raise SystemExit(1)

            if self.stop > datetime.now():
                print 'Stop date must be in the past!'
                raise SystemExit(1)
        else:
            self.stop = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    def export(self, start, stop):
        """
        Export all Sumologic logs from start to stop.

        All logs will be downloaded one day at a time, and put into a local
        folder named 'exports'.

        :param str start: The datetime at which to start downloading logs.
        :param str stop: The datetime at which to stop downloading logs.
        """
        # Validate / cleanup the date inputs.
        self.init_dates(start, stop)

        print 'Exporting all logs from: %s to %s... This may take a while.\n' % (
            prettify(self.start),
            prettify(self.stop),
        )

        print 'Exporting Logs'
        print '--------------'

        date = self.start
        while date < self.stop:

            # Schedule the Sumologic job.
            job_url = self.client.create_job(date, date + self.INCREMENT)
            print '- Created Job: ', prettify(date)

            # Pause to allow Sumologic to process this job.
            sleep(self.SLEEP_SECONDS)

            # Figure out how many logs there are for the given date.
            total_logs = self.client.get_count(job_url)

            # If there are logs to be downloaded, let's do it.
            if total_logs:
                print ' - Downloading %d logs.' % total_logs
                write_to_file(prettify(date),self.client.get_logs(job_url, total_logs))
            else:
                print ' - No logs found.'

            # Move forward.
            date += self.INCREMENT

        print '\nFinished downloading logs!'


def write_to_file(file_name, logs):
    with gzip.open(file_name, mode='wb') as f:
        for log in logs:
            f.write(dumps(log) + '\n')


def configure():
    """
    Read in and store the user's Sumologic credentials.

    Credentials will be stored in ~/.sumo
    """
    print 'Initializing `sumologic_export.py`...\n'
    print "To get started, we'll need to get your Sumologic credentials."

    while True:
        email = raw_input('Enter your email: ').strip()
        password = raw_input('Enter your password: ').strip()
        if not (email or password):
            print '\nYour Sumologic credentials are needed to continue!\n'
            continue

        print 'Your API credentials are stored in the file:', CONFIG_FILE, '\n'
        print 'Run sumologic_export.py for usage information.'

        with open(CONFIG_FILE, 'wb') as cfg:
            cfg.write(dumps({
                'email': email,
                'password': password,
            }, indent=2, sort_keys=True))

        # Make the configuration file only accessible to the current user --
        # this makes the credentials a bit more safe.
        chmod(CONFIG_FILE, 0600)

        break


def main(args):
    """
    Handle command line options.

    :param args: Command line arguments.
    """
    if args['-v']:
        print VERSION
        raise SystemExit()

    elif args['configure']:
        configure()
        raise SystemExit()

    exporter = Exporter()
    exporter.export(args['<start>'], args['<stop>'])


if __name__ == '__main__':
    main(docopt(__doc__, version=VERSION))
