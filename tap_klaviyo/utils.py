import datetime
import time
import singer
from singer import metrics
import requests
import hashlib
import backoff
import time
import random

DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
DATETIME_FAP = "%Y-%m-%d"


session = requests.Session()
logger = singer.get_logger()


def dt_to_ts(dt):
    return int(time.mktime(datetime.datetime.strptime(
        dt, DATETIME_FMT).timetuple()))

def dt(dt):
    return datetime.datetime.strptime(
        dt, DATETIME_FMT)


def ts_to_dt(ts):
    return datetime.datetime.fromtimestamp(
        int(ts)).strftime(DATETIME_FMT)

def dt_to_fds(dt, format_string):
    return datetime.datetime.strptime(dt, DATETIME_FMT).strftime(format_string)


def update_state(state, entity, dt):
    if dt is None:
        return

    # convert timestamp int to datetime
    if isinstance(dt, int):
        dt = ts_to_dt(dt)

    if entity not in state:
        state['bookmarks'][entity] = {'since': dt}

    if dt >= state['bookmarks'][entity]['since']:
        state['bookmarks'][entity] = {'since': dt}

    logger.info("Replicated %s up to %s" % (
        entity, state['bookmarks'][entity]))


def get_starting_point(stream, state, start_date):
    if stream['stream'] in state['bookmarks'] and \
            state['bookmarks'][stream['stream']] is not None:
        return dt_to_ts(state['bookmarks'][stream['stream']]['since'])
    elif start_date:
        return dt_to_ts(start_date)
    else:
        return None

def get_starting_point_additional_properties(stream, state, start_date):
    if stream['stream'] in state['bookmarks'] and \
                    state['bookmarks'][stream['stream']] is not None:
        return datetime.datetime.strptime(state['bookmarks'][stream['stream']]['since'], DATETIME_FMT)
    elif start_date:
        return dt(start_date)
    else:
        return None


def get_latest_event_time(events):
    return ts_to_dt(int(events[-1]['timestamp'])) if len(events) else None


@backoff.on_exception(backoff.expo, requests.HTTPError, max_tries=10, factor=2, logger=logger)
def authed_get(source, url, params):
    with metrics.http_request_timer(source) as timer:
        resp = session.request(method='get', url=url, params=params)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return resp


def get_all_using_next(stream, url, api_key, since=None):
    while True:
        r = authed_get(stream, url, {'api_key': api_key,
                                     'since': since,
                                     'sort': 'asc'})
        yield r
        if 'next' in r.json() and r.json()['next']:
            since = r.json()['next']
        else:
            break

def get_all_additional_properties_using_next(stream, url, api_key, since=None):
    while True:
        split_stream = stream.rsplit("_",1)
        r = authed_get(stream, url, {'api_key': api_key,
                                     'start_date': since.strftime(DATETIME_FAP),
                                     'end_date': since.strftime(DATETIME_FAP),
                                     'by' : '$'+split_stream[0],
                                     'measurement' :  split_stream[1]
                                     })
        yield r
        if since < datetime.datetime.now() - datetime.timedelta(days=1):
            since = since + datetime.timedelta(days=1)
        else:
            break


def get_all_pages(source, url, api_key):
    page = 0
    while True:
        r = authed_get(source, url, {'page': page, 'api_key': api_key})
        yield r
        if r.json()['end'] < r.json()['total'] - 1:
            page += 1
        else:
            break

def list_members_request(url, api_key, id, marker=None):
    #Sleep timer between requests to avoid hitting Klaviyo's API rate limit
    time.sleep(0.05)
    if(marker != None):
        r = authed_get('list_members', url.format(list_id=id), {'api_key': api_key,
                                                                'marker': marker})
    else:
        r = authed_get('list_members', url.format(list_id=id), {'api_key': api_key})
    return r

def get_list_members(url, api_key, id):
    marker = None
    while True:
        raw_response = list_members_request(url, api_key, id, marker)
        response = raw_response.json()
        if 'detail' in response.keys() and 'throttled' in response.get('detail'):
            response = None
            retryLimit = 5
            retryCount = 0
            while(response == None and retryCount < retryLimit):
                retryCount += 1
                #Dynamic sleep method uses the Retry-After header from the throttle response to set a sleep timer
                # time.sleep(int(raw_response.headers['Retry-After']))
                time.sleep(600)
                retry = list_members_request(url, api_key, id).json()
                if 'detail' not in retry.keys() or 'throttled' not in retry.get('detail'):
                    response = retry
        curr_records = response.get('records')
        if curr_records is not None:
            records = hydrate_record_with_list_id(curr_records, id)
            yield records
            marker = response.get('marker')
            if not marker:
                break
        else:
            logger.info("reached point where curr_records is none")
            logger.info(f"id is: {id}")
            logger.info(f"response is: {response}")
            break


def hydrate_record_with_list_id(records, list_id):
    """
    Args:
        records (array [JSON]):
        list_id (str):
    Returns:
        array of records, with the list_id appended to each record
    """
    for record in records:
        record['list_id'] = list_id

    return records


def get_incremental_pull(stream, endpoint, state, api_key, start_date):
    latest_event_time = get_starting_point(stream, state, start_date)

    with metrics.record_counter(stream['stream']) as counter:
        url = '{}{}/timeline'.format(
            endpoint,
            stream['tap_stream_id']
        )
        for response in get_all_using_next(
                stream['stream'], url, api_key,
                latest_event_time):
            events = response.json().get('data')

            if events:
                counter.increment(len(events))

                singer.write_records(stream['stream'], events)

                update_state(state, stream['stream'],
                             get_latest_event_time(events))
                singer.write_state(state)

    return state

def get_incremental_pull_additional_properties(stream, endpoint, state, api_key, start_date):
    latest_event_time = get_starting_point_additional_properties(stream, state, start_date)
    with metrics.record_counter(stream['stream']) as counter:
        url = '{}{}/export'.format(
            endpoint,
            stream['tap_stream_id']
        )
        for response in get_all_additional_properties_using_next(
                stream['stream'], url, api_key,
                latest_event_time):
            events = response.json()
            if events:
                # latest_date = None
                for result in events.get('results'):
                    result['id'] = events.get('id')
                    result['data'][0]['segment'] = result['segment']
                    result['data'][0]['metric'] = events['metric']
                    counter.increment(1)
                    hash_object = hashlib.sha224((result['segment']+result['data'][0]['date']).encode('utf-8'))
                    result['data'][0]['id'] = hash_object.hexdigest()

                    singer.write_records(stream['stream'], result.get('data'))
                update_state(state, stream['stream'], datetime.datetime.today().strftime(DATETIME_FMT))
                singer.write_state(state)

    return state

def get_full_pulls(resource, endpoint, api_key, list_ids=None):
    with metrics.record_counter(resource['stream']) as counter:
        if resource['stream'] == 'list_members':
            time.sleep(0.02)
            for id in list_ids:
                logger.info(f'id is: {id}')
                for records in get_list_members(endpoint, api_key, id):
                    if records:
                        counter.increment(len(records))
                        singer.write_records(resource['stream'], records)
        else:
            for response in get_all_pages(resource['stream'], endpoint, api_key):
                records = response.json().get('data')

                if records:
                    counter.increment(len(records))
                    singer.write_records(resource['stream'], records)