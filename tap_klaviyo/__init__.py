#!/usr/bin/env/python

import json
import os
import singer
from singer import metadata
from tap_klaviyo.utils import get_incremental_pull, get_full_pulls, get_all_pages, get_incremental_pull_additional_properties

ENDPOINTS = {
    'global_exclusions': 'https://a.klaviyo.com/api/v1/people/exclusions',
    'lists': 'https://a.klaviyo.com/api/v1/lists',
    # to get list of available metrics
    'metrics': 'https://a.klaviyo.com/api/v1/metrics',
    # to get individual metric data
    'metric': 'https://a.klaviyo.com/api/v1/metric/',
    # to get list members
    'list_members': 'https://a.klaviyo.com/api/v2/group/{list_id}/members/all',
    # to get list info
    'list_info': 'https://a.klaviyo.com/api/v2/list/{list_id}'
}

EVENT_MAPPINGS = {
    "Received Email": "receive",
    "Clicked Email": "click",
    "Opened Email": "open",
    "Bounced Email": "bounce",
    "Unsubscribed": "unsubscribe",
    "Marked Email as Spam": "mark_as_spam",
    "Unsubscribed from List": "unsub_list",
    "Subscribed to List": "subscribe_list",
    "Updated Email Preferences": "update_email_preferences",
    "Dropped Email": "dropped_email",
    "Placed Order": "placed_order",
    "Cancelled Order": "cancelled_order"
}

ADDITIONAL_PROPERTIES = {
    "placed_order": {
        "attributed_flow_value",
        "attributed_flow_count",
        "carthook_funnel_id_value",
        "carthook_funnel_id_count",
    }
}

ADDITIONAL_PROPERTIES_KEYS = {
    "attributed_flow_value",
    "attributed_flow_count",
    "carthook_funnel_id_value",
    "carthook_funnel_id_count"
}

logger = singer.get_logger()

class ListMemberStreamException(Exception):
    pass

class Stream(object):
    def __init__(self, stream, tap_stream_id, key_properties, puller):
        self.stream = stream
        self.tap_stream_id = tap_stream_id
        self.key_properties = key_properties
        self.puller = puller
        self.metadata = []

    def to_catalog_dict(self):
        schema = load_schema(self.stream)

        # self.metadata.append({
        #     'breadcrumb': (),
        #     'metadata': {
        #         'table-key-properties': self.key_properties,
        #         'forced-replication-method': self.replication_method
        #     }
        # })

        # for k in schema['properties']:
        #     self.metadata.append({
        #         'breadcrumb': ('properties', k),
        #         'metadata': { 'inclusion': 'automatic' }
        #     })

        return {
            'stream': self.stream,
            'tap_stream_id': self.tap_stream_id,
            'key_properties': self.key_properties,
            'schema': load_schema(self.stream),
            'metadata': self.metadata
        }


CREDENTIALS_KEYS = ["api_key"]
REQUIRED_CONFIG_KEYS = ["start_date"] + CREDENTIALS_KEYS

GLOBAL_EXCLUSIONS = Stream(
    'global_exclusions',
    'global_exclusions',
    'email',
    'full'
)

LISTS = Stream(
    'lists',
    'lists',
    'id',
    'lists'
)

LIST_MEMBERS = Stream(
    'list_members',
    'list_members',
    'email',
    'full'
)

FULL_STREAMS = [GLOBAL_EXCLUSIONS, LISTS, LIST_MEMBERS]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(name):
    return json.load(open(get_abs_path('schemas/{}.json'.format(name))))


def do_sync(config, state, catalog):
    api_key = config['api_key']
    list_ids = config.get('list_ids')
    start_date = config['start_date'] if 'start_date' in config else None
    stream_ids_to_sync = set()

    for stream in catalog.get('streams'):
        mdata = metadata.to_map(stream['metadata'])
        if metadata.get(mdata, (), 'selected'):
            stream_ids_to_sync.add(stream['tap_stream_id'])
    
    for stream in catalog['streams']:
        if stream['tap_stream_id'] not in stream_ids_to_sync:
            continue
        singer.write_schema(
            stream['stream'],
            stream['schema'],
            stream['key_properties']
        )
        if stream['stream'] in EVENT_MAPPINGS.values():
            get_incremental_pull(stream, ENDPOINTS['metric'], state,
                                 api_key, start_date)
        elif stream['stream'] == 'list_members':
            if list_ids:
                get_full_pulls(stream, ENDPOINTS[stream['stream']], api_key, list_ids)
            else:
                raise ListMemberStreamException(
                    'A list of Klaviyo List IDs must be specified in the client tap '
                    'config if extracting list members. Check out the Untuckit Klaviyo '
                    'tap for reference')
        elif stream['stream'] in ADDITIONAL_PROPERTIES_KEYS:
            get_incremental_pull_additional_properties(stream, ENDPOINTS['metric'], state,
                                 api_key, start_date)
        else:
            get_full_pulls(stream, ENDPOINTS[stream['stream']], api_key)


def get_available_metrics(api_key):
    metric_streams = []
    for response in get_all_pages('metric_list',
                                  ENDPOINTS['metrics'], api_key):
        for metric in response.json().get('data'):
            if metric['name'] in EVENT_MAPPINGS:
                metric_streams.append(
                    Stream(
                        stream=EVENT_MAPPINGS[metric['name']],
                        tap_stream_id=metric['id'],
                        key_properties="id",
                        puller='INCREMENTAL'
                    )
                )
                if EVENT_MAPPINGS[metric['name']] in ADDITIONAL_PROPERTIES:
                     for additional_property in ADDITIONAL_PROPERTIES[EVENT_MAPPINGS[metric['name']]]:
                        logger.info("attributed endpoint " + additional_property)
                        metric_streams.append(
                            Stream(
                                stream=additional_property,
                                tap_stream_id=metric['id'],
                                key_properties="id",
                                puller='INCREMENTAL'
                            )
                        )
    return metric_streams


def discover(api_key):
    metric_streams = get_available_metrics(api_key)
    return {"streams": [a.to_catalog_dict()
                        for a in metric_streams + FULL_STREAMS]}


def do_discover(api_key):
    print(json.dumps(discover(api_key), indent=2))


def main():

    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config['api_key'])

    else:
        catalog = args.catalog.to_dict() if args.catalog else discover(
            args.config['api_key'])
        state = args.state if args.state else {"bookmarks": {}}
        do_sync(args.config, state, catalog)


if __name__ == '__main__':
    main()