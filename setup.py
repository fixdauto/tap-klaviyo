#!/usr/bin/env python

from setuptools import setup

setup(name='tap-klaviyo',
      version='0.1.1',
      description='Singer.io tap for extracting data from the Klaviyo API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_klaviyo'],
      install_requires=['singer-python==5.8.1',
                        'requests==2.20.0',
                        'backoff',
                        'ipdb'],
      entry_points='''
          [console_scripts]
          tap-klaviyo=tap_klaviyo:main
      ''',
      packages=['tap_klaviyo'],
      package_data={
          'tap_klaviyo/schemas': [
                "bounce.json",
                "click.json",
                "mark_as_spam.json",
                "open.json",
                "receive.json",
                "unsubscribe.json",
                "dropped_email.json",
                "global_exclusions.json",
                "lists.json",
                "list_members.json",
                "subscribe_list.json",
                "unsub_list.json",
                "update_email_preferences.json",
                "cancelled_order.json"
                "placed_order.json"
              ]
         },
      include_package_data=True
)