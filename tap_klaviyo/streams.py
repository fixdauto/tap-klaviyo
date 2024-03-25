"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

import typing as t
from pathlib import Path

from urllib.parse import parse_qsl
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import time

from singer_sdk import metrics

from tap_klaviyo.client import KlaviyoStream

if t.TYPE_CHECKING:
    from urllib.parse import ParseResult

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class CampaignMetricAggregatesStream(KlaviyoStream):
    """Set up to aggregate the last three months of performance"""

    name = "campaignmetricaggregates"
    path = "/metric-aggregates"
    primary_keys = ["campaign name", "date"]
    replication_key = ""
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"
    rest_method = "POST"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$[links][next]"
    # Give a default aggregate to "count". Change this if you want a stream to use "sum_value"
    # or "unique" instead
    aggregate = "count"

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: _TToken | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Developers may override this method if the API requires a custom payload along
        with the request. (This is generally not required for APIs which use the
        HTTP 'GET' method.)

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        now = datetime.now()
        # TO-DO: Make the time period something that can be changed in the config
        last_three_months = datetime.now() - relativedelta(months=6)
        start_date = last_three_months.strftime("%Y-%m-%dT%H:%M:%S")
        end_date = now.strftime("%Y-%m-%dT%H:%M:%S")
        
        return {
            "data": {
                "type": "metric-aggregate",
                "attributes": {
                    "metric_id": self.metric_id,
                    "interval": "day",
                    # TO-DO: Figure out how to paginate so we don't need a huge page size
                    "page_size": 10000,
                    "timezone": "UTC",
                    "by": ["Campaign Name"],
                    "measurements": [f"{self.aggregate}"],
                    "filter": [f"greater-or-equal(datetime,{start_date})",f"less-than(datetime,{end_date})"]
                }
            }
        }

class ReceivedEmailsByCampaignStream(CampaignMetricAggregatesStream):
    """Define custom stream"""

    name = "receivedemailsbycampaign"
    metric_id = "H4DrTy"
    
    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record['data']['attributes']['data']:
                dimensions = item['dimensions'][0]
                counts = item['measurements']['count']
                
                for date, count in zip(record['data']['attributes']['dates'], counts):
                    result = {
                        "Date": date,
                        "Campaign Name": dimensions,
                        "Count": count
                    }
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record

class OpenedEmailsByCampaignStream(CampaignMetricAggregatesStream):
    """Define custom stream"""

    name = "openedemailsbycampaign"
    metric_id = "P4W93C"
    
    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record['data']['attributes']['data']:
                dimensions = item['dimensions'][0]
                counts = item['measurements']['count']
                
                for date, count in zip(record['data']['attributes']['dates'], counts):
                    result = {
                        "Date": date,
                        "Campaign Name": dimensions,
                        "Count": count
                    }
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record

class ClickedEmailsByCampaignStream(CampaignMetricAggregatesStream):
    """Define custom stream"""

    name = "clickedemailsbycampaign"
    metric_id = "MYayva"
    
    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record['data']['attributes']['data']:
                dimensions = item['dimensions'][0]
                counts = item['measurements']['count']
                
                for date, count in zip(record['data']['attributes']['dates'], counts):
                    result = {
                        "Date": date,
                        "Campaign Name": dimensions,
                        "Count": count
                    }
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record

class BouncedEmailsByCampaignStream(CampaignMetricAggregatesStream):
    """Define custom stream"""

    name = "bouncedemailsbycampaign"
    metric_id = "Ld4b2k"
    
    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record['data']['attributes']['data']:
                dimensions = item['dimensions'][0]
                counts = item['measurements']['count']
                
                for date, count in zip(record['data']['attributes']['dates'], counts):
                    result = {
                        "Date": date,
                        "Campaign Name": dimensions,
                        "Count": count
                    }
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record

class UnsubscribedFromListByCampaignStream(CampaignMetricAggregatesStream):
    """Define custom stream"""

    name = "unsubscribedfromlistbycampaign"
    metric_id = "MvjPKh"
    
    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record['data']['attributes']['data']:
                dimensions = item['dimensions'][0]
                counts = item['measurements']['count']
                
                for date, count in zip(record['data']['attributes']['dates'], counts):
                    result = {
                        "Date": date,
                        "Campaign Name": dimensions,
                        "Count": count
                    }
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record

# TO-DO: Figure out correct metrics to track for Placed Orders and Revenue Earned, and
# figure out why they don't capture Campaign Names

# class PlacedOrderByCampaignStream(CampaignMetricAggregatesStream):
#     """Define custom stream"""

#     name = "placedorderbycampaign"
#     metric_id = "T7RgqW"

    
#     def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
#         """Return a generator of record-type dictionary objects.

#         Each record emitted should be a dictionary of property names to their values.

#         Args:
#             context: Stream partition or context dictionary.

#         Yields:
#             One item per (possibly processed) record in the API.
#         """
#         for record in self.request_records(context):
#             for item in record['data']['attributes']['data']:
#                 dimensions = item['dimensions'][0]
#                 counts = item['measurements']['count']
                
#                 for date, count in zip(record['data']['attributes']['dates'], counts):
#                     result = {
#                         "Date": date,
#                         "Campaign Name": dimensions,
#                         "Count": count
#                     }
#                     if count == 0:
#                         continue
#                     transformed_record = self.post_process(result, context)
#                     if transformed_record is None:
#                         continue
#                     yield transformed_record

# class CheckoutAmountByCampaignStream(CampaignMetricAggregatesStream):
#     """Define custom stream"""

#     name = "checkoutamountbycampaign"
#     metric_id = "NZdCMH"
#     aggregate = "sum_value"
#     schema_filepath = SCHEMAS_DIR / "metricaggregatessumbycampaign.json"
    
#     def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
#         """Return a generator of record-type dictionary objects.

#         Each record emitted should be a dictionary of property names to their values.

#         Args:
#             context: Stream partition or context dictionary.

#         Yields:
#             One item per (possibly processed) record in the API.
#         """
#         for record in self.request_records(context):
#             for item in record['data']['attributes']['data']:
#                 dimensions = item['dimensions'][0]
#                 sum_values = item['measurements']['sum_value']
                
#                 for date, sum_value in zip(record['data']['attributes']['dates'], sum_values):
#                     result = {
#                         "Date": date,
#                         "Campaign Name": dimensions,
#                         "Sum": sum_value
#                     }
#                     if sum_value == 0:
#                         continue
#                     transformed_record = self.post_process(result, context)
#                     if transformed_record is None:
#                         continue
#                     yield transformed_record

class EventsStream(KlaviyoStream):
    """Define custom stream."""

    name = "events"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"
    schema_filepath = SCHEMAS_DIR / "event.json"

    UTC = timezone.utc
    DEFAULT_START_DATE = datetime(2000, 1, 1, tzinfo=UTC).isoformat()

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        self.logger.info(f"result is: {row}")
        row["datetime"] = row["attributes"]["datetime"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True
    
    def _isodate_from_date_string(date_string: str) -> str:
        """Convert a date string to an ISO date string.

        Args:
            date_string: The date string to convert.

        Returns:
            An ISO date string.
        """
        return datetime.strptime(date_string, "%Y-%m-%d").replace(tzinfo=UTC).isoformat()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}

        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        if self.replication_key:
            if self.get_starting_timestamp(context):
                filter_timestamp = self.get_starting_timestamp(context)
            elif self.config.get("start_date"):
                filter_timestamp = _isodate_from_date_string(self.config("start_date"))
            else:
                filter_timestamp = DEFAULT_START_DATE

            if self.is_sorted:
                params["sort"] = self.replication_key

            params["filter"] = f'equals(metric_id,"H4DrTy"),greater-than({self.replication_key},{filter_timestamp})'

        if self.max_page_size:
            params["page[size]"] = self.max_page_size
        return params


class CampaignsStream(KlaviyoStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "campaigns.json"

    @property
    def partitions(self) -> list[dict] | None:
        return [
            {
                "filter": "equals(messages.channel,'email')",
            },
            {
                "filter": "equals(messages.channel,'sms')",
            },
        ]

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        url_params = super().get_url_params(context, next_page_token)

        # Apply channel filters
        if context:
            parent_filter = url_params["filter"]
            url_params["filter"] = f"and({parent_filter},{context['filter']})"

        return url_params

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated_at"] = row["attributes"]["updated_at"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True


class ProfilesStream(KlaviyoStream):
    """Define custom stream."""

    name = "profiles"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "profiles.json"
    max_page_size = 100

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True


class MetricsStream(KlaviyoStream):
    """Define custom stream."""

    name = "metrics"
    path = "/metrics"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "metrics.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class ListsStream(KlaviyoStream):
    """Define custom stream."""

    name = "lists"
    path = "/lists"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "lists.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        context = context or {}
        context["list_id"] = record["id"]

        return super().get_child_context(record, context)  # type: ignore[no-any-return]

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class ListPersonStream(KlaviyoStream):
    """Define custom stream."""

    name = "listperson"
    path = "/lists/{list_id}/relationships/profiles/"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ListsStream
    schema_filepath = SCHEMAS_DIR / "listperson.json"
    max_page_size = 1000

    def post_process(self, row: dict, context: dict) -> dict | None:
        row["list_id"] = context["list_id"]
        return row


class FlowsStream(KlaviyoStream):
    """Define custom stream."""

    name = "flows"
    path = "/flows"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "flows.json"


class TemplatesStream(KlaviyoStream):
    """Define custom stream."""

    name = "templates"
    path = "/templates"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "templates.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True