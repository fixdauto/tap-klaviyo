"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

import typing as t
from pathlib import Path

from urllib.parse import parse_qsl
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import time

from singer_sdk import metrics

from tap_klaviyo.client import KlaviyoStream, _isodate_from_date_string

if t.TYPE_CHECKING:
    from urllib.parse import ParseResult

    import requests

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class CampaignValuesReport(KlaviyoStream):
    """Grabs overall performance by campaign over the last 12 months"""

    name = "campaignvaluesreport"
    path = "/campaign-values-reports"
    primary_keys = ["CampaignId"]
    replication_key = ""
    rest_method = "POST"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$[links][next]"
    schema_filepath = SCHEMAS_DIR / "campaignvaluesreport.json"

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
        return {
            "data": {
                "type": "campaign-values-report",
                "attributes": {
                    "statistics": [
                        "recipients",
                        "opens_unique",
                        "open_rate",
                        "clicks_unique",
                        "click_rate",
                        "bounced",
                        "bounce_rate",
                        "spam_complaints",
                        "spam_complaint_rate",
                        "conversions",
                        "conversion_uniques",
                        "conversion_value",
                        "conversion_rate",
                        "average_order_value",
                        "revenue_per_recipient",
                        "unsubscribes",
                        "unsubscribe_rate"
                    ],
                    "timeframe": {
                        "key": "last_365_days"
                    },
                    "conversion_metric_id": "T7RgqW"
                },
            }
        }
    
    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["results"]:
                campaign_id = item["groupings"]["campaign_id"]
                recipients = item["statistics"]["recipients"]
                unique_opens = item["statistics"]["opens_unique"]
                open_rate = item["statistics"]["open_rate"]
                unique_clicks = item["statistics"]["clicks_unique"]
                click_rate = item["statistics"]["click_rate"]
                bounced_emails = item["statistics"]["bounced"]
                bounce_rate = item["statistics"]["bounce_rate"]
                spam_complaints = item["statistics"]["spam_complaints"]
                spam_complaint_rate = item["statistics"]["spam_complaint_rate"]
                conversions = item["statistics"]["conversions"]
                conversion_uniques = item["statistics"]["conversion_uniques"]
                conversion_value = item["statistics"]["conversion_value"]
                conversion_rate = item["statistics"]["conversion_rate"]
                average_order_value = item["statistics"]["average_order_value"]
                revenue_per_recipient = item["statistics"]["revenue_per_recipient"]
                unsubscribes = item["statistics"]["unsubscribes"]
                unsubscribe_rate = item["statistics"]["unsubscribe_rate"]

                result = {
                    "CampaignId": campaign_id,
                    "Recipients": recipients,
                    "UniqueOpens": unique_opens,
                    "OpenRate": open_rate,
                    "UniqueClicks": unique_clicks,
                    "ClickRate": click_rate,
                    "BouncedEmails": bounced_emails,
                    "BounceRate": bounce_rate,
                    "SpamComplaints": spam_complaints,
                    "SpamComplaintRate": spam_complaint_rate,
                    "Conversions": conversions,
                    "ConversionUniques": conversion_uniques,
                    "ConversionValue": conversion_value,
                    "ConversionRate": conversion_rate,
                    "AverageOrderValue": average_order_value,
                    "RevenuePerRecipient": revenue_per_recipient,
                    "Unsubscribes": unsubscribes,
                    "UnsubscribeRate": unsubscribe_rate
                }
                
                transformed_record = self.post_process(result, context)
                if transformed_record is None:
                    continue
                yield transformed_record


class FlowValuesReport(KlaviyoStream):
    """Grabs overall performance by flow over the last 12 months."""

    name = "flowvaluesreport"
    path = "/flow-values-reports"
    primary_keys = ["FlowId"]
    replication_key = ""
    rest_method = "POST"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$[links][next]"
    schema_filepath = SCHEMAS_DIR / "flowvaluesreport.json"

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
        return {
            "data": {
                "type": "flow-values-report",
                "attributes": {
                    "statistics": [
                        "recipients",
                        "opens_unique",
                        "open_rate",
                        "clicks_unique",
                        "click_rate",
                        "bounced",
                        "bounce_rate",
                        "spam_complaints",
                        "spam_complaint_rate",
                        "conversions",
                        "conversion_uniques",
                        "conversion_value",
                        "conversion_rate",
                        "average_order_value",
                        "revenue_per_recipient",
                        "unsubscribes",
                        "unsubscribe_rate"
                    ],
                    "timeframe": {
                        "key": "last_365_days"
                    },
                    "conversion_metric_id": "T7RgqW"
                },
            }
        }
    
    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["results"]:
                flow_id = item["groupings"]["flow_id"]
                recipients = item["statistics"]["recipients"]
                unique_opens = item["statistics"]["opens_unique"]
                open_rate = item["statistics"]["open_rate"]
                unique_clicks = item["statistics"]["clicks_unique"]
                click_rate = item["statistics"]["click_rate"]
                bounced_emails = item["statistics"]["bounced"]
                bounce_rate = item["statistics"]["bounce_rate"]
                spam_complaints = item["statistics"]["spam_complaints"]
                spam_complaint_rate = item["statistics"]["spam_complaint_rate"]
                conversions = item["statistics"]["conversions"]
                conversion_uniques = item["statistics"]["conversion_uniques"]
                conversion_value = item["statistics"]["conversion_value"]
                conversion_rate = item["statistics"]["conversion_rate"]
                average_order_value = item["statistics"]["average_order_value"]
                revenue_per_recipient = item["statistics"]["revenue_per_recipient"]
                unsubscribes = item["statistics"]["unsubscribes"]
                unsubscribe_rate = item["statistics"]["unsubscribe_rate"]

                result = {
                    "FlowId": flow_id,
                    "Recipients": recipients,
                    "UniqueOpens": unique_opens,
                    "OpenRate": open_rate,
                    "UniqueClicks": unique_clicks,
                    "ClickRate": click_rate,
                    "BouncedEmails": bounced_emails,
                    "BounceRate": bounce_rate,
                    "SpamComplaints": spam_complaints,
                    "SpamComplaintRate": spam_complaint_rate,
                    "Conversions": conversions,
                    "ConversionUniques": conversion_uniques,
                    "ConversionValue": conversion_value,
                    "ConversionRate": conversion_rate,
                    "AverageOrderValue": average_order_value,
                    "RevenuePerRecipient": revenue_per_recipient,
                    "Unsubscribes": unsubscribes,
                    "UnsubscribeRate": unsubscribe_rate
                }
                
                transformed_record = self.post_process(result, context)
                if transformed_record is None:
                    continue
                yield transformed_record


class FlowSeriesReport(KlaviyoStream):
    """Grabs daily performance by flow over the last 60 days."""

    name = "flowseriesreport"
    path = "/flow-series-reports"
    primary_keys = ["Date", "FlowId", "FlowMessageId"]
    replication_key = ""
    rest_method = "POST"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$[links][next]"
    schema_filepath = SCHEMAS_DIR / "flowseriesreport.json"

    # Computed once per sync in get_records and reused by prepare_request_payload
    # on every page, so the timeframe window is stable across paginated requests.
    _start_date: str | None = None
    _end_date: str | None = None

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: _TToken | None,
    ) -> dict | None:
        # Use pre-computed dates if available (set in get_records before the first
        # request so all pages share the same window). Fall back to computing inline
        # only if called outside of a get_records cycle.
        if self._start_date and self._end_date:
            start_date = self._start_date
            end_date = self._end_date
        else:
            now = datetime.now(timezone.utc)
            last_60_days = now - relativedelta(days=60)
            start_date = last_60_days.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            end_date = now.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        return {
            "data": {
                "type": "flow-series-report",
                "attributes": {
                    "statistics": [
                        "recipients",
                        "opens_unique",
                        "open_rate",
                        "clicks_unique",
                        "click_rate",
                        "bounced",
                        "bounce_rate",
                        "spam_complaints",
                        "spam_complaint_rate",
                        "conversions",
                        "conversion_uniques",
                        "conversion_value",
                        "conversion_rate",
                        "average_order_value",
                        "revenue_per_recipient",
                        "unsubscribes",
                        "unsubscribe_rate"
                    ],
                    "timeframe": {
                        "start": start_date,
                        "end": end_date
                    },
                    "interval": "daily",
                    "conversion_metric_id": "T7RgqW",
                },
            }
        }

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        # Pin the window once before the first request so that every paginated
        # POST uses the exact same timeframe (prepare_request_payload reads these).
        now = datetime.now(timezone.utc)
        last_60_days = now - relativedelta(days=60)
        self._start_date = last_60_days.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        self._end_date = now.strftime("%Y-%m-%dT%H:%M:%S+00:00")

        for record in self.request_records(context):
            results = record["data"]["attributes"]["results"]
            if len(results) == 0:
                continue
            dates = record["data"]["attributes"]["date_times"]
            for i in range(len(dates)):
                date = dates[i]
                for j in range(len(results)):
                    flow_id = results[j]["groupings"]["flow_id"]
                    flow_message_id = results[j]["groupings"]["flow_message_id"]
                    recipients = results[j]["statistics"]["recipients"][i]
                    unique_opens = results[j]["statistics"]["opens_unique"][i]
                    open_rate = results[j]["statistics"]["open_rate"][i]
                    unique_clicks = results[j]["statistics"]["clicks_unique"][i]
                    click_rate = results[j]["statistics"]["click_rate"][i]
                    bounced_emails = results[j]["statistics"]["bounced"][i]
                    bounce_rate = results[j]["statistics"]["bounce_rate"][i]
                    spam_complaints = results[j]["statistics"]["spam_complaints"][i]
                    spam_complaint_rate = results[j]["statistics"]["spam_complaint_rate"][i]
                    conversions = results[j]["statistics"]["conversions"][i]
                    conversion_uniques = results[j]["statistics"]["conversion_uniques"][i]
                    conversion_value = results[j]["statistics"]["conversion_value"][i]
                    conversion_rate = results[j]["statistics"]["conversion_rate"][i]
                    average_order_value = results[j]["statistics"]["average_order_value"][i]
                    revenue_per_recipient = results[j]["statistics"]["revenue_per_recipient"][i]
                    unsubscribes = results[j]["statistics"]["unsubscribes"][i]
                    unsubscribe_rate = results[j]["statistics"]["unsubscribe_rate"][i]

                    result = {
                        "Date": date,
                        "FlowId": flow_id,
                        "FlowMessageId": flow_message_id,
                        "Recipients": recipients,
                        "UniqueOpens": unique_opens,
                        "OpenRate": open_rate,
                        "UniqueClicks": unique_clicks,
                        "ClickRate": click_rate,
                        "BouncedEmails": bounced_emails,
                        "BounceRate": bounce_rate,
                        "SpamComplaints": spam_complaints,
                        "SpamComplaintRate": spam_complaint_rate,
                        "Conversions": conversions,
                        "ConversionUniques": conversion_uniques,
                        "ConversionValue": conversion_value,
                        "ConversionRate": conversion_rate,
                        "AverageOrderValue": average_order_value,
                        "RevenuePerRecipient": revenue_per_recipient,
                        "Unsubscribes": unsubscribes,
                        "UnsubscribeRate": unsubscribe_rate
                    }

                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class MetricAggregatesStream(KlaviyoStream):
    """Set up to aggregate the last three months of performance"""

    name = "campaignmetricaggregates"
    path = "/metric-aggregates"
    primary_keys = ["CampaignName", "Date"]
    replication_key = ""
    rest_method = "POST"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$[links][next]"
    # Give a default aggregate to "count" and aggregate_by of "Campaign Name". Change
    # this if you want a stream to use "sum_value" or "unique" instead, or if you want
    # to aggregate by "attributed_flow"
    aggregate = "count"
    aggregate_by = ["Campaign Name"]

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
        # Twelve months is as far back as you can go for the metric-aggregates endpoint
        last_twelve_months = datetime.now() - relativedelta(months=12)
        start_date = last_twelve_months.strftime("%Y-%m-%dT%H:%M:%S")
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
                    "by": self.aggregate_by,
                    "measurements": [f"{self.aggregate}"],
                    "filter": [
                        f"greater-or-equal(datetime,{start_date})",
                        f"less-than(datetime,{end_date})",
                    ],
                },
            }
        }


class ReceivedEmailsByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "receivedemailsbycampaign"
    metric_id = "H4DrTy"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class OpenedEmailsByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "openedemailsbycampaign"
    metric_id = "P4W93C"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"
    aggregate = "unique"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["unique"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class ClickedEmailsByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "clickedemailsbycampaign"
    metric_id = "MYayva"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"
    aggregate = "unique"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["unique"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class BouncedEmailsByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "bouncedemailsbycampaign"
    metric_id = "Ld4b2k"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class UnsubscribesByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "unsubscribesbycampaign"
    metric_id = "QqUYDV"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class PlacedOrdersByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "placedordersbycampaign"
    metric_id = "T7RgqW"
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbycampaign.json"
    aggregate_by = ["$attributed_message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "CampaignName": dimensions, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class CheckoutAmountsByCampaignStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "checkoutamountsbycampaign"
    metric_id = "T7RgqW"
    schema_filepath = SCHEMAS_DIR / "metricaggregatessumbycampaign.json"
    aggregate = "sum_value"
    aggregate_by = ["$attributed_message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                dimensions = item["dimensions"][0]
                sum_values = item["measurements"]["sum_value"]

                for date, sum_value in zip(
                    record["data"]["attributes"]["dates"], sum_values
                ):
                    result = {
                        "Date": date,
                        "CampaignName": dimensions,
                        "Sum": sum_value,
                    }
                    if sum_value == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class ReceivedEmailsByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "receivedemailsbyflowmessage"
    metric_id = "H4DrTy"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate_by = ["$message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class OpenedEmailsByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "openedemailsbyflowmessage"
    metric_id = "P4W93C"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate = "unique"
    aggregate_by = ["$message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["unique"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class ClickedEmailsByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "clickedemailsbyflowmessage"
    metric_id = "MYayva"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate = "unique"
    aggregate_by = ["$message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["unique"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class BouncedEmailsByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "bouncedemailsbyflowmessage"
    metric_id = "Ld4b2k"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate_by = ["$message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class UnsubscribesByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "unsubscribesbyflowmessage"
    metric_id = "QqUYDV"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate_by = ["$message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["count"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class PlacedOrdersByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "placedordersbyflowmessage"
    metric_id = "T7RgqW"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatescountbyflowmessage.json"
    aggregate = "unique"
    aggregate_by = ["$attributed_message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                counts = item["measurements"]["unique"]

                for date, count in zip(record["data"]["attributes"]["dates"], counts):
                    result = {"Date": date, "FlowMessage": flow_message, "Count": count}
                    if count == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class CheckoutAmountsByFlowMessageStream(MetricAggregatesStream):
    """Define custom stream"""

    name = "checkoutamountsbyflowmessage"
    metric_id = "T7RgqW"
    primary_keys = ["FlowMessage", "Date"]
    schema_filepath = SCHEMAS_DIR / "metricaggregatessumbyflowmessage.json"
    aggregate = "sum_value"
    aggregate_by = ["$attributed_message"]

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        for record in self.request_records(context):
            for item in record["data"]["attributes"]["data"]:
                flow_message = item["dimensions"][0]
                sum_values = item["measurements"]["sum_value"]

                for date, sum_value in zip(
                    record["data"]["attributes"]["dates"], sum_values
                ):
                    result = {
                        "Date": date,
                        "FlowMessage": flow_message,
                        "Sum": sum_value,
                    }
                    if sum_value == 0:
                        continue
                    transformed_record = self.post_process(result, context)
                    if transformed_record is None:
                        continue
                    yield transformed_record


class OpenEventsStream(KlaviyoStream):
    """Define custom stream."""

    name = "openevents"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"
    schema_filepath = SCHEMAS_DIR / "events.json"

    UTC = timezone.utc
    DEFAULT_START_DATE = datetime(2000, 1, 1, tzinfo=UTC).isoformat()

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
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
        return (
            datetime.strptime(date_string, "%Y-%m-%d").replace(tzinfo=UTC).isoformat()
        )

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}

        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        if self.replication_key:
            filter_timestamp = self.get_starting_replication_key_value(context)
            if filter_timestamp == self.config.get("start_date"):
                filter_timestamp = datetime.now() - relativedelta(days=5)
            if self.is_sorted:
                params["sort"] = self.replication_key

            params["fields[profile]"] = "email"
            params["include"] = "profile"

            params[
                "filter"
            ] = f'equals(metric_id,"P4W93C"),greater-than({self.replication_key},{filter_timestamp})'
        if self.max_page_size:
            params["page[size]"] = self.max_page_size
        return params


class SmsEventsStream(KlaviyoStream):
    """Events for a configurable set of metric IDs (`sms_event_metric_ids`).

    The stream creates one partition per configured metric ID, so each metric
    keeps its own incremental replication bookmark, and the metric filter uses
    the widely supported `equals(metric_id,...)` operator. Profile email and
    phone are resolved from the response's `included` profile resources and
    emitted as top-level columns.
    """

    name = "smsevents"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"
    schema_filepath = SCHEMAS_DIR / "smsevents.json"

    @property
    def partitions(self) -> list[dict] | None:
        return [
            {"metric_id": metric_id}
            for metric_id in self.config.get("sms_event_metric_ids", [])
        ]

    @property
    def is_sorted(self) -> bool:
        return True

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}

        if next_page_token:
            params.update(parse_qsl(next_page_token.query))
            return params

        metric_id = (context or {}).get("metric_id")
        if not metric_id:
            msg = (
                "The smsevents stream requires the sms_event_metric_ids "
                "config option to be a non-empty list of metric IDs."
            )
            raise ValueError(msg)

        filter_timestamp = self.get_starting_replication_key_value(context)
        if filter_timestamp and "T" not in str(filter_timestamp):
            filter_timestamp = _isodate_from_date_string(str(filter_timestamp))

        params["sort"] = self.replication_key
        params["fields[profile]"] = "email,phone_number"
        params["include"] = "profile"
        params["filter"] = (
            f'equals(metric_id,"{metric_id}"),'
            f"greater-than({self.replication_key},{filter_timestamp})"
        )
        if self.max_page_size:
            params["page[size]"] = self.max_page_size
        return params

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        payload = response.json()
        profiles = {
            item["id"]: item.get("attributes") or {}
            for item in payload.get("included") or []
            if item.get("type") == "profile"
        }
        for row in payload.get("data") or []:
            relationship = (row.get("relationships") or {}).get("profile") or {}
            profile_id = (relationship.get("data") or {}).get("id")
            profile_attributes = profiles.get(profile_id) or {}
            row["profile_id"] = profile_id
            row["profile_email"] = profile_attributes.get("email")
            row["profile_phone"] = profile_attributes.get("phone_number")
            yield row

    def post_process(
        self,
        row: dict,
        context: dict | None = None,
    ) -> dict | None:
        row["datetime"] = row["attributes"]["datetime"]
        row["metric_id"] = (context or {}).get("metric_id")
        return row


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
        row["campaign_name"] = row["attributes"]["name"]
        row["status"] = row["attributes"]["status"]
        row["sent_at"] = row["attributes"]["send_time"]
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
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "profiles.json"
    max_page_size = 100

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["created"] = row["attributes"]["created"]
        row["updated"] = row["attributes"]["updated"]
        time.sleep(1)
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
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "flows.json"

    @property
    def partitions(self) -> list[dict] | None:
        return [
            {"filter": "equals(archived,false)"},
            {"filter": "equals(archived,true)"},
        ]

    def get_url_params(self, context, next_page_token):
        url_params = super().get_url_params(context, next_page_token)
        if context:
            parent_filter = url_params.get("filter", "")
            if parent_filter:
                url_params["filter"] = f"and({parent_filter},{context['filter']})"
            else:
                url_params["filter"] = context["filter"]
        return url_params

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        context = context or {}
        context["flow_id"] = record["id"]

        return super().get_child_context(record, context)  # type: ignore[no-any-return]

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


class FlowActionsStream(KlaviyoStream):
    """Define custom stream."""

    name = "flowactions"
    path = "/flows/{flow_id}/flow-actions"
    primary_keys = ["id"]
    replication_key = "updated"
    parent_stream_type = FlowsStream
    schema_filepath = SCHEMAS_DIR / "flowactions.json"
    max_page_size = 50

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        context = context or {}
        context["flow_action_id"] = record["id"]

        return super().get_child_context(record, context)  # type: ignore[no-any-return]

    def post_process(self, row: dict, context: dict) -> dict | None:
        row["flow_id"] = context["flow_id"]
        row["updated"] = row["attributes"]["updated"]
        time.sleep(1)
        return row

    @property
    def is_sorted(self) -> bool:
        return True


class FlowMessagesStream(KlaviyoStream):
    """Define custom stream."""

    name = "flowmessages"
    path = "/flow-actions/{flow_action_id}/flow-messages"
    primary_keys = ["id"]
    replication_key = "updated"
    parent_stream_type = FlowActionsStream
    schema_filepath = SCHEMAS_DIR / "flowmessages.json"
    max_page_size = 50

    def post_process(self, row: dict, context: dict) -> dict | None:
        row["flow_id"] = context["flow_id"]
        row["flow_action_id"] = context["flow_action_id"]
        row["updated"] = row["attributes"]["updated"]
        time.sleep(1)
        return row

    @property
    def is_sorted(self) -> bool:
        return True



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
