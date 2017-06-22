from airflow.plugins_manager import AirflowPlugin


def download_s3(bucket, key, output_location):
    """
    TODO - This is not complete yet.
    To be implemented.

    TODO write this as a new proper hook.
    Downloads files from S3 to an output location.
    Wrote this to make use of Boto3 and IAM Roles
    instead of using S3 credentials with Boto2,
    this will be useful in AWS Mumbai like
    newer regions where authentication is
    based on some V4 like API's.
    :param bucket: Bucket name
    :param key: s3 object key
    :param output_location: local temp location.
    :return: True for success, else False.
    """
    try:
        import boto3
        from botocore.client import Config
        from botocore.exceptions import ClientError
        s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
        buck = s3.Bucket(bucket)
        buck.download_file(key, output_location)
        return True
    except Exception, e:
        return False


def to_ist_string(time_in_utc, input_format=None, output_format=None):
    """
    Converts given UTC time to IST, returns string in opted output_format.

    Example :

    In postgres/mysql/hive operator we require a query where

    select * from bookings.hotels where bookingdate >
    '{{ macros.go.to_ist_string(ts) }}'::TIMESTAMP

    The above command will be rendered to

    select * from bookings.hotels where bookingdate >
    '2017-02-02 05:30:00'::TIMESTAMP

    Now you may use this macro inside redshift, bash, docker operators which supports
    jinja templating.

    :param time_in_utc: Datetime String in UTC which adheres to given Input format
    :param input_format: Format for Input time, defaults to '%Y-%m-%dT%H:%M:%S'
    :param output_format: Format for Output time, defaults to '%Y-%m-%d %H:%M:%S'
    :return: String, Datetime in IST timezone.
    """
    from datetime import datetime, timedelta
    if not input_format:
        input_format = '%Y-%m-%dT%H:%M:%S'
    if not output_format:
        output_format = '%Y-%m-%d %H:%M:%S'
    return (datetime.strptime(time_in_utc, input_format) +
            timedelta(hours=5,
                      minutes=30)).strftime(output_format)


def to_utc_string(time_in_ist, input_format=None, output_format=None):
    """
    Very similar to to_ist_string.
    Converts given IST time to UTC, returns string in opted output_format.
    :param time_in_ist: Datetime String in IST which adheres to given Input format
    :param input_format: Format for Input IST time, defaults to '%Y-%m-%dT%H:%M:%S'
    :param output_format: Format for Output IST time, defaults to '%Y-%m-%d %H:%M:%S'
    :return: String, Datetime in UTC timezone.
    """
    from datetime import datetime, timedelta
    if not input_format:
        input_format = '%Y-%m-%dT%H:%M:%S'
    if not output_format:
        output_format = '%Y-%m-%d %H:%M:%S'
    return (datetime.strptime(time_in_ist, input_format) -
            timedelta(hours=5,
                      minutes=30)).strftime(output_format)


class GoAirflow(AirflowPlugin):
    name = "go"
    operators = []
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = [to_utc_string, to_ist_string]
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
