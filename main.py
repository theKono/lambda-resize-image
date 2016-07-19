#!/usr/bin/env python

# standard library imports
from contextlib import closing
from copy import deepcopy
import logging
import mimetypes
import os.path
import re
from tempfile import NamedTemporaryFile
import urllib

# third party related imports
import boto3
from imgutil import imgresize, imgoptimize
from PIL import Image
from retrying import retry

# local library imports
from config import Config


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ResizePolicy(object):

    DEFAULT = 'DEFAULT'
    ONLY_SHRINK = 'ONLY_SHRINK'


def download_s3_object(bucket, key_name, output_name):

    logger.info('bucket: %s, key_name: %s, output: %s', bucket, key_name, output_name)
    boto3.resource('s3').Bucket(bucket).download_file(key_name, output_name)


def find_config_objects(bucket, key_name):

    ret = []

    for c in Config.setting:
        if 'bucket' not in c or c['bucket'] != bucket:
            continue

        if 'key_regexp' not in c or len(c.get('outputs', [])) == 0:
            continue

        match_obj = re.match(c['key_regexp'], key_name)

        if match_obj is None:
            continue

        c = deepcopy(c)

        for output in c['outputs']:
            if 'bucket' not in output:
                output['bucket'] = bucket

            output['key'] = output['key'].format(*match_obj.groups())

        ret.append(c)

    return ret


def resize(filename, resize_policy, width, output_f):

    if resize_policy == ResizePolicy.ONLY_SHRINK:
        if width >= Image.open(filename).size[0]:
            with closing(open(filename, 'rb')) as f:
                output_f.write(f.read())
                output_f.flush()
                return

    imgresize(filename, width=width, output_filename=output_f.name)


def get_put_object_params(attrs):

    if attrs is None:
        return {}

    permitted = (
        'ACL',
        'CacheControl',
        'ContentDisposition',
        'ContentEncoding',
        'ContentLanguage',
        'ContentLength',
        'ContentMD5',
        'ContentType',
        'Expires',
        'GrantFullControl',
        'GrantRead',
        'GrantReadACP',
        'GrantWriteACP',
        'Metadata',
        'ServerSideEncryption',
        'StorageClass',
        'WebsiteRedirectLocation',
        'SSECustomerAlgorithm',
        'SSECustomerKey',
        'SSEKMSKeyId',
        'RequestPayer'
    )
    ret = {}

    for key in attrs:
        if key in permitted:
            ret[key] = attrs[key]

    return ret


def guess_mimetype(filename):

    ret = {}
    content_type, encoding = mimetypes.guess_type(filename)

    if content_type is not None:
        ret['ContentType'] = content_type

    if encoding == 'gzip':
        ret['ContentEncoding'] = 'gzip'

    return ret


@retry(stop_max_attempt_number=10)
def upload(filename, setting):

    if setting.get('region') is not None:
        s3 = boto3.client('s3', region_name=setting['region'])
    else:
        s3 = boto3.client('s3')

    args = guess_mimetype(filename)
    extra_args = get_put_object_params(setting.get('attr'))

    for key in args:
        if key not in extra_args:
            extra_args[key] = args[key]

    logger.info(
        'Upload s3://%s/%s, args = %s',
        setting['bucket'],
        setting['key'],
        extra_args
    )
    s3.upload_file(
        filename,
        setting['bucket'],
        setting['key'],
        ExtraArgs=extra_args
    )


def process(filename, setting):

    if setting.get('width') is None:
        raise ValueError('Unknown width')

    width = setting['width']
    resize_policy = setting.get('policy', ResizePolicy.DEFAULT)
    _, ext = os.path.splitext(filename)

    with closing(NamedTemporaryFile(suffix=ext)) as resized_name:
        resize(filename, resize_policy, width, resized_name)
        logger.info('resized')

        imgoptimize(resized_name.name)
        logger.info('optimized')

        upload(resized_name.name, setting)
        logger.info('uploaded')


def setup_path():

    cwd = os.path.abspath(os.path.curdir)
    os.environ['PATH'] = cwd + os.pathsep + os.environ['PATH']


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key_name = event['Records'][0]['s3']['object']['key']
    key_name = urllib.unquote_plus(key_name).decode('utf8')

    settings = find_config_objects(bucket, key_name)
    logger.info(settings)
    if len(settings) == 0:
        logger.warn('Cannot find any config object of `%s`', key_name)
        return

    setup_path()

    raised_exc = None
    _, ext = os.path.splitext(key_name)
    with closing(NamedTemporaryFile(suffix=ext)) as original_file:
        download_s3_object(bucket, key_name, original_file.name)
        logger.info('Download s3 object')

        for setting in settings:
            for output_setting in setting['outputs']:
                try:
                    process(original_file.name, output_setting)
                except Exception as e:
                    logger.exception(e)
                    raised_exc = e

    if raised_exc is not None:
        raise raised_exc
