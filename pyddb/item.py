import json
import logging
from time import time as now
from uuid import uuid4

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError


DELETE_FAIL = 'delete fail'
INSERT_COLLISION = 'insert collision'
SAVE_FAIL = 'save fail'
UPDATE_COLLISION = 'update collision'


class DynamoDBItemException(Exception):
    pass


class DynamoDBItem(object):

    PARTITION_KEY_NAME = ''
    SORT_KEY_NAME = ''
    TABLE_NAME = ''
    TTL_ATTR_NAME = 'ttl_expires'

    def __init__(self, boss, **kwargs):
        if 'ttl_seconds' in kwargs:
            ttl_seconds = kwargs.pop('ttl_seconds')
            # explicity test for None, as zero is a valid value
            if ttl_seconds is not None:
                setattr(self, self.TTL_ATTR_NAME, int(now() + ttl_seconds))
        self.__dict__.update(**kwargs)
        # all non-dynamodb attributes should start with an underscore
        self._boss = boss
        self._table = boss.get_table(self)

    def delete(self):
        resp = self._table.delete_item(Key=self.key)
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            log = logging.getLogger('DynamoDBItem.delete')
            dat = {'resp': resp, 'table': self.TABLE_NAME, 'key': self.key}
            log.error('%s: %s' % (DELETE_FAIL, json.dumps(dat)))
            raise DynamoDBItemException(DELETE_FAIL)
        return resp

    @property
    def item_dict(self):
        item_dict = dict(self.__dict__)
        # remove all non-dynamodb attributes (those that start with an
        #   underscore) from the item_dict; iterate a list() of the dict keys
        #   because we are going to delete some of the keys from the dict
        for k in list(item_dict.keys()):
            if k[0] == '_':
                del item_dict[k]
        # hook for child classes to remove any other desired attributes
        self._prune_non_persistent_attributes(item_dict)
        return item_dict

    @property
    def key(self):
        key = {self.PARTITION_KEY_NAME: getattr(self, self.PARTITION_KEY_NAME)}
        if self.SORT_KEY_NAME:
            key[self.SORT_KEY_NAME] = getattr(self, self.SORT_KEY_NAME)
        return key

    def _prune_non_persistent_attributes(self, item):
        """
        Override this method for child classes that store non-persistent
            or non-persistable attributes in the instance and need to
            remove them from the item dict before storing the item in
            DynamoDB.
        """
        pass

    def save(self):
        old_revtag = getattr(self, 'revtag', None)
        self.revtag = str(uuid4())
        kwargs = {
            'Item': self.item_dict,
            'ConditionExpression': Attr('revtag').eq(old_revtag) \
                if old_revtag \
                else Attr('revtag').not_exists()
            }
        try:
            resp = self._table.put_item(**kwargs)
            if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                log = logging.getLogger('DynamoDBItem.save')
                dat = {'resp': resp, 'table': self.TABLE_NAME,
                                        'item': kwargs['Item']}
                log.error('%s: %s' % (SAVE_FAIL, json.dumps(dat)))
                raise DynamoDBItemException(SAVE_FAIL)
            return resp
        except ClientError as ex:
            if ex.response['Error']['Code'] \
                                    == 'ConditionalCheckFailedException':
                log = logging.getLogger('DynamoDBItem.save')
                dat = {'table': self.TABLE_NAME, 'item': kwargs['Item']}
                jdat = json.dumps(dat)
                if old_revtag:
                    log.warning('%s: %s' % (UPDATE_COLLISION, jdat))
                    raise DynamoDBItemException(UPDATE_COLLISION)
                else:
                    log.error('%s: %s' % (INSERT_COLLISION, jdat))
                    raise DynamoDBItemException(INSERT_COLLISION)
            raise


