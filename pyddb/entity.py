from uuid import uuid4

from .helpers import EZQuery
from .item import DynamoDBItem


entity_item_classes = {}


def register_entity_item(entity_item_class):
    entity_item_classes[entity_item_class.ITEM_CLASS_NAME] = entity_item_class
    return entity_item_class


class EntityItem(DynamoDBItem):

    ENTITY_ITEM_CLASS_NAME = 'entity'
    INDICES = {('entity_key', 'owner_key'): 'entity_key-owner_key-index'}
    ITEM_CLASS_NAME = 'entity'
    PARTITION_KEY_NAME = 'owner_key'
    SORT_KEY_NAME = 'entity_key'

    def __init__(self, boss, owner_key=None, entity_key=None, **kwargs):
        super().__init__(boss, **kwargs)
        self.item_class_name = self.ITEM_CLASS_NAME
        key = self.__class__.make_entity_key(self.generate_id())
        self.owner_key = owner_key if owner_key else key
        self.entity_key = entity_key if entity_key else key

    @property
    def entity_class_name(self):
        return self.entity_key.split(':')[0]

    @property
    def _entity_id(self):
        return ':'.join(self.entity_key.split(':')[1:])

    def generate_id(self):
        return str(uuid4())

    @classmethod
    def get_index_name(self, partition_attr, sort_attr=None):
        return self.INDICES.get((partition_attr, sort_attr))

    @classmethod
    def make_entity_key(self, id):
        return '%s:%s' % (self.ENTITY_ITEM_CLASS_NAME, id)

    @property
    def owner_entity_class_name(self):
        return self.owner_key.split(':')[0]

    @property
    def _owner_id(self):
        return ':'.join(self.owner_key.split(':')[1:])


def _make_item(boss, item_dict):
    return boss.item_factory(
        entity_item_classes[item_dict['item_class_name']],
        item_dict
        )


def _make_items(boss, item_dict_list, sort_key_lambda=None, reverse=False):
    result = [_make_item(boss, item_dict) for item_dict in item_dict_list]
    if sort_key_lambda:
        result.sort(key=sort_key_lambda, reverse=reverse)
    return result


def get_items(boss, item_base_class, key_attr, key, range_attr=None,
            range_item_class_name=None, sort_key_lambda=None, reverse=False):
    tbl = boss.get_table(item_base_class)
    kwargs = {
        'index_name': item_base_class.get_index_name(key_attr, range_attr),
        'reverse': reverse
        }
    if range_attr and range_item_class_name:
        kwargs['range_attr'] = range_attr
        kwargs['op'] = 'begins_with'
        kwargs['range'] = '%s:' % range_item_class_name
    items = EZQuery(tbl, key_attr, key, **kwargs)
    return _make_items(boss, items, sort_key_lambda, reverse)


def get_owned_items(boss, item_base_class, owner_key,
            owned_item_class_name=None, sort_key_lambda=None, reverse=False):
    return get_items(boss, item_base_class, 'owner_key', owner_key,
                'entity_key', owned_item_class_name, sort_key_lambda, reverse)


def get_owner_items(boss, item_base_class, entity_key,
            owner_item_class_name=None, sort_key_lambda=None, reverse=False):
    return get_items(boss, item_base_class, 'entity_key', entity_key,
                'owner_key', owner_item_class_name, sort_key_lambda, reverse)


