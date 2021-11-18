from uuid import uuid4

from .item import DynamoDBItem


class EntityItem(DynamoDBItem):

    ENTITY_KEY_PREFIX = 'entity'
    INDICES = {('entity_key', 'owner_key'): 'entity_key-owner_key-index'}
    ITEM_CLASS_NAME = 'entity'
    PARTITION_KEY_NAME = 'owner_key'
    SORT_KEY_NAME = 'entity_key'

    def __init__(self, boss, owner_key=None, entity_key=None, **kwargs):
        super().__init__(boss, **kwargs)
        key = self.__class__.make_entity_key(self.generate_id())
        self.owner_key = owner_key if owner_key else key
        self.entity_key = entity_key if entity_key else key

    @property
    def entity_class_name(self):
        return self.entity_key.split(':')[0]

    @property
    def entity_id(self):
        return ':'.join(self.entity_key.split(':')[1:])

    def generate_id(self):
        return str(uuid4())

    @classmethod
    def make_entity_key(self, id):
        return '%s:%s' % (self.ENTITY_KEY_PREFIX, id)

    @property
    def owner_entity_class_name(self):
        return self.owner_key.split(':')[0]

    @property
    def owner_id(self):
        return ':'.join(self.owner_key.split(':')[1:])


def get_owned_items(boss, item_base_class, owner_key,
            owned_entity_key_prefix=None, sort_key_lambda=None, reverse=False):
    return boss.get_items(item_base_class, 'owner_key', owner_key,
                'entity_key', owned_entity_key_prefix, sort_key_lambda, reverse)


def get_owner_items(boss, item_base_class, entity_key,
            owner_entity_key_prefix=None, sort_key_lambda=None, reverse=False):
    return boss.get_items(item_base_class, 'entity_key', entity_key,
                'owner_key', owner_entity_key_prefix, sort_key_lambda, reverse)
