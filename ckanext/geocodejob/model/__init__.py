from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import types
from sqlalchemy.engine.reflection import Inspector

from ckan.model.domain_object import DomainObject
from ckan.model.meta import metadata, mapper, Session
from ckan import model

import logging
log = logging.getLogger(__name__)


resource_geocode_data_table = None


def setup():
    # setup resource_geocode_table
    if resource_geocode_data_table is None:
        define_resource_geocode_data_table()
        log.debug('ResourceGeocodeData  table defined in memory')

    if model.resource_table.exists():
        if not resource_geocode_data_table.exists():
            resource_geocode_data_table.create()
            log.debug('ResourceGeocodeData  table create')
        else:
            log.debug('ResourceGeocodeData table already exists')


class GeodataBaseModel(DomainObject):
    @classmethod
    def filter(cls, **kwargs):
        return Session.query(cls).filter_by(**kwargs)

    @classmethod
    def exists(cls, **kwargs):
        if cls.filter(**kwargs).first():
            return True
        else:
            return False

    @classmethod
    def get(cls, **kwargs):
        instance = cls.filter(**kwargs).first()
        return instance

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        Session.add(instance)
        Session.commit()
        return instance.as_dict()


class ResourceGeocodeData(GeodataBaseModel):

    @classmethod
    def get_geocode_data_values(cls, resource_id):
        '''
        Return a list of resource geodata key,values with the passed resource_id.
        '''
        geocode_data_values = {}
        geocode_data_list = \
            Session.query(cls.key,cls.value).filter_by(
                resource_id=resource_id).all()
        for geocode_data_item in geocode_data_list:
            geocode_data_values[geocode_data_item[0]] = geocode_data_item[1]
        return geocode_data_values


def define_resource_geocode_data_table():
    global resource_geocode_data_table

    resource_geocode_data_table = Table(
        'resource_geocode_data', metadata,
        Column('id', types.Integer, primary_key=True),
        Column('resource_id', types.UnicodeText,
               ForeignKey('resource.id',
                          ondelete='CASCADE',
                          onupdate='CASCADE'),
               primary_key=False, nullable=False),
        Column('key', types.UnicodeText,
               primary_key=False, nullable=True),
        Column('value', types.UnicodeText,
               primary_key=False, nullable=True)
    )

    mapper(ResourceGeocodeData, resource_geocode_data_table)
