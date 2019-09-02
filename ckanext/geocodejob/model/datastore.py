from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression
from sqlalchemy.orm import sessionmaker

from sqlalchemy import (
    Column,
    UnicodeText,
    DateTime,
    Numeric,
)
from datetime import datetime

from ckanext.datastore.backend.postgres import (
    literal_string,
    get_write_engine,
    get_read_engine,
)

Base = declarative_base()
metadata = Base.metadata


class utcnow(expression.FunctionElement):
    type = DateTime()


@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


def drop_tables():
    metadata.drop_all(get_write_engine())


def create_tables():
    metadata.create_all(get_write_engine())


class GeocodeRequested(Base):
    __tablename__ = 'geocode_request'
    address = Column(UnicodeText, primary_key=True)


class GeocodeCache(Base):
    __tablename__ = 'geocode_cache'
    address = Column(UnicodeText, primary_key=True)
    latitude = Column(Numeric, nullable=True)
    longitude = Column(Numeric, nullable=True)
    timestamp = Column(DateTime, server_default=utcnow(), nullable=False)


def any_requested_rows():
    '''
    return True when the geocode_request table is not empty
    '''
    Session = sessionmaker(bind=get_write_engine())
    return bool(Session().query(GeocodeRequested).first())


def write_session():
    '''
    return a new session for the datastore write user
    '''
    Session = sessionmaker(bind=get_write_engine())
    return Session()


def requested_remove_batch(session, num):
    '''
    Return and remove num rows from geocode_request
    '''
    batch = session.query(GeocodeRequested).limit(num)
    addresses = [b.address for b in batch]
    session.query(GeocodeRequested).filter(
        GeocodeRequested.address.in_(addresses)).delete(
        synchronize_session=False)
    return addresses


def insert_cached_rows(session, rows):
    '''
    Add new rows to geocode_cache
    '''
    session.add_all(
        [
            GeocodeCache(address=a, longitude=x, latitude=y)
            for (a, x, y) in rows
        ]
    )
    session.flush()
