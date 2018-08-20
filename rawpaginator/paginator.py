import re
from django.core.paginator import Page, Paginator as DefaultPaginator
from django.db.models.query import RawQuerySet
from django.db import connections


class DatabaseNotSupportedException(Exception):
    pass


class RawQuerySetPaginator(DefaultPaginator):
    "An efficient paginator for RawQuerySets."
    _count = None

    def __init__(self, object_list, per_page, orphans=0, allow_empty_first_page=True):
        super(RawQuerySetPaginator, self).__init__(object_list, per_page, orphans, allow_empty_first_page)
        self.raw_query_set = self.object_list
        self.connection = connections[self.raw_query_set.db]

    def _get_count(self):
        if self._count is None:
            cursor = self.connection.cursor()
            raw_query = self.raw_query_set.raw_query
            if self.connection.vendor.lower() == 'microsoft':
                raw_query = re.sub('ORDER\s+BY.*', '', raw_query, flags=re.IGNORECASE)
            count_query = 'SELECT COUNT(*) FROM (%s) AS sub_query_for_count' % raw_query
            cursor.execute(count_query, self.raw_query_set.params)
            self._count = cursor.fetchone()[0]

        return self._count
    count = property(_get_count)

    # mysql, postgresql, and sqlite can all use this syntax
    def _get_limit_offset_query(self, limit, offset):
        return '''SELECT * FROM (%s) as sub_query_for_pagination
                LIMIT %s OFFSET %s''' % (self.raw_query_set.raw_query, limit, offset)

    mysql_getquery = _get_limit_offset_query
    postgresql_getquery = _get_limit_offset_query
    sqlite_getquery = _get_limit_offset_query

    # Get the oracle query, but check the version first
    # Query is only supported in oracle version >= 12.1
    # TODO:TESTING
    def oracle_getquery(self, limit, offset):
        major_version, minor_version = self.connection.oracle_version[0:2]
        if major_version < 12 or (major_version == 12 and minor_version < 1):
            raise DatabaseNotSupportedException('Oracle version must be 12.1 or higher')
        return '''SELECT * FROM (%s) as sub_query_for_pagination
                  OFFSET %s ROWS FETCH NEXT %s ROWS ONLY''' % (self.raw_query_set.raw_query, offset, limit)

    def firebird_getquery(self, limit, offset):  # TODO:TESTING
        return '''SELECT FIRST %s SKIP %s *
                FROM (%s) as sub_query_for_pagination''' % (limit, offset, self.raw_query_set.raw_query)

    def microsoft_getquery(self, limit, offset):
        order_by_match = re.search('(ORDER\s+BY.*)', self.raw_query_set.raw_query, flags=re.IGNORECASE)
        if order_by_match:
            order_by = order_by_match.groups()[0]
        else:
            order_by = 'ORDER BY id'
        raw_query = re.sub('ORDER\s+BY.*', '', self.raw_query_set.raw_query, flags=re.IGNORECASE)
        return '''SELECT t2.*
                FROM (
                    SELECT ROW_NUMBER() OVER(%s) AS row, t1.*
                    FROM (%s) t1
                ) t2
                WHERE t2.row BETWEEN %s+1 AND %s+%s;''' % (order_by, raw_query, offset, offset, limit)

    def page(self, number):
        number = self.validate_number(number)
        offset = (number - 1) * self.per_page
        limit = self.per_page
        if offset + limit + self.orphans >= self.count:
            limit = self.count - offset
        database_vendor = self.connection.vendor

        try:
            query_with_limit = getattr(self, '%s_getquery' % database_vendor)(limit, offset)
        except AttributeError:
            raise DatabaseNotSupportedException('%s is not supported by RawQuerySetPaginator' % database_vendor)

        return Page(list(self.raw_query_set.model.objects.raw(query_with_limit, self.raw_query_set.params)), number, self)


class PaginatorFactory(type):
    def __init__(cls, name, bases, nmspc):
        pass

    def __call__(cls, object_list, per_page, orphans=0, allow_empty_first_page=True):
        if isinstance(object_list, RawQuerySet):
            return RawQuerySetPaginator(object_list, per_page, orphans=orphans, allow_empty_first_page=allow_empty_first_page)
        else:
            return DefaultPaginator(object_list, per_page, orphans=orphans, allow_empty_first_page=allow_empty_first_page)


class Paginator(metaclass=PaginatorFactory):
    __metaclass__ = PaginatorFactory
