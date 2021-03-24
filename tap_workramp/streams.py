import datetime
import pytz
import singer

from singer.utils import strftime as singer_strftime


LOGGER = singer.get_logger()


# NOTE: this API doesn't support any date filtering or sorting of results
# as such each of these tables will be synced in full/no states will be written

class Stream():
    name = None
    replication_method = None
    key_properties = None
    stream = None
    datetime_fields = None
    url = None

    def __init__(self, client=None, start_date=None):
        self.client = client
        if start_date:
            self.start_date = start_date
        else:
            self.start_date = datetime.datetime.min.strftime('%Y-%m-%d')

    def is_selected(self):
        return self.stream is not None

    def transform_value(self, key, value):
        if key in self.datetime_fields and value:
            value = datetime.datetime.fromtimestamp(value/1000, pytz.utc)
            # reformat to use RFC3339 format
            value = singer_strftime(value)

        return value


class Paths(Stream):
    name = 'paths'
    replication_method = 'FULL_TABLE'
    key_properties = ['id']
    replication_key = 'updatedAt'
    datetime_fields = set([
        'createdAt', 'updatedAt'
    ])
    url = 'paths'

    def sync(self, state):

        assignments_stream = PathAssignments(self.client)

        # paths are retrieved in no particular order, with no date filtering
        for row in self.client.get(self.url):
            path = {k: self.transform_value(k, v) for (k, v) in row.items()}
            yield(self.stream, path)
            if assignments_stream.is_selected():
                yield from assignments_stream.sync(path['id'])


class PathAssignments(Stream):
    name = 'path_assignments'
    replication_method = 'FULL_TABLE'
    key_properties = ['id']
    replication_key = 'updatedAt'
    url = 'paths/{}/assignments'
    datetime_fields = set([
        'createdAt', 'updatedAt', 'dueDate'
    ])

    def sync(self, path_id):
        url = self.url.format(path_id)
        for row in self.client.get(url):
            assignment = {k: self.transform_value(k, v) for (k, v) in row.items()}
            yield(self.stream, assignment)


class Guides(Stream):
    name = 'guides'
    replication_method = "FULL_TABLE"
    key_properties = ['id']
    url = 'guides'
    datetime_fields = set([
        'createdAt', 'updatedAt'
    ])

    def sync(self, state):

        assignments_stream = GuideAssignments(self.client)

        # guides are retrieved in no particular order, with no date filtering
        for row in self.client.get(self.url):
            guide = {k: self.transform_value(k, v) for (k, v) in row.items()}
            yield(self.stream, guide)
            if assignments_stream.is_selected():
                yield from assignments_stream.sync(guide['id'])


class GuideAssignments(Stream):
    name = 'guide_assignments'
    replication_method = 'FULL_TABLE'
    key_properties = ['id']
    replication_key = 'updatedAt'
    url = 'guides/{}/assignments'
    datetime_fields = set([
        'createdAt', 'updatedAt', 'dueDate', 'completedAt'
    ])

    def sync(self, guide_id):
        url = self.url.format(guide_id)
        for row in self.client.get(url):
            assignment = {k: self.transform_value(k, v) for (k, v) in row.items()}
            yield(self.stream, assignment)


class Users(Stream):
    name = 'users'
    replication_method = 'FULL_TABLE'
    key_properties = 'id'
    url = 'users'
    datetime_fields = set(['createdAt', 'updatedAt'])

    def sync(self, sync_thru):
        for row in self.client.get(self.url):
            record = {k: self.transform_value(k, v) for (k, v) in row.items()}
            yield(self.stream, record)


STREAMS = {
    "guides": Guides,
    "guide_assignments": GuideAssignments,
    "paths": Paths,
    "path_assignments": PathAssignments,
    "users": Users
}
