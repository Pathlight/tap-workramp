import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from .client import WorkrampAPI
from .streams import STREAMS
from .sync import sync_stream


REQUIRED_CONFIG_KEYS = ["access_token", "start_date"]
SUB_STREAMS = {
    'paths': ['path_assignments'],
    'guides': ['guide_assignments']
}
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def get_selected_streams(catalog):
    selected_stream_names = []
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        if stream_is_selected(mdata):
            selected_stream_names.append(stream.tap_stream_id)
    return selected_stream_names


def get_sub_stream_names():
    sub_stream_names = []
    for parent_stream in SUB_STREAMS:
        sub_stream_names.extend(SUB_STREAMS[parent_stream])
    return sub_stream_names


class DependencyException(Exception):
    pass


def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")
    for parent_stream_name in SUB_STREAMS:
        sub_stream_names = SUB_STREAMS[parent_stream_name]
        for sub_stream_name in sub_stream_names:
            if sub_stream_name in selected_stream_ids and parent_stream_name not in selected_stream_ids:
                errs.append(msg_tmpl.format(sub_stream_name, parent_stream_name))

    if errs:
        raise DependencyException(" ".join(errs))


def populate_class_schemas(catalog, selected_stream_names):
    for stream in catalog.streams:
        if stream.tap_stream_id in selected_stream_names:
            STREAMS[stream.tap_stream_id].stream = stream


def do_sync(client, catalog, state, config):
    start_date = config['start_date']

    selected_stream_names = get_selected_streams(catalog)
    validate_dependencies(selected_stream_names)
    populate_class_schemas(catalog, selected_stream_names)
    all_sub_stream_names = get_sub_stream_names()

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id
        if stream_name not in selected_stream_names:
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        sub_stream_names = SUB_STREAMS.get(stream_name)

        # parent stream will sync sub stream
        if stream_name in all_sub_stream_names:
            continue

        singer.write_schema(
            stream_name,
            stream.schema.to_dict(),
            stream.key_properties
        )

        if sub_stream_names:
            for sub_stream_name in sub_stream_names:
                if sub_stream_name not in selected_stream_names:
                    continue
                sub_instance = STREAMS[sub_stream_name]
                sub_stream = STREAMS[sub_stream_name].stream
                sub_stream_schema = sub_stream.schema.to_dict()
                singer.write_schema(
                    sub_stream.tap_stream_id,
                    sub_stream_schema,
                    sub_instance.key_properties
                )

        LOGGER.info("%s: Starting sync", stream_name)
        instance = STREAMS[stream_name](client, start_date)
        counter_value = sync_stream(state, start_date, instance, config)
        singer.write_state(state)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    singer.write_state(state)
    LOGGER.info("Finished sync")


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        key_properties = ['id']
        valid_replication_keys = ['updatedAt']
        stream_metadata = metadata.get_standard_metadata(
            schema=schema.to_dict(),
            key_properties=key_properties,
            valid_replication_keys=valid_replication_keys,
            replication_method=None
        )
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        client = WorkrampAPI(args.config)
        do_sync(client, catalog, args.state, args.config)


if __name__ == "__main__":
    main()
