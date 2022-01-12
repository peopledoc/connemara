#!env python3
import argparse
import asyncio
import itertools
import json
import logging
import os
import sys

from pglast.enums import ObjectType, AlterTableType, ConstrType
from pglast.printer import IndentedStream
import psycopg2
from psycopg2.extensions import parse_dsn

from connemara.sqlparser.remappers import remap_node, name_to_fqname, object_creation
from connemara.sqlparser import SchemaRestorer
from connemara.restore import restore_tables
from connemara.schema_dump import SchemaDumper


def logger_setup(level):
    """Setup the logger
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # log to syslog
    frmat = logging.Formatter('connemara_basebackup.py %(levelname)s, %(name)s, %(message)s')
    sysloghdlr = logging.handlers.SysLogHandler(address='/dev/log')
    sysloghdlr.setLevel(logging.INFO)
    sysloghdlr.setFormatter(frmat)
    logger.addHandler(sysloghdlr)

    # log to console
    frmat = logging.Formatter('%(asctime)-15s %(levelname)s, %(name)s, %(message)s')
    stdhandler = logging.StreamHandler(stream=sys.stderr)
    stdhandler.setLevel(level)
    stdhandler.setFormatter(frmat)
    logger.addHandler(stdhandler)
    return logger


CONF_LOCATIONS = [
    os.path.expanduser("~/.config/connemara_basebackup.json"),
    os.path.expanduser("~/.connemara_basebackup.json"),
    "/etc/connemara_basebackup.json"
]


def argparser():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--source",
                           required=True,
                           help="DSN to connect to the replication source")
    argparser.add_argument("--target",
                           required=True,
                           help="DSN to connect to the replication target")
    argparser.add_argument("--slot",
                           required=False,
                           default=None,
                           help="Slot to use for replication")
    argparser.add_argument("--slot-is-temp",
                           default=False,
                           action="store_true",
                           help="Make the replication slot temporary")
    argparser.add_argument("--config-file",
                           required=False,
                           default=None,
                           help="Which config file to use (defaults to %s)" %
                                " or ".join(CONF_LOCATIONS))
    argparser.add_argument("--is-timescaledb",
                           action="store_true",
                           help="Set this flag if working with timescaledb")
    argparser.add_argument("--debug",
                           "-d",
                           default=False,
                           action="store_true",
                           help="Turn on DEBUG level messages")
    return argparser


def load_conf(args):
    config_file = args.config_file
    if config_file is None:
        for possible_location in CONF_LOCATIONS:
            if os.path.exists(possible_location):
                config_file = possible_location
                break
    if config_file is not None:
        with open(config_file) as f:
            conf = json.load(f)
    else:
        conf = {}
    return conf


def should_apply_post_data_stmt(stmt):
    if stmt.node_tag == 'AlterTableStmt':
        # In case of multiple commands, be safe and apply it
        if len(stmt.cmds) != 1:
            return True
        cmd = stmt.cmds[0]
        # Only keep some types of constraints
        if cmd.subtype == AlterTableType.AT_AddConstraint:
            kept_consttypes = (
                    ConstrType.CONSTR_PRIMARY,
                    ConstrType.CONSTR_UNIQUE,
                    ConstrType.CONSTR_FOREIGN)
            return getattr(cmd, 'def').contype.value in kept_consttypes
        if cmd.subtype == AlterTableType.AT_ClusterOn:
            return False
        return True
    if stmt.node_tag == 'IndexStmt':
        if stmt.unique:
            return True
        return False
    # When in doubt, keep it
    return True


if __name__ == '__main__':
    args = argparser().parse_args()
    logger = logger_setup(logging.DEBUG if args.debug else logging.WARNING)
    conf = load_conf(args)

    source_dsn = args.source
    target_dsn = args.target
    slot_name = args.slot
    dumper = SchemaDumper(source_dsn, slot_name=slot_name,
                          keepslot=not args.slot_is_temp)

    # Build the list of objects to ignore.
    # This include every object belonging to an extension,
    # plus any object specifically ignored in the config file.
    objects_in_extensions = dumper.fetch_objects_in_extensions()
    all_objects = list(itertools.chain(*objects_in_extensions.values()))
    for obj in conf.get('ignored_objects', []):
        all_objects.append(obj)
    ignore_whole_schemas = conf.get('ignored_schemas', [])
    ignore_whole_schemas.append('_timescaledb_internal')
    dumper.create_repl_slot()
    ddlscript = dumper.dump(ignored_schemas=ignore_whole_schemas)

    dbname = parse_dsn(source_dsn)['dbname']
    schema_map = {'public': '%s_public' % dbname}
    for schema in ddlscript.objects_created_by_type[ObjectType.OBJECT_SCHEMA]:
        schema_map[schema] = '%s_%s' % (dbname, schema)
    table_mapping = {}
    # Remap the object and keep track of tables
    # This is fine for «small» dumps, but if we ever get gigantic schemas
    # RAM could be an issue.
    # In that case we shouldn't keep the whole dump's parse tree in RAM.
    filtered_statements = []
    for statement in ddlscript.statements:
        obj_creation = object_creation(statement)
        if obj_creation:
            objtype, fqname = obj_creation
            if objtype == ObjectType.OBJECT_SCHEMA:
                schema_name = fqname
            else:
                if '.' in fqname:
                    schema_name, _ = fqname.split('.')
                else:
                    schema_name = 'public'
            if schema_name in ignore_whole_schemas:
                continue
        # Remove SET default_table_access_method
        if statement.node_tag == 'VariableSetStmt':
            if statement.name == 'default_table_access_method':
                continue
        filtered_statements.append(statement)
        if statement.node_tag == 'CreateStmt':
            # This is a create table
            old_table_name = name_to_fqname(statement.relation)
            # Remove accessmethod
            statement.parse_tree.pop('accessMethod', None)
        remap_node(statement, schema_map, all_objects, None, None)
        if statement.node_tag == 'CreateStmt':
            table_mapping[old_table_name] = name_to_fqname(statement.relation)

    ddlscript.statements = filtered_statements
    restorer = SchemaRestorer(target_dsn, ddlscript, schema_map)
    blacklist_objects = []
    if args.is_timescaledb:
        blacklist_objects.append((ObjectType.OBJECT_EXTENSION, "timescaledb"))
    post_data = restorer.restore_schema(
        blacklist_objects=blacklist_objects, failable_objects=all_objects
    )
    # Now, let's copy the table contents.
    restore_tables(args.source, args.target, table_mapping,
                   snapshot_name=dumper.snapshot_name,
                   include_inherited=args.is_timescaledb)
    logger.info("Table content restored, now onto post data objects")
    # Finally, apply «the rest»
    conn = psycopg2.connect(target_dsn)
    cur = conn.cursor()

    if slot_name is None:
        logger.info("No slot name given, don't create a replication_origin")
    else:
        logger.info("Creating replication origin")
        cur.execute("SELECT pg_replication_origin_create(%s)", (slot_name,))
        cur.execute("SELECT pg_replication_origin_advance(%s, %s)",
                    (slot_name, dumper.consistent_point))
    cur.execute("COMMIT")
    logger.info("Restoring indices")

    loop = asyncio.get_event_loop()
    for stmt in post_data:
        loop.run_in_executor(None, create_index, target_dsn, stmt)

    logger.info("Everything restored")
    logger.info("Finished !")


def create_index(target_dsn, stmt):
    conn = psycopg2.connect(target_dsn)
    cur = conn.cursor()
    # The statements have already been remapped. However, we need to
    # filter them.
    if should_apply_post_data_stmt(stmt):
        cur.execute(IndentedStream(expression_level=1)(stmt))
