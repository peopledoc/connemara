import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from psycopg2.extensions import parse_dsn
from connemara.sqlparser.parser import Parser
from connemara.sqlparser.remappers import remap_node, object_creation
from pglast.enums import ObjectType

import subprocess
from collections import defaultdict

from connemara.sqlparser import DDLScript



class SchemaDumper():

    def __init__(self, source_dsn, slot_name=None, keepslot=False,
                 decoder='wal2json'):
        self.source_dsn = source_dsn
        self.dbname = parse_dsn(source_dsn)['dbname']
        self.conn = None
        self.slot_name = slot_name or 'connemara_%s' % self.dbname
        self.keepslot = keepslot
        self.decoder = decoder
        self.statements = []
        self.conn = psycopg2.connect(
                self.source_dsn,
                connection_factory=LogicalReplicationConnection)


    def create_repl_slot(self):
        cur = self.conn.cursor()
        cur.execute("CREATE_REPLICATION_SLOT %s %s LOGICAL %s" % (
                    self.slot_name,
                    'TEMPORARY' if not self.keepslot else '',
                    self.decoder))
        _, self.consistent_point, self.snapshot_name, _ = cur.fetchone()
        cur.close()


    def fetch_objects_in_extensions(self):
        # We don't need more than types and functions for now
        objects_in_extensions = defaultdict(list)
        cur = self.conn.cursor()
        cur.execute("""SET search_path TO ''""")
        cur.execute("""
            SELECT DISTINCT objid::regproc
            FROM pg_depend WHERE deptype = 'e'
            AND classid = 'pg_proc'::regclass
        """)
        objects_in_extensions[ObjectType.OBJECT_FUNCTION] = [
                res[0] for res in cur]
        cur.execute("""
            SELECT DISTINCT objid::regtype
            FROM pg_depend WHERE deptype = 'e'
            AND classid = 'pg_type'::regclass
        """)
        objects_in_extensions[ObjectType.OBJECT_TYPE] = [
                res[0] for res in cur]
        return objects_in_extensions


    def dump(self, ignored_schemas=None):
        ignored_schemas = ignored_schemas or []
        if self.snapshot_name is None:
            raise RuntimeError(
                "Can not perform a dump if we don't have a valid "
                "snapshot name.")

        parser = Parser()
        args = [
                "pg_dump",
                "-d", self.source_dsn,
                "--snapshot=%s" % self.snapshot_name,
                "-s"]
        for schema in ignored_schemas:
            args.append("-N")
            args.append(schema)

        p = subprocess.Popen(args, stdout=subprocess.PIPE)

        with p as proc:
            for line in proc.stdout:
                parser.feed(line.decode('utf8'))
        return DDLScript(parser.statements)
