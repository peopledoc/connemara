"""
This module provides several utility classes related to SQLParsing
"""

import re
import logging
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
import uuid

import psycopg2

from connemara.sqlparser.parser import Parser as SqlParser
from connemara.sqlparser.remappers import object_creation, remap_node
from pglast import Node
from pglast.enums import AlterTableType, ObjectType
from pglast.printer import IndentedStream

from . import pglast_overrides  # pylint: disable=unused-import


class DDLScript():
    """
    A class representing a DDLScript.

    Args:
        statements (iterable): an iterable of `pglast.Node` instances
        comments (iterable): an iterable of strings representing comments
        sql_file (pathlib.Path): a `pathlib.Path` instance pointing to the
            original SQL file being parsed

    Attributes:
        statements (iterable): an iterable of `pglast.Node` instances
        comments (iterable): an iterable of strings representing comments
        sql_file (pathlib.Path): a `pathlib.Path` instance pointing to the
            original SQL file being parsed

    Example usage:

    >>> script = DDLScript.load_from_file(pathlib.Path('/path/to/file.sql'))
    >>> for stmt in script.statements:
    >>      print(stmt)
    """

    _ATTRS_ = tuple()

    def __init__(self, statements, comments=None, sql_file=None):
        self.statements = list(statements)
        self.comments = list(comments) if comments else []
        self.sql_file = sql_file

    @property
    def objects_created_by_type(self):
        """
        Returns:
            dict: a dictionary mapping `pglast.enums.ObjectType` types to the
            list of fully qualified names of objects of this type that the
            script would create.
        """
        objects_created_by_type = defaultdict(list)

        for statement in self.statements:
            created_object = object_creation(statement)

            if created_object:
                objtype, fqn = created_object
                objects_created_by_type[objtype].append(fqn)

        return objects_created_by_type

    @classmethod
    def load_from_file(cls, sqlfile):
        """
        Arguments:
            sqlfile (`pathlib.Path`): The path to the sqlfile to parse.

        Returns:
            DDLScript: a ddlscript instance representing the given sqlfile.
        """
        parser = SqlParser()
        with sqlfile.open() as f:
            for line in f:
                parser.feed(line)

        return cls(parser.statements, parser.comments, sqlfile)

    @property
    def string_statements(self):
        """
        Returns:
            generator: a generator deparsing the `pglast.Node`
            representing staements to their SQL string representation.
        """
        for statement in self.statements:
            yield IndentedStream()(statement)

    def __str__(self):
        return ";\n".join(self.string_statements)

    def translate(self, schema_map, default=None, ignored_objects=None):
        r"""
        Translate the given ddlscript by replacing schemas in every statement
        by its corresponding entry in schema_map, returning a new DDLScript
        instance.

        >>> script = DDLScript.load_from_file(pathlib.Path('test1.sql'))
        >>> translated = script.translate(schema_map={'schema1': 'schema2'})
        >>> print(list(translated.string_statements))
            ['SELECT *\nFROM schema2.table1']


        Arguments:
          schema_map (dict): a dictionary mapping schema names to another
              schema name with which it should be replaced.
          default (str): a string specifying which schema should we use when
              an object is unqualified in the original script. Defaults to
              None, meaning that the schema should stay unqualified.
          ignored_objects (list): a list of fully qualified object names for
              which no remapping should take place.

        Returns:
           DDLSCript: a new DDLScript instance built from the translated
           statements.

        """
        ignored_objects = ignored_objects or []
        translated_statements = []

        for statement in self.statements:
            node = Node({statement.node_tag: deepcopy(statement.parse_tree)},
                        parent=statement.parent_node)
            remap_node(node, schema_map, ignored_objects, default)
            translated_statements.append(node)
        inst = self.__class__(translated_statements, self.comments)

        for attr in self.__class__._ATTRS_:
            setattr(inst, attr, getattr(self, attr, None))

        return inst


class SchemaRestorer():
    """
    Restores a schema, ignoring certain objects in the process.

    Args:
      target_dsn (str): DNS to connect to the database where the restoration
          should take place
      ddlscript: (`connemara.sqlparser.DDLScript`): DDLScript to restore the
          schema.
      schema_map: (dict): Mapping between old schema names and new ones, used
          mostly to ensure the new schemas are created in the target DB.
    """

    def __init__(self, target_dsn, ddlscript, schema_map):
        self.target_dsn = target_dsn
        self.schema_map = schema_map
        self.ddlscript = ddlscript

    def _create_missing_schemas(self, cursor):
        objects_created_by_type = self.ddlscript.objects_created_by_type
        created_schemas = set(objects_created_by_type[ObjectType.OBJECT_SCHEMA])
        missing_schemas = set(self.schema_map.values()) - created_schemas

        for schema in missing_schemas:
            cursor.execute('CREATE SCHEMA IF NOT EXISTS %s;' % schema)

    def restore_schema(self, blacklist_objects=None, failable_objects=None):
        """
        Restore the DDLScript in the target DB.

        Arguments:
          blacklist_objects (list): a list of fully qualified object names
            that we shouldn't restore.

          failable_objects (list): a list of objects for which we ignore failures
            during restoration.

        Returns:
          list: a list of statements to execute after data has been restored.
          This includes table constraints and indexes.

        The restoration only restores table definitions, and specifically
        ignore triggers, event triggers, object privileges, views, and RLS.
        """
        target_conn = psycopg2.connect(self.target_dsn)
        cursor = target_conn.cursor()
        cursor.execute("BEGIN")
        self._create_missing_schemas(cursor)
        blacklist_objects = blacklist_objects or []
        failable_objects = failable_objects or []
        # Skip certain kind of statements: we are not interested in non-unique
        # indexes, nor in triggers
        post_data = []

        for statement in self.ddlscript.statements:
            if statement.node_tag in ('CreateTrigStmt',
                                      'CreateEventTrigStmt',
                                      'GrantStmt',
                                      'AlterDefaultPrivilegesStmt',
                                      'CreatePolicyStmt',
                                      'CommentStmt',
                                      'CreateCastStmt',
                                      'AlterOwnerStmt'):

                continue

            if statement.node_tag == 'IndexStmt':
                post_data.append(statement)

                continue

            if statement.node_tag == 'AlterTableStmt':
                # Ignore ALTER INDEX statement altogether
                if statement.relkind == ObjectType.OBJECT_INDEX:
                    continue

                # We assume that if we have an add constraint command, it's
                # the only one. This is safe because we only expect to work
                # on pg_dump outputs here.
                if any(cmd.subtype.value == AlterTableType.AT_AddConstraint
                       for cmd in statement.cmds):
                    post_data.append(statement)

                    continue

                if any(cmd.subtype.value == AlterTableType.AT_ClusterOn
                        for cmd in statement.cmds):
                    post_data.append(statement)

                    continue

                # Just ignore ALTER TABLE .. OWNER TO
                if any(cmd.subtype.value == AlterTableType.AT_ChangeOwner
                        for cmd in statement.cmds):

                    continue

                # Just ignore ALTER TABLE .. ROW LEVEL SECURITY
                if any(cmd.subtype.value == AlterTableType.AT_EnableRowSecurity
                        for cmd in statement.cmds):

                    continue

                # Just ignore ALTER TABLE ..SET  REPLICA FULL
                if any(cmd.subtype.value == AlterTableType.AT_ReplicaIdentity
                        for cmd in statement.cmds):

                    continue

            str_statement = IndentedStream(expression_level=1)(statement)

            obj_creation_tuple = object_creation(statement)
            obj_creation = None
            if obj_creation_tuple:
                obj_creation = obj_creation_tuple[1]

            if obj_creation in blacklist_objects:
                logging.debug("Ignore %s" % str_statement)

                continue

            savepoint_name = None
            if obj_creation in failable_objects:
                savepoint_name = 'svpt_' + uuid.uuid4().hex
                cursor.execute("SAVEPOINT %s" % savepoint_name)

            logging.debug("Restoring %s" % str_statement)
            try:
                cursor.execute(str_statement)
            except psycopg2.Error:
                if savepoint_name:
                    logging.debug("Rollback to savepoint for %s", str_statement)
                    cursor.execute("ROLLBACK TO SAVEPOINT %s" % savepoint_name)
                else:
                    raise

        cursor.execute("COMMIT")

        return post_data
