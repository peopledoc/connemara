"""
This module contains everything to remap SQL statements to another schema.

The node_remapper decorator is used ot register remappers for a type of SQL
Node.

"""

from pglast import parse_sql, Node
from pglast.printer import IndentedStream
from pglast.node import Scalar, List
from pglast.enums import ObjectType
import pglast.node
import logging
import logging.config

logger = logging.getLogger('connemara_remapper')

NODE_REMAPPERS = {}


def object_creation(statement):
    """
    If this statement creates an object, return a tuple in the form of
    (object_type, object_name), else return None.
    """
    if statement.node_tag in (
            'VariableSetStmt',
            'SelectStmt',
            'AlterDefaultPrivilegesStmt',
            'AlterOwnerStmt',
            'AlterTableStmt',
            'AlterSeqStmt',
            'CommentStmt',
            'TransactionStmt',
            'InsertStmt',
            'UpdateStmt',
            'VacuumStmt',
            'DropStmt',
            'DoStmt',
            'TruncateStmt',
            'CreateCastStmt',
            'AlterEnumStmt',
            'GrantRoleStmt',
            'GrantStmt'):
        return None
    if statement.node_tag == 'CreateSchemaStmt':
        return (ObjectType.OBJECT_SCHEMA, statement.schemaname.value)
    if statement.node_tag == 'CreateExtensionStmt':
        return (ObjectType.OBJECT_EXTENSION, statement.extname.value)
    if statement.node_tag == 'CreateEnumStmt':
        return (ObjectType.OBJECT_TYPE, name_to_fqname(statement.typeName))
    if statement.node_tag == 'CreateFunctionStmt':
        return (ObjectType.OBJECT_FUNCTION, name_to_fqname(statement.funcname))
    if statement.node_tag == 'CreateStmt':
        return (ObjectType.OBJECT_TABLE, name_to_fqname(statement.relation))
    if statement.node_tag == 'ViewStmt':
        return (ObjectType.OBJECT_VIEW, name_to_fqname(statement.view))
    if statement.node_tag == 'CreateSeqStmt':
        return (ObjectType.OBJECT_SEQUENCE, name_to_fqname(statement.sequence))
    if statement.node_tag == 'IndexStmt':
        return (ObjectType.OBJECT_INDEX, statement.idxname.value)
    if statement.node_tag == 'CreateTrigStmt':
        return (ObjectType.OBJECT_TRIGGER, statement.trigname.value)
    if statement.node_tag == 'CreateEventTrigStmt':
        return (ObjectType.OBJECT_EVENT_TRIGGER, statement.trigname.value)
    if statement.node_tag == 'CreatePolicyStmt':
        return (ObjectType.OBJECT_POLICY, statement.policy_name.value)
    if statement.node_tag == 'CompositeTypeStmt':
        return (ObjectType.OBJECT_TYPE, name_to_fqname(statement.typevar))
    if statement.node_tag == 'CreateTableAsStmt':
        return (ObjectType.OBJECT_TYPE, name_to_fqname(statement.into.rel))
    raise NotImplementedError(
            "Object creation detection not implemented for %s" %
            statement.node_tag)


def name_to_fqname(name):
    """
    Arguments:
        name (object): something looking like a name, can be either a
          `pglast.node.List` in which case it's items represents a schema name
          and a relation name, or a RangeVar.

    Returns:
        str: fully qualified name in the form schema.object.
    """
    if isinstance(name, pglast.node.List):
        return ".".join(a.string_value for a in name)
    if name.node_tag == 'RangeVar':
        if name.schemaname:
            return '"%s"."%s"' % (name.schemaname.value, name.relname.value)
        else:
            return name.relname.value
    return name


def node_remapper(*node_tags):
    """
    Decorator registering a node_remapper.
    The implementation mirros the one done in pglast for printers.
    """
    def decorator(impl):
        if len(node_tags) == 1:
            parent_tags = (None,)
            tag = node_tags[0]
        elif len(node_tags) == 2:
            parent_tags, tag = node_tags
            if not isinstance(parent_tags, (list, tuple)):
                parent_tags = (parent_tags,)
        else:
            raise ValueError("Must specify one or two tags, got %d instead" %
                             len(node_tags))

        for parent_tag in parent_tags:
            t = tag if parent_tag is None else (parent_tag, tag)
            NODE_REMAPPERS[t] = impl
        return impl
    return decorator


def replace_schema_in_namelist(namelist, schema_map, ignore_objects,
                               default_schema, only_objects):
    """
    Utility function replacing a schema name in `pglast.node.List` representing
    an object.

    Arguments:
       namelist (`pglast.node.List`): the namelist to modify
       schema_map (dict): a dictionary mapping old schema names to their
          replacements
       ignore_objects (list): a list of fully qualified object names to ignore.
       default_schema (str): a schema name to use in an unqualified
           name
       only_objects (list): the opposite of ignore_objects: only remap names
           which are in this list.

    Returns:

        None: the namelist is modified in-place.

    """

    if not isinstance(namelist, List):
        raise TypeError("Exepected %s got %s" % (List, type(namelist)))
    if len(namelist) >= 2:
        schema_name = namelist[0].string_value
        table_name = namelist[1].string_value
    else:
        return
    if schema_name is None:
        return
    fqn = "%s.%s" % (schema_name, table_name)
    if fqn in ignore_objects or []:
        return
    if only_objects is not None and fqn not in only_objects:
        return
    if schema_name in schema_map:
        newschema = schema_map[schema_name]
        if newschema is None:
            namelist.pop(0)
        else:
            namelist[0].parse_tree['str'] = newschema


def replace_schema_in_fqn(fqn, schema_map, ignore_objects,
                          default_schema, only_objects):
    """
    Repace schema in a fully qualified name.

    Arguments:
       fqn (str): the fully qualified name to replace (schema.object)
       schema_map (dict): a dictionary mapping old schema names to their
          replacements
       ignore_objects (list): a list of fully qualified object names to ignore.
       default_schema (str): this argument is here for consistency with
       `replace_schema_in_namelist` but isn't actually used, since fqn MUST be
           fully qualified.
       only_objects (list): the opposite of ignore_objects: only remap names
           which are in this list.
    """
    if fqn in ignore_objects or []:
        return fqn
    if only_objects is not None and fqn not in only_objects:
        return fqn
    schema_name, object_name = fqn.split(".")
    schema_name = schema_map.get(schema_name, schema_name)
    return "%s.%s" % (schema_name, object_name)


def default_remapper(node, schema_map, ignore_objects, default_schema,
                     only_objects):
    """
    Default remapper for all node: just call recursively the appropriate
    remapper for every child node.
    """
    for item in node.traverse():
        remap_one_node(item, schema_map, ignore_objects, default_schema,
                       only_objects)
    return


def get_remapper(node):
    """
    For a given node, return the appropriate remapper.
    """
    if isinstance(node, Scalar):
        return None
    parent_tag = node.parent_node and node.parent_node.node_tag
    remapper = NODE_REMAPPERS.get((parent_tag, node.node_tag))
    if remapper is None:
        remapper = NODE_REMAPPERS.get(node.node_tag, None)
    return remapper



def remap_one_node(node, schema_map, ignore_objects, default_schema,
                   only_objects=None):
    """
    Remap only one node if a corresponding remapper is found.
    """
    remapper = get_remapper(node)
    if remapper:
        remapper(node, schema_map, ignore_objects, default_schema, only_objects)


def remap_node(node, schema_map, ignore_objects, default_schema,
               only_objects=None):
    """
    Entry point for remapping schema names in a statement.

    Arguments:
      node (`pglast.node.Node`): the node to remap
      schema_map (dict): a dictionary mapping old schema names to their
         replacements.
      ignore_objects (list): a list of FQN of objects to ignore.
      default_schema (str): when an unqualified object name is found in the
         statement, qualify it with the given schema.
      only_objects (list): opposite of ignore_objects: a list of FQN to
          process, any objects which name isn't in this list will be ignore.
    """
    if isinstance(node, Scalar):
        return
    remapper = default_remapper
    remapper(node, schema_map, ignore_objects, default_schema, only_objects)


@node_remapper('VariableSetStmt')
def remap_variable(node, schema_map, ignore_objects, default_schema,
                   only_objects):
    """
    Don't do anythin with SET statements for now.
    FIXME: special case for SET search_path ?
    """
    return


@node_remapper('CreateEnumStmt')
def remap_enum(node, schema_map, ignore_objects, default_schema,
               only_objects):
    """
    Remapper for CREATE TYPE AS ENUM
    """
    replace_schema_in_namelist(node.typeName, schema_map, ignore_objects,
                               default_schema, only_objects)
    return


@node_remapper('GrantStmt')
def remap_grant(node, schema_map, ignore_objects, default_schema,
                only_objects):
    """
    Remapper for GRANT statements.
    """
    objecttype = ObjectType(node.objtype.value)
    # In that case, we directly have the schema name here
    if objecttype == ObjectType.OBJECT_SCHEMA:
        for item in node.objects:
            current_schema = item.parse_tree['str']
            if current_schema in schema_map:
                item.parse_tree['str'] = schema_map[current_schema]
        return
    # In that case, just recurse into the function name
    if objecttype == ObjectType.OBJECT_FUNCTION:
        for item in node.objects:
            remap_one_node(item, schema_map, ignore_objects, default_schema,
                       only_objects)
        return


@node_remapper('CreateTrigStmt')
def remap_create_trig(node, schema_map, ignore_objects,
                      default_schema, only_objects):
    """
    Remapper for CREATE TRIGGER statements.
    """
    remap_one_node(node.relation, schema_map, ignore_objects, default_schema,
               only_objects)
    replace_schema_in_namelist(node.funcname, schema_map, ignore_objects,
                               default_schema, only_objects)
    if node.whenClause:
        remap_one_node(node.whenClause, schema_map, ignore_objects,
                   default_schema, only_objects)


@node_remapper('ObjectWithArgs')
def remap_object_with_args(node, schema_map, ignore_objects,
                           default_schema, only_objects):
    """
    Remapper for ObjectWithArgs nodes (ex: function references)
    """
    replace_schema_in_namelist(node.objname, schema_map, ignore_objects,
                               default_schema, only_objects)


@node_remapper('AlterSeqStmt')
def remap_alterseq(node, schema_map, ignore_objects,
                   default_schema, only_objects):
    """
    Remapper for ALTER SEQUENCE nodes.
    """
    remap_one_node(node.sequence, schema_map, ignore_objects,
               default_schema, only_objects)
    for option in node.options:
        if option.defname == 'owned_by':
            replace_schema_in_namelist(option.arg, schema_map, ignore_objects,
                                       default_schema, only_objects)


@node_remapper('RangeVar')
def remap_rangevar(node, schema_map, ignore_objects, default_schema,
                   only_objects):
    """
    Remapper for RangeVar nodes (eg, relation names).
    """
    current_schema = node.parse_tree.get('schemaname', default_schema)
    if current_schema is None:
        # Do not do anything with unqualified calls
        return
    fqn = "%s.%s" % (node.schemaname and node.schemaname.value,
                     node.relname.value)
    if fqn in ignore_objects or []:
        return
    if only_objects is not None and fqn not in only_objects:
        return
    if current_schema in schema_map:
        newschema = schema_map[current_schema]
        if newschema is None:
            node.parse_tree.pop('schemaname', None)
        else:
            node.parse_tree['schemaname'] = newschema
    return


@node_remapper('CreateFunctionStmt')
def remap_createfunction(node, schema_map, ignore_objects, default_schema,
                         only_objects):
    """
    Remapper for CREATE FUNCTION statements.
    """
    replace_schema_in_namelist(node.funcname, schema_map, ignore_objects,
                               default_schema, only_objects)
    for param in node.parameters or []:
        remap_one_node(param, schema_map, ignore_objects, default_schema,
                   only_objects)

    remap_one_node(node.returnType, schema_map, ignore_objects, default_schema,
               only_objects)

    if node.language == 'sql':
        for option in node.options:
            if option.defname.value == 'as':
                query = Node(parse_sql(option.arg[0].string_value))[0].stmt
                remap_one_node(query, schema_map, ignore_objects, default_schema,
                           only_objects)
                option.arg[0].parse_tree['str'] = IndentedStream()(query)
    else:
        _, funcname = object_creation(node)
        logger.warn("Did not rewrite function %s body (language: %s)" %
                    (funcname, node.language.value))


@node_remapper('TypeName')
def remap_typename(node, schema_map, ignore_objects, default_schema,
                   only_objects):
    """
    Remapper for TypeName nodes.
    """
    replace_schema_in_namelist(node.names, schema_map, ignore_objects,
                               default_schema, only_objects)


@node_remapper('AlterTypeStmt')
def remap_altertype(node, schema_map, ignore_objects, default_schema,
                    only_objects):
    """
    Remapper for AlterType statements.
    """
    replace_schema_in_namelist(node.typename, schema_map, ignore_objects,
                               default_schema, only_objects)


@node_remapper('CreateEventTrigStmt')
def reamp_create_event_trig(node, schema_map, ignore_objects, default_schema,
                            only_objects):
    """
    Remapper for CREATE EVENT TRIGGER statements.
    """
    replace_schema_in_namelist(node.funcname, schema_map, ignore_objects,
                               default_schema, only_objects)


@node_remapper('CommentStmt')
@node_remapper('AlterOwnerStmt')
def remap_alterowner(node, schema_map, ignore_objects, default_schema,
                     only_objects):
    """
    Remapper for ALTER OWNER and COMMENT statements.
    """
    if node.node_tag == 'AlterOwnerStmt':
        objtype = ObjectType(node.objectType.value)
    else:
        objtype = ObjectType(node.objtype.value)
    if objtype == ObjectType.OBJECT_SCHEMA:
        current_schema = node.object.parse_tree['str']
        if current_schema in schema_map:
            node.object.parse_tree['str'] = schema_map[current_schema]
        return
    if isinstance(node.object, List):
        replace_schema_in_namelist(node.object, schema_map, ignore_objects,
                                   default_schema, only_objects)
        return
    remap_one_node(node.object, schema_map, ignore_objects,
                   default_schema, only_objects)


@node_remapper('FuncCall')
def remap_funccall(node, schema_map, ignore_objects, default_schema,
                only_objects):
    """
    Remapper for function call nodes.
    """
    replace_schema_in_namelist(node.funcname, schema_map, ignore_objects,
                               default_schema, only_objects)


@node_remapper('CreateSchemaStmt')
def remap_createschema(node, schema_map, ignore_objects, default_schema,
                       only_objects):
    """
    Remapper for CREATE SCHEMA statements.
    """
    schemaname = node.schemaname.value
    node.parse_tree['schemaname'] = schema_map.get(schemaname, schemaname)


@node_remapper('ColumnRef')
def remap_column_ref(node, schema_map, ignore_objects, default_schema,
                     only_objects):
    """
    Remapper for references to a column.
    """
    if len(node.fields) == 3:
        replace_schema_in_namelist(node.fields, schema_map, ignore_objects,
                                   default_schema, only_objects)

@node_remapper('TypeCast')
def remap_typecast(node, schema_map, ignore_objects, default_schema,
                   only_objects):
    """
    Remapper for a cast expression.
    This one is special cased to look for string values cast to regclass, in
    order to rewrite the original string value with the new schema.

    Exemple: SELECT nextval('public.sequence1'::regclass) neeeds to be
    rewritten as SELECT nextval('newschema.sequence1'::regclass).

    """
    # Look for casts from string constant to catalog type (eg: ::regclass)
    fqname = name_to_fqname(node.typeName.names)
    if fqname in ('pg_catalog.regclass', 'regclass'):
        if node.arg.node_tag == 'A_Const':
            val = node.arg.val
            if val.node_tag == 'String':
                val.parse_tree['str'] = replace_schema_in_fqn(
                        val.string_value, schema_map, ignore_objects,
                        default_schema, only_objects)
    else:
        # Replace the schema in the typename itself, then recurse as usual
        replace_schema_in_namelist(node.typeName.names, schema_map,
                                   ignore_objects, default_schema,
                                   only_objects)

