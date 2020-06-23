Architecture
============

This uses PostgreSQL logical decoding to achieve logical replication. We can
distinguish several parts:

`Logical basebackup`_
  With logical replication, we have the same need as with physical replication
  to capture a coherent backup on which we can apply subsequent changes. The
  script :code:`connemara_basebackup.py` performs this backup, on one database
  of a cluster.
`Logical decoder`_
  On the source database we wish to replicate, we need a logical decoder to
  decode the WAL changes into a stream of :code:`DML` statements. We use the
  `wal2json`_ logical decoder.
`WAL receiver`_
  The wal receiver connects to the source database using a replication
  connection, and consumes the decoded-changes as sent by the `Logical
  decoder`_. Those changes are inserted in a spool table, ready to be applied.
  The :code:`connemara_replication` executable performs those duties.
`Change replayer`_
  Once the changes are stored in the spool table, they need to be applied to the
  tables in the target database. This is done by the :code:`connemara_replay.pl`
  script.



.. graphviz::

  digraph top {

    labelloc="t";
    splines="polyline";
    compound="true";

    graph [
        rankdir = "LR"
    ];

   subgraph cluster_0 {
        label="Connemara";
        style=filled;
        color=lightgrey;

        subgraph cluster_1 {
          label = "Source Host";
          compound="true";
          style=filled;
          color=white;
          subgraph cluster_2 {
            style=filled;
            color=lightgrey;
            label = "Source PostgreSQL";
            sourcedb [style=invis shape=none];
            database1;
            database2;
          }
          sourcehost[style=invis shape=none];
        }

        subgraph cluster_5 {
          label = "Target Host";
          compound="true";
          style=filled;
          color=white;
          subgraph cluster_6 {
            compound="true";
            style=filled;
            color=lightgrey;

            label = "Target Database";
            targetdb [style=invis shape=none];
            database1_public [shape=folder, label="database1_public schema"];
            database2_public [shape=folder, label="database2_public schema"];
            replication_messages [shape=rectangle, label="replication.raw_messages"];
          }
          targethost [style=invis shape=none];
          connemara_basebackup [shape=square label="connemara_basebackup.py"]
          connemara_replication_database1 [shape=square label="connemara_replication@database1"]
          connemara_replication_database2 [shape=square label="connemara_replication@database2"]
          connemara_replay [shape=square label="connemara_replay.pl"]
        }

        sourcehost -> targethost [ltail="cluster_1", lhead="cluster_5"];

        connemara_basebackup -> database2[label="Create replication slot\nPG Dump"]
        connemara_basebackup -> database1[label="Create replication slot\nPG Dump"]
        connemara_basebackup -> targetdb[label="Create replication_origin"][lhead=cluster_6]
        connemara_basebackup -> database2_public[label="Restore data"]
        connemara_basebackup -> database1_public[label="Restore data"]
        connemara_replication_database1 -> database1[label="Consume WAL"]
        connemara_replication_database2 -> database2[label="Consume WAL"]
        connemara_replication_database1 -> replication_messages [label="Insert changes"]
        connemara_replication_database2 -> replication_messages [label="Insert changes"]
        connemara_replay -> replication_messages [label="consume changes"]
        connemara_replay -> database1_public [label="Apply changes"]
        connemara_replay -> database2_public [label="Apply changes"]
      }
  }


Logical basebackup
------------------

The logical basebackup is performed by the script :code:`connemara_basebackup.py`. The playbook deploys it in /usr/local/bin/.

To perform a logical basebackup, the script performs the following steps:

  * connect to the source database using a replication connection, and create a
    replication slot while leaving the connection open. This operation returns a snapshot name and the associated LSN.
  * the scripts then performs a :code:`pg_dump` of the source database using
    this snapshot. This guarantees we have the exact data corresponding to the
    LSN.
  * On the target database, we create a matching schema with the name
    :code:`<sourcedb>_public`, and setup a replication origin. A replication
    origin allows to store the source database LSN corresponding to the target
    state.
  * Using a clever set of regular expression, the dump is restored into the
    newly created schema. We only restore the :code:`pre-data` and :code:`data`
    section of the dump.
  * Since we need primary keys, unique indexes and foreign key, we restore those
    selectively.
  * Finally, every view is dropped in order to make DDL statement replay easier.



Logical decoder
---------------

The logical decoder we use is `wal2json`_. We use it with the following options:

  * :code:`include-types = false`
  * :code:`include-xids = true`
  * :code:`write-in-chunks = true`
  * :code:`include-unchanged-toast = false` WARNING: this option has been
    removed from the decoder in latest versions to default to false in all cases. Once the new
    version is released, we need to get rid of it.
  * :code:`filter-tables = bi.*` to ignore any changes made to tables in the bi
    schema.



WAL receiver
------------

The wal receiver is a small daemon written in C. One instance of the daemon has
to be spawned for each source database. The daemon is quite simple: it initiates
a connection to the source database using the replication slot created by the
logical basebackup, and starts streaming WAL from the position recorded in the
replication origin on the target database. Every transaction from the source
database is commited at once in a :code:`replication.raw_messages` table, using
the replication origin to update the replication progress.


Change replayer
---------------

The change replayer is a multi-threaded Perl daemon. There is only one instance of this.
It takes the content from :code:`replication.raw_messages`, row per row, and performs
corresponding operations to the tables as efficiently as possible. The principle is this:

  * The program starts as many replayer threads as requested (by default 4).
    Each replayer is responsible for a set of tables (they don't have to be in the same
    database).
  * Each operation (insert/update/delete), and all of its variants (depending on
    the updated colums for instance) is prepared when first needed and kept prepared by
    the responsible thread.
  * The main thread acts as a dispatcher: it reads a batch of records from
    :code:`replication.raw_messages` (30s worth of activity at most, but keeping consistent
    with the transactions of the source database) and queues records for
    consumption, on dedicated queues for each replayer thread. The thread is determined
    by a modulo on the md5 of :code:`source_db/source_schema/table`. When a 30s batch is finished
    each thread is asked to remove the records it applied from :code:`replication.raw_messages`
    and commit its work.
  * The main thread has 2 other responsibilities:

    * Detect DDLs: these are replays for the :code:`public.sql_ddl_statements` table of any database.
      They are replayed differently: first, all work performed until that point is committed,
      then the DDL is attempted (there is no way to guarantee it will work… we only replay
      it with a special search_path or die trying), then we start another transaction on all threads: some DDLs aren't
      transactional. DDLs are captured on the source databases with the :code:`connemara_log_ddl`
      event trigger and logged in the :code:`public.sql_ddl_statements`.
    * Build missing FK indexes. This is asked to another, dedicated worker thread, each time
      the dispatcher detects it has caught-up with the master, so as not to overload the server.
  * On any error, the daemon commits suicide, so the lag will rapidly increase, even if we miss
    the error from logwatch.


.. _wal2json: https://github.com/eulerto/wal2json
