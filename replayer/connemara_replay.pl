#!/usr/bin/perl
# This script applies replication events from the replication.raw_messages table

use strict;
use warnings;

use DBI;
use DBD::Pg qw(:pg_types);
use Carp qw(confess cluck);
use Data::Dumper;
use Encode;
use Getopt::Long;
use Try::Tiny;
use threads;
use Thread::Queue;
use Digest::MD5;
use Cpanel::JSON::XS; # Thread safe json::xs
use IO::Handle;

# This lib contains the functions for rewrites
# The use lib dirname hack adds the script's directory in @INC
use File::Basename;
use lib dirname(__FILE__);
use connemara_rewrite;


#
# This will be necessary all over (because stdout is going to journalctl and Perl
# would consider it buffered IO witouht this)
STDOUT->autoflush(1);
STDERR->autoflush(1);
$Carp::MaxArgLen=0;
# Global variables, simpler than to send them to threads.
# They are both set once on GetOptions
our $debug=0;
our $keep_data=0;
our $keep_schema=0;
our $nb_threads=4;
# other variables with no default values
our ($connection_string, $help, $conf_file);
our %db_restrict_slot;

# Global log function
sub logmessage
{
    my ($message)=@_;
    if ($debug)
    {
        print STDERR $message;
    }
}



#############################
# This part is thread-local #
#############################
my $ctid; # Thread local current ctid, used for debug trace


# Use a cache... it will be thread local, which is what we want, as each thread
# is responsible for a determined set of table
# Avoid asking the catalog for the coltypes each time we see a variant of a query
# on the same table
my %cached_coltypes;
my $prep_statement_coltype; # Just to store the prepared query once and for all
sub get_col_type
{
    my ($dbh,$schema,$table,$col)=@_;
    if (defined($cached_coltypes{$schema}->{$table}->{$col}))
    {
        return $cached_coltypes{$schema}->{$table}->{$col};
    }
    $prep_statement_coltype->execute("${schema}.${table}",$col);
    my $resultarray=$prep_statement_coltype->fetchall_arrayref;
    my $result=$resultarray->[0]->[0];
    die("Couldn't find the type of $table/$col") unless (defined $result);
    $cached_coltypes{$schema}->{$table}->{$col}=$result;
    return $result;
}

# Use another cache, thread local, same as previous
# This will also be used in the main thread, to dispatch to threads
my %cached_pks;
my $prep_statement_pks; # Store the prepared query once and for all
sub get_pk
{
    my ($db,$schema,$table)=@_;
    if (defined($cached_pks{$db}->{$schema}->{$table}))
    {
        return $cached_pks{$db}->{$schema}->{$table};
    }
    my $dest_schema = connemara_rewrite::fix_schema($schema,$db);
    $prep_statement_pks->execute("${dest_schema}.${table}");
    my $resultarray=$prep_statement_pks->fetchall_arrayref;
    die("Couldn't find the pk cols of $table") unless (defined $resultarray);
    # make this a one-dimension array
    my @result = map {$_->[0]} @$resultarray;
    $cached_pks{$db}->{$schema}->{$table}=\@result;
    return \@result; # Do this by reference, faster
}

# This function's purpose is to call get_pk if we're in the normal case
# or return something else in the case of events
# It's only there for the timescale to PG 12 migration
sub get_keys
{
    my ($db,$schema,$table)=@_;
    if ($db eq 'audit' and $table eq 'events')
    {
        my @result = ("event_id", "created_at");
        return \@result;
    }
    else
    {
        return get_pk($db,$schema,$table);
    }
}

# And another for unique constraints/indexes
my %cached_uniques;
my $prep_statement_uniques;
sub has_unique
{
    my ($db,$schema,$table)=@_;
    if (defined($cached_uniques{$db}->{$schema}->{$table}))
    {
        return $cached_uniques{$db}->{$schema}->{$table};
    }
    my $dest_schema = connemara_rewrite::fix_schema($schema,$db);
    $prep_statement_uniques->execute("${dest_schema}.${table}");
    my $resultarray=$prep_statement_uniques->fetchall_arrayref;
    die("Couldn't determine if $table has unique constraints") unless (defined $resultarray);
    my $nb_unique = @$resultarray[0]->[0];
    my $result=0;
    if ($nb_unique>1)
    {
        #There's more than a PK here
        $result=1;
    }
    $cached_uniques{$db}->{$schema}->{$table}=$result;
    return $result; # Do this by reference, faster
}

# This function takes a list of columns of a table
# and returns the formatted column list, and the
# list of column expressions to get the data from the payload
# (cast the json object's string representation into the correct data type)
sub generate_cols_sql
{
    my ($dbh, $db, $schema, $table, $columns)=@_;
    my $dest_schema = connemara_rewrite::fix_schema($schema,$db);

    # decode_json "expects an UTF-8 (binary) string of an json reference"
    # not an encoded string. So we need to encode it at the same time as
    # a binary string (ugly)
    my @colarray=@{decode_json(encode("utf8",$columns))};
    my @jsongetdata;
    for (my $i=0;$i<=$#colarray;$i++)
    {
        my $coltype = get_col_type($dbh, $dest_schema, $table, $colarray[$i]);
        if ($coltype ne 'bytea')
        {
            push @jsongetdata,("(payload#>>'{columnvalues,$i}')::${coltype}");
        }
        else
        {
            push @jsongetdata,("decode(payload#>>'{columnvalues,$i}','hex')");
        }
    }
    return (\@colarray,\@jsongetdata);
}

# Produce a list of $x, $x+1... $n.
# Just a utility function for the prep_statement_builder
sub generate_prep_dollars
{
    my @result;
    my ($min,$amount)=@_;
    for (my $i=$min;$i<$min+$amount;$i++)
    {
        push @result,('$' . $i);
    }
    return join(',',@result);
}

# Build a cache of prepared statements for all statements we will use, and the function to
# populate and query this cache. Thread local
my %cached_statements;
my $fetch_prep_statement;
sub get_prep_statement_object
{
    # The cached statements: for each db|schema|table|operation quadruplet, we store a prepared query
    my ($dbh,$row,$db,$schema,$table)=@_;
    my ( $operation,        $columns)=
       ( $row->{operation}, $row->{columns});
    my $dest_schema = connemara_rewrite::fix_schema($schema,$db);
    my $lookup_key;
    my $keys=get_keys($db,$schema,$table);
    # Update, and insert needs the list of columns: some will not be set (toast for instance), and anyway
    # it's simpler to just ask postgresql to cast the sub-json part as the correct data type
    # But won't do until we are able to propagate DDLs
    if ($operation eq 'update')
    {
        $lookup_key = "$db|$schema|$table|$operation|$columns";
    }
    else
    {
        $lookup_key = "$db|$schema|$table|$operation";
    }
    if (defined ($cached_statements{$lookup_key}))
    {
        return $cached_statements{$lookup_key}->{prep_statement};
    }
    # New statement, let's go
    # Statements don't look even remotely alike for insert, update and delete. Thanks, SQL...
    # Even the column order is different. We work around that by using $1,$2,$3... as arguments of the query
    # and having those always numbered the same

    # keys and placeholders for the WHERE clause (point to the PK)
    my $keys_in_sql;
    my $placeholders_in_sql;
    $keys_in_sql = join(',',map {$dbh->quote_identifier($_)} @$keys); # Used for update/delete
    $placeholders_in_sql = generate_prep_dollars(1,scalar(@$keys)); # Create the ($1,$2...)
    my $ctid_dollar;
    if ($operation eq 'insert')
    {
        $ctid_dollar='$1';
    }
    else
    {
        $ctid_dollar='$' . (scalar(@$keys)+1);
    }

    my $statement;
    if ($operation eq 'insert')
    {
        my ($refcolarray, $refjsongetdata) = generate_cols_sql($dbh, $db, $schema, $table, $columns);
        $statement = "INSERT INTO ${dest_schema}.${table}
                      (" .
                      join(',', map ('"' . $_ . '"', @{$refcolarray})) .
                      ") SELECT " . join(',', @{$refjsongetdata}) .
                      " FROM replication.raw_messages
                      WHERE raw_messages.ctid=$ctid_dollar";
    }
    elsif ($operation eq 'delete')
    {
        $statement = "DELETE FROM ${dest_schema}.${table}
                      WHERE (${keys_in_sql})=(".
                      $placeholders_in_sql
                      .")";
    }
    # This is special: updates may alter some columns and not some others
    elsif ($operation eq 'update')
    {
        my ($refcolarray, $refjsongetdata) = generate_cols_sql($dbh, $db, $schema, $table, $columns);

        $statement = "UPDATE ${dest_schema}.${table}
                      SET (" .
                      join(',', map ('"' . $_ . '"', @{$refcolarray})) .
                      ") = (
                       SELECT " . join(',', @{$refjsongetdata}) .
                       " FROM replication.raw_messages WHERE raw_messages.ctid=$ctid_dollar)
                      WHERE (${keys_in_sql})=(" .
                      $placeholders_in_sql
                      . ")";
    }
    else
    {
        confess("I don't know this operation: $operation");
    }
    my $sth = $dbh->prepare($statement, {pg_switch_prepared => 1, pg_prepare_now => 1});
    $cached_statements{$lookup_key}->{prep_statement}=$sth;
    $cached_statements{$lookup_key}->{statement}=$statement;
    return $sth;
}

# This is called when idling. We create missing indexes, in case we got a FK creation for instance
sub create_missing_indexes
{
    my ($dbh)=@_;
    # We cannot be in a transaction for this
    $dbh->commit();
    my $query=q{
    select 'DROP INDEX CONCURRENTLY ' || indexrelid::regclass from pg_index
    where not indisvalid
    and not exists
        (select 1 from pg_locks where relation=pg_index.indexrelid)
    };
    my $records_to_drop=$dbh->selectall_arrayref($query, {pg_direct => 1});
    foreach my $record(@$records_to_drop)
    {
        my $index=$record->[0];
        print "Dropping this invalid index: $index\n";
        $dbh->do($index);
    }

    $query=q{
with not_indexed_constraints as (
        select conname, conrelid::regclass as tablename, conkey
        from pg_constraint
        where contype = 'f'
          and not exists (
                 select 1
                 from pg_index
                 where indrelid=conrelid
                 -- We need to limit ourselves to indnkeyatts because the keys after that aren't indexed
                 -- then to array_upper(conkey,1) because our index must START exactly the same as the conkey
                 -- the slice on indkey is offset by -1 because indkey is a int2vector, those start at 0
                 -- As the key order is of no importance, the arrays just need to be included in each other
                 -- (equals in the ensemblist sense)
                   and ((indkey::int4[])[0:indnkeyatts-1])[1:array_upper(conkey,1)]@>conkey::int4[]
                   and ((indkey::int4[])[0:indnkeyatts-1])[1:array_upper(conkey,1)]<@conkey::int4[]
                        )
               ),
     unnested_constraints as (
        select conname, tablename, unnest.* FROM not_indexed_constraints,unnest(conkey) with ordinality)
SELECT 'CREATE INDEX CONCURRENTLY ' || conname || ' ON ' || tablename::text || '(' ||
       string_agg(quote_ident(attname::text), ',' order by ordinality) || ')'
from unnested_constraints
join  pg_attribute on (unnested_constraints.tablename=pg_attribute.attrelid
                   and pg_attribute.attnum=unnested_constraints.unnest)
group by tablename,conname
    };
    # This query is pg_direct, because it seems DBD::Pg is lead
    # astray by the array slice in the query
    my $records_to_create=$dbh->selectall_arrayref($query, {pg_direct => 1});
    foreach my $record(@$records_to_create)
    {
        my $index=$record->[0];
        print "Creating this index: $index\n";
        $dbh->do($index);
    }
    $dbh->begin_work();
}

# This is a DDL: the table is sql_ddl_statements in the public schema
# Let's try to do the DDL
# We have a special transaction semantics here, as some DDL are not transactional:
# We commit before and start a transaction at the end. While replaying DDLs, we
# do one DDL then delete its record from the raw_messages table, so we cannot lose anything
# At the very worst, we could end up with a DDL that worked and not deleting the record from
# the replay table. Of course, what will happen most in terms of failure is not being able
# to replay a statement.
sub do_ddl_change
{
    my ($dbh, $datarow)=@_;
    # The table is INSERT only. Ignore anything else (who knows, maybe someone will try to delete from it manually)
    if ($datarow->{operation} ne 'insert')
    {
        print("Unexpected " . $datarow->{operation} . " payload decoding a DDL. Here is the row:\n" . Dumper($datarow));
        return;
    }
    $dbh->commit(); # Go autocommit
    # Parse the json. For this we need to refetch the record… we only get it partially in the main loop
    my $row=$dbh->selectrow_hashref("SELECT * FROM replication.raw_messages WHERE ctid='". $datarow->{ctid}  . "'");
    my $payload=decode_json($row->{payload});
    my %columns;
    # This one needs an explanation: @columns{ array } is a hash slice
    # This directly maps the columnnames in @{$payload->{columnnames}}
    # to the values in @{$payload->{columnvalues}}
    @columns{@{$payload->{columnnames}}}=@{$payload->{columnvalues}};
    # Just to get those 3 columns without fuss
    logmessage(Dumper(\%columns));
    my $query=$columns{current_query};
    my $search_path=$columns{search_path};
    my $database=$datarow->{database};

    # Is it our kill command ?
    if ($query =~ /(?i)comment(?-i).*'KILL_ME'/)
    {
        confess "Got the kill command";
    }

    # tags are in postgresql's array text representation. As the array is one dimension,
    # and not containing weird characters, this is really easy to parse.
    my $command_tags=$columns{command_tags};
    $command_tags=~ s/{|}//g; # Remove starting { and finishing }
    my @tags = split(/\s*,\s*/,$command_tags); # Split by comma
    foreach my $tag(@tags)
    {
        $tag =~ s/^"(.*)"$/$1/;
    }
    logmessage("Parsed tags: " . Dumper(\@tags));

    # Ok, this is an insert on the sql_ddl_statements table,
    # and we have the search_path, the DDL query, and a list of tags
    # We only have a restricted list of cases where we accept going further
    # If not, we punt, and trace in the log enough information to help the
    # DBA fix it
    # Assuming there is no comma in a schema name...
    my $new_search_path=connemara_rewrite::fix_search_path($search_path, $database);

    # Will be reset by reset_session()
    $dbh->do("SET search_path TO $new_search_path,public"); # Public last, we need it for some extensions

    # Call for query rewrite. The function will return undef if the query is to be ignored.
    # Tags are probably worthless, but let's keep them in case
    my $rewritten = connemara_rewrite::fix_schema_ddl($query, $row, \@tags);
    if (defined($rewritten))
    {
        local $dbh->{RaiseError}; # Turn off errors just there
        my $performed = $dbh->do($rewritten);
        unless (defined($performed)) # the query failed
        {
            # If that's a drop table, it's not important. It may have been a temp table
            # But only ignore if the object doesn't exist. Other errors are still valid
            if ($rewritten =~ /^\s*drop\s+table/i and $dbh->state eq '42P01')
            {
                logmessage("Ignored $rewritten, which failed because it's non existent, $dbh->errstr\n");
            }
            else
            {
                logmessage("Failed replaying $rewritten");
                die $dbh->errstr;
            }
        }
        logmessage("DDL $rewritten replayed\n");
    }
    else
    {
        logmessage("$query ignored\n");
    }
    $dbh->begin_work(); # Leave autocommit
}




# Get one event, and do the operation on the dbhandler provided
sub do_db_change
{
    my ($dbh, $row)=@_;

    # Before even getting the prepared statements: is there any point in doing the statement at all ?
    if ($row->{table} =~ /^pg_temp/)
    {
        # This is a table rewrite. For now no way to trap it (even logical replication fails on this)
        # We'll only replay the DDL and hope for the best (now() may be a bit offset for instance)
        # print("Table rewrite record. Ignoring:" . Dumper($row));
        return;
    }


    # Let's see if this is a DDL. If yes, send it to a dedicated function
    if ($row->{table} eq 'sql_ddl_statements' and $row->{schema} eq 'public')
    {
        do_ddl_change($dbh, $row);
        return;
    }
    # Not a DDL
    # We may have to fix database/table/schema. So we copy these in work variables
    my ($database, $schema, $table) = ($row->{database}, $row->{schema}, $row->{table});

    ($database, $schema, $table) = connemara_rewrite::fix_object_dml($database, $schema, $table, $row);
    return if (not defined($database));
    my $prepstatement=get_prep_statement_object($dbh, $row, $database, $schema, $table);
    my @PKvalues=();
    # Will set those for update and delete. Will be empty for inserts, of course
    if ($row->{operation} ne 'insert')
    {
        # get the PK's list of columns, and find those in the json
        my $keys=get_keys($database,$schema,$table);
        # Build an hash from the columnames/columnvalues arrays
        my %record;

        my @columnnames=@{decode_json(encode("utf8", $row->{keys}))};
        my @columnvalues=@{decode_json(encode("utf8", $row->{keyvalues}))};

        @record{@columnnames}=@columnvalues; # This maps 1:1 the two arrays in the hash.
        # We need to match the PK to the one we already know (same order, don't trust wal2json)
        # Now build the PK values
        @PKvalues=@record{@$keys};
        # Did we find everything ?
        confess('Problem with the PK on record') unless(scalar(@PKvalues) == scalar(@$keys));
    }
    # We have a distinct arg list for deletes: we don't need the ctid for it...
    my @args;
    if ($row->{operation} eq 'delete')
    {
        @args=@PKvalues;
    }
    elsif ($row->{operation} eq 'insert' or $row->{operation} eq 'update')
    {
        @args=(@PKvalues, $row->{ctid});
    }
    else
    {
        confess("Unknown operation " . Dumper($row));
    }
    try {
        my $affected_rows = $prepstatement->execute(@args);
        if ($affected_rows != 1)
        {
            die "Should have affected one row, but the statement affected $affected_rows";
        }
    } catch {
        print("Cannot handle " . Dumper($row) . "\nwith query " . $prepstatement->{statement});
        confess();
    };
}
sub build_fetch_query
{
    my ($dbh)=@_;

    my $row_query = "SELECT ctid,
                             database,
                             payload->>'kind' as operation,
                             payload->>'schema' as schema,
                             payload->>'table' as table,
                             payload#>'{oldkeys,keynames}' as keys,
                             payload#>'{oldkeys,keyvalues}' as keyvalues,
                             payload->'columnnames' as columns,
                             payload->'columnvalues' as columnvalues
                      FROM replication.raw_messages
                      WHERE ctid=?";
    return $dbh->prepare($row_query);
}

sub build_coltype_query
{
    my ($dbh)=@_;

    my $coltype_query="select atttypid::regtype::text
                       from pg_catalog.pg_attribute
               where attrelid=?::regclass and attname=?";
    return $dbh->prepare($coltype_query);
}

sub build_colpk_query
{
    my ($dbh)=@_;

    # Using to_regclass because it won't trigger an exception if the object doesn't exist
    my $colpks_query="select attname
                      from (select unnest(conkey),conrelid
                            from pg_constraint where conrelid = to_regclass(?) and contype='p') tmp
                            join pg_attribute on (tmp.conrelid=pg_attribute.attrelid and unnest=attnum)
                      order by attnum";
    return $dbh->prepare($colpks_query);
}

sub build_unique_query
{
    my ($dbh)=@_;

    # Using to_regclass because it won't trigger an exception if the object doesn't exist
    my $has_unique_query="select count(*)
                         from pg_index
                         where indrelid=to_regclass(?) and indisunique
                         ";
    return $dbh->prepare($has_unique_query);
}
# Just discard everything and reset the cache
sub reset_session
{
    my ($dbh)=@_;
    # Empty the caches
    undef(%cached_coltypes);
    undef(%cached_statements); # Will call the destructors of the prepared statements
    undef($fetch_prep_statement);
    undef($prep_statement_coltype);
    undef(%cached_pks);
    undef(%cached_uniques);

    $dbh->do('RESET ALL'); # reset without destroying the prepared statements
    $dbh->do('DISCARD PLANS');
    $dbh->do('SET session_replication_role TO replica');
    $dbh->do("SET lock_timeout TO '10s'"); # If I can't lock, let's die and retry later
    # Turn off checking of function bodies
    $dbh->do('SET check_function_bodies TO off');

    $fetch_prep_statement=build_fetch_query($dbh);
    $prep_statement_coltype=build_coltype_query($dbh);
    $prep_statement_pks=build_colpk_query($dbh);
    $prep_statement_uniques=build_unique_query($dbh);
}

# This deletes or archives the ctid depending on connemara.record_log being 't' or 'f'
# We passe the ctids array by reference, as it can contain a very large number of records
sub clean_raw_messages
{
    my ($dbh,$ctids)=@_;

    my $delete_query=
"DELETE FROM replication.raw_messages
WHERE ctid IN ("
. join(',',map("'". $_ . "'",@$ctids))
. ")";

    if ($keep_data)
    {
        # Going to cost a bit as we create a second big string
        # But this is a debug mode
        $dbh->do(
"WITH deleted AS
      (" . $delete_query . " RETURNING *)"
. "INSERT INTO replication.replayed SELECT * FROM deleted"
                );
    }
    else
    {
        $dbh->do($delete_query);
    }
}

# The main() of each worker
# Each worker reads messages from its queue
# There are 3 kinds of messages:
# Either a ctid pointing to the record to replay, sent by the main thread
# Or "SYNC", meaning we will commit (to which we respond we are ok)
# Or "COMMIT", to commit simultaneously on all threads
# Or "DISCARD", to end all that has been done to this point and cleanup all caches (after a DDL has been replayed)
# Or "CREATE MISSING INDEXES", which will be sent to the async worker thread to build missing indexes
sub new_worker
{
    my ($connection_string,$queue,$queue_message_ok)=@_;
    # When a thread dies, it only kills the thread by default. This handler calls exit() to terminate the program in case of a die in a thread.
    $SIG{__DIE__} = sub {print "Thread died while working on ctid $ctid. Exiting the whole program\nIf you have solved the problem, perform <DELETE FROM replication.raw_messages WHERE ctid='$ctid'>\n"; cluck(); exit(1)};
    my $dbh_write= DBI->connect("dbi:Pg:$connection_string",
                                undef,
                                undef,
                                {RaiseError => 1,
                                 pg_server_prepare => 1,
                                });

    reset_session($dbh_write); # Sets a clean session, and default session params
    $dbh_write->begin_work();
    my @ctids;
    while (my $msg=$queue->dequeue())
    {
        $ctid=$msg; # For debugging purposes, it will be in the traceback
        # Delete messages every 1000 replay (saves memory)
        if (@ctids==1000)
        {
            clean_raw_messages($dbh_write,\@ctids);
            @ctids=();
        }
        if ($msg eq 'SYNC')
        {
            # If there are records to delete, lets do it
            if (@ctids)
            {
                clean_raw_messages($dbh_write,\@ctids);
            }
            undef(@ctids);
            $queue_message_ok->enqueue('SYNC OK');
        }
        elsif ($msg eq 'COMMIT')
        {
            $dbh_write->commit();
            $queue_message_ok->enqueue('COMMIT OK');
            $dbh_write->begin_work();
        }
        elsif ($msg eq 'DISCARD')
        {
            # Some schema changes have been performed. We need to
            # forget our prepared statements, plans, everything
            # If there are records to delete, lets do it
            if (@ctids)
            {
                clean_raw_messages($dbh_write,\@ctids);
            }
            undef(@ctids);
            $dbh_write->commit();
            reset_session($dbh_write);
            $dbh_write->begin_work();
            # Confirm
            $queue_message_ok->enqueue('DISCARD OK');
        }
        elsif ($msg eq 'CREATE MISSING INDEXES')
        {
            # This session will create missing indexes
            create_missing_indexes($dbh_write);
        }
        elsif ($msg eq 'SWITCH LOGGING')
        {
            # Thread local variable… need to do this in all threads
            $keep_data = !$keep_data;
        }
        else
        {
            # So we got a ctid. Let't get it from the database and work on it
            $fetch_prep_statement->execute($msg);
            my $row=$fetch_prep_statement->fetchrow_hashref;
            do_db_change($dbh_write, $row);
            push @ctids,($msg);
            # Maybe do a prepared query, we'll see
        }
    }
}

############################################################
# From here, this is the dispatcher thread's (main) code   #
############################################################
# Except from parsing command line and printing help, what this part does is
# * Read a batch of records from the log table, restricting to a range of dates to keep
# the batch size reasonable
# * Dispatch these records to the threads
# * Synchronize them at the end of the batch
my @queues; # list of the queues, local to the main

sub help
{
    die(qq{
$0 [--help] \
   --conf
});
}

# This discards all prepared statements and cached data
sub discard_sessions
{
    my ($queue_message_ok)=@_;
    logmessage("Discarding prepared statements and caches in all sessions after schema change\n");
    foreach my $queue(@queues)
    {
        $queue->enqueue('DISCARD');
    }
    for (my $i=0;$i<=$#queues;$i++)
    {
        $queue_message_ok->dequeue(); # We don't care what is here. It's OK anyway :)
    }
}

# ask all sessions to commit their batch. Used when we have finished a batch
# We ask all sessions to confirm they have emptied their queue, then ask for a commit
# the purpose being that these commits are as simultaneous as possible
sub commit_sessions
{
    my ($queue_message_ok, $last_replayed,)=@_;
    logmessage("Syncing threads\n");
    foreach my $queue(@queues)
    {
        $queue->enqueue('SYNC');
    }
    for (my $i=0;$i<=$#queues;$i++)
    {
        $queue_message_ok->dequeue(); # We don't care what is here. It's OK anyway :)
    }
    # Ok, all threads are ready to commit
    logmessage("Asking for commit\n");
    foreach my $queue(@queues)
    {
        $queue->enqueue('COMMIT');
    }
    for (my $i=0;$i<=$#queues;$i++)
    {
        $queue_message_ok->dequeue(); # We don't care what is here. It's OK anyway :)
    }
    # All threads have committed
    logmessage("Commit\n");
    print("Reached $last_replayed\n") if (defined($last_replayed));
 }

sub switch_replay_mode_main
{
    foreach my $queue(@queues)
    {
        $queue->enqueue('SWITCH LOGGING');
    }
}


GetOptions ( "help"  => \$help,
             "conf=s" => \$conf_file) or help();
help() unless ($conf_file);
help() if ($help);

unless (defined $conf_file)
{
    print "Conf file not provided\n";
    help();
}

#Let's parse the conf file
my $config;
unless ($config = do $conf_file)
{
    die "couldn't parse $conf_file: $@" if $@;
    die "couldn't understand $conf_file: $!"    unless defined $config;
    die "couldn't run $conf_file"       unless $config;
}
# Get some variables from the configuration
# First the mandatory ones
foreach my $var ('connection_string')
{
    unless (defined $config->{$var})
    {
        die "$var is missing from the configuration file";
    }
    # To be able to use $$
    no strict "refs";
    $$var = $config->{$var};
    use strict "refs";
}
# Then non mandatory
foreach my $var ('nb_threads', 'debug', 'keep_data', 'keep_schema')
{
    next unless (defined $config->{$var});
    # To be able to use $$
    no strict "refs";
    $$var = $config->{$var};
    use strict "refs";
}
# Then create a hash with DB restrictions on providers
if (exists $config->{db_options})
{
    while (my ($db,$keys) = each %{$config->{db_options}})
    {
        if (exists($keys->{source_slotname}))
        {
            $db_restrict_slot{$db}=$keys->{source_slotname};
        }
    }
}


# Main !

# Connect to the database.
# The 2 undefs are user and password. They can be provided in the connection string in PG
# but can't in some other SGDBs, hence their mandatory presence
# RaiseError means to produce exceptions for all operations instead of return values
my $dbh_events= DBI->connect("dbi:Pg:$connection_string",
                             undef,
                             undef,
                             {RaiseError => 1,
                              pg_server_prepare => 1,
                             });

# First, cleanup the "replayed" table. But only if we are started with --keep data. We'll only create the table
# keeping records in it if we're not started with --keep_data
if ($keep_data)
{
    $dbh_events->do("DROP TABLE IF EXISTS replication.replayed");
}
$dbh_events->do("CREATE TABLE IF NOT EXISTS replication.replayed (LIKE replication.raw_messages)");

# Set up signal handlers
$SIG{USR1} = \&switch_replay_mode_main;


# Build the threads and their queues
# We need as many queues as there will be threads
my $queue_message_ok=Thread::Queue->new();
# We create one thread more, to send utility tasks to it, such as building missing indexes
for (my $i=0;$i<$nb_threads+1;$i++)
{
    my $queue=Thread::Queue->new();
    $queue->limit = 1000; # No more than 1000 messages in each queue, block after
    my $thread=threads->create('new_worker',$connection_string,$queue,$queue_message_ok);
    push @queues,($queue);
}

# Remove the latest queue of this list to use as async queue
my $queue_async = pop(@queues);

# Build lists to ignore dbs from slots and the additional WHERE clause
# if we filter on dbs
my @db_list=map { $dbh_events->quote($_,{ pg_type => PG_VARCHAR })  } keys(%db_restrict_slot);
my $db_list=join(',',@db_list);

my @db_in_tuples;
while (my ($key,$value) = each (%db_restrict_slot))
{
    $key = $dbh_events->quote($key,{ pg_type => PG_VARCHAR });
    $value = $dbh_events->quote($value,{ pg_type => PG_VARCHAR });
    push @db_in_tuples, ("($key,$value)");
}
my $in_list=join(',',@db_in_tuples);

my $db_list_where_clause='';
if ($db_list)
{
    # build sthing like
    # database not in ('toto','tutu') or (database,source_slotname) not in ( ('tata','foo'), ('tete', 'bar'));
    $db_list_where_clause="AND ( database not in ($db_list) or (database,source_slotname) in ($in_list) )";
}

while (1)
{
    # Let's fetch a new batch
    my $updated = 0;
    my $updated_by_pk = 0;
    my $last_replayed;


    $dbh_events->begin_work();
    # This may be a lot of data. Fetch this 1000 per 1000 with a cursor
    # Don't do batches of more than 30s worth of transactions.
    # That makes it easier for vacuum to cleanup
    my $read_query = "SELECT ctid,
                             insert_timestamp,
                             lsn_start,
                             database,
                             source_slotname,
                             payload->>'kind' as operation,
                             payload->>'schema' as schema,
                             payload->>'table' as table,
                             payload#>'{oldkeys,keynames}' as keys,
                             payload#>'{oldkeys,keyvalues}' as keyvalues,
                             payload->'columnnames' as columnnames,
                             payload->'columnvalues' as columnvalues
                      FROM replication.raw_messages
                      WHERE insert_timestamp < (SELECT min(insert_timestamp) + '30s'::interval FROM replication.raw_messages)
                      $db_list_where_clause
                      ORDER BY insert_timestamp,lsn_start";
    $dbh_events->do("DECLARE curs CURSOR FOR $read_query");
    $prep_statement_pks=build_colpk_query($dbh_events); # We need this in this thread too
    $prep_statement_uniques=build_unique_query($dbh_events); # We need this in this thread too
    # Read and dispatch
    while (1)
    {
        my @ctids;
        my $sth_events = $dbh_events->prepare("FETCH 1000 FROM curs");
        $sth_events->execute();
        last if 0 == $sth_events->rows;
        while (my $row = $sth_events->fetchrow_hashref)
        {
            # We have two cases here. Most of the time we'll get "normal" dmls, which we know how to replay simply
            # But we may also get DDLs. Those are captured in the sources database with an event trigger, which inserts the
            # source query in public.sql_ddl_statements. When we meet one of those, we
            # * Commit what we have done (the ddl statements need to acquire exclusive locks)
            # * Dispatch THIS message, on its own, to the first worker
            # * Do our best to work out what to do with this DDL (set search_path, build missing indexes in cases of foreign keys)
            # * Discard on all sessions
            # * Commit
            if ($row->{table} eq 'sql_ddl_statements' and $row->{schema} eq 'public')
            {
                commit_sessions($queue_message_ok, $last_replayed);
                $queues[0]->enqueue($row->{ctid});
                commit_sessions($queue_message_ok, $last_replayed);
                discard_sessions($queue_message_ok);
                undef(%cached_pks); # Forget the main thread PKs cache, something might have changed because of the DDL
                $updated=0; # We will start a new batch
                $updated_by_pk=0; # We will start a new batch
            }
            else
            {
                # If the table has another unique constraint, we cannot dispatch per PK. We need to do this per table (or we'll have conflicts)
                my $has_unique = has_unique($row->{database},$row->{schema},$row->{table});
                $updated++;
                if (! $has_unique)
                {
                    # What thread should get it ? Easiest way I found is to do a md5, take one int
                    # from it, and modulo the amount of threads to determine the target queue/thread
                    # We use database/schema/table/pkcols if possible (no other unique constraint, or we'll conflict)
                    # or simply database/schema/table if not
                    # The reason is that we can replay the statements per-PK or per-table as a failback
                    # to speedup things
                    $updated_by_pk++;
                    my @PKvalues;
                    my $keys=get_keys($row->{database},$row->{schema},$row->{table});
                    my %record;
                    my @columnnames;
                    my @columnvalues;
                    my $pkchanged=0;
                    if ($row->{operation} eq 'delete')
                    {
                        # Nothing better to do. Trust wal2json to give us the correct keys
                        # We still need to do this, to be sure we'll see them in the
                        # correct order
                        @columnnames=@{decode_json(encode("utf8", $row->{keys}))};
                        @columnvalues=@{decode_json(encode("utf8", $row->{keyvalues}))};
                    }
                    elsif ($row->{operation} eq 'update')
                    {
                        # We have a special case to handle: does the PK change. If yes,
                        # as it will change threads, we need to commit, or we face a deadlock
                        # risk. We need to commit before, to be sure we don't have unique violation
                        # and after, because we will change threads.
                        @columnnames=@{decode_json(encode("utf8", $row->{keys}))};
                        @columnvalues=@{decode_json(encode("utf8", $row->{keyvalues}))};
                        my @newcolumnnames=@{decode_json(encode("utf8", $row->{columnnames}))};
                        my @newcolumnvalues=@{decode_json(encode("utf8", $row->{columnvalues}))};
                        # Build a has to make using newcolumns more efficient for our test
                        my %newcolumns;
                        @newcolumns{@newcolumnnames}=@newcolumnvalues;
                        # Ok, let's find the values in this hash associated with the PK/PK values
                        my $pksize = scalar(@columnnames);
                        for (my $i=0;$i<$pksize;$i++)
                        {
                            if ($newcolumns{$columnnames[$i]} ne $columnvalues[$i])
                            {
                                $pkchanged=1;
                            }
                        }
                        if ($pkchanged)
                        {
                            print STDERR "PK has changed for a record on this table: $row->{database},$row->{schema},$row->{table}\n";
                        }
                    }
                    else
                    {
                        # This is an insert... we calculate it ourselves
                        @columnnames=@{decode_json(encode("utf8", $row->{columnnames}))};
                        @columnvalues=@{decode_json(encode("utf8", $row->{columnvalues}))};
                    }
                    @record{@columnnames}=@columnvalues; # This maps 1:1 the two arrays in the hash
                    # Now build the PK values
                    @PKvalues=@record{@$keys};

                    my $cksum=Digest::MD5::md5($row->{database},$row->{schema},$row->{table},encode("utf8",join('-',@PKvalues)));
                    my ($hash)=unpack('NNNN',$cksum); # 32 bits will be more than enough
                    my $thread_number=$hash % $nb_threads;
                    # Has the PK changed ? That's probably a very rare event in a normal database.
                    # But it's bad, as we need to commit everything before (to avoid PK conflicts)
                    # and after (to avoid locks). So this is a rare case when we will show intermediary results
                    commit_sessions($queue_message_ok, $last_replayed) if ($pkchanged);
                    $queues[$thread_number]->enqueue($row->{ctid});
                    commit_sessions($queue_message_ok, $last_replayed) if ($pkchanged);
                }
                else # This table has other unique values... tough luck
                {
                    # What thread should get it ? We have other unique, so do a md5, take one int
                    # from it, and modulo the amount of threads to determine the target queue/thread
                    my $cksum=Digest::MD5::md5($row->{database},$row->{schema},$row->{table});
                    my ($hash)=unpack('NNNN',$cksum); # 32 bits will be more than enough
                    my $thread_number=$hash % $nb_threads;
                    $queues[$thread_number]->enqueue($row->{ctid});
                }
                $last_replayed=$row->{insert_timestamp};
           }
       }
    }
   # End of this batch, no more record to fetch
    $dbh_events->do("CLOSE curs");
    if (not $updated)
    {
        logmessage("Nothing to do\n");
        $dbh_events->commit(); # Just to release the snapshot for next cursor iteration
        $queue_async->enqueue("CREATE MISSING INDEXES"); # Ask to the worker thread to have a look to missing indexes, best moment, we're idle
        sleep(5);
        next;
    }
    commit_sessions($queue_message_ok, $last_replayed);

    $dbh_events->commit(); # Just to release the snapshot for next cursor
    # Log updated stats if in debug
    logmessage("Updated $updated records, " . sprintf("%.2f",($updated_by_pk/$updated)*100) . "% by primary key\n")
}
