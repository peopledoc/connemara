#!/usr/bin/perl
#

package Gui;
use warnings;
use strict;
use Cpanel::JSON::XS;
use Curses::UI;
use DBI;
use DBD::Pg qw(:pg_types);

use Data::Dumper;

use Data::Dumper;
use File::Basename;
use lib dirname(__FILE__);
use connemara_rewrite;

sub parse_conf
{
    my $self=shift;
    my ($conf_file) = @_;
    my $config;
    unless ($config = do $conf_file)
    {
        die "couldn't parse $conf_file: $@" if $@;
        die "couldn't understand $conf_file: $!"    unless defined $config;
        die "couldn't run $conf_file"       unless $config;
    };
    die "couldn't get the connection string from configuration" unless ($config->{connection_string});
    $self->{config}=$config;

}

sub connect_db
{
    my $self=shift;
    my ($connection_string)=$self->{config}->{connection_string};
    my $dbh= DBI->connect("dbi:Pg:$connection_string",
                                 undef,
                                 undef,
                                 {RaiseError => 1,
                                  pg_server_prepare => 1,
                                 });
    $self->{dbh}=$dbh;
}

sub get_next_record
{
    my $self=shift;
    my $dbh = $self->{dbh};
    my %db_restrict_slot;
    my $config = $self->{config};
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

    # get the next record to replay (it's supposed to be failed)
    # return a hash with parsed data and metadata, for the GUI to display
    # Build lists to ignore dbs from slots and the additional WHERE clause
    # if we filter on dbs
    my @db_list=map { $dbh->quote($_,{ pg_type => PG_VARCHAR })  } keys(%db_restrict_slot);
    my $db_list=join(',',@db_list);

    my @db_in_tuples;
    while (my ($key,$value) = each (%db_restrict_slot))
    {
        $key = $dbh->quote($key,{ pg_type => PG_VARCHAR });
        $value = $dbh->quote($value,{ pg_type => PG_VARCHAR });
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

    my $read_query = "SELECT ctid,
                         insert_timestamp,
                         lsn_start,
                         database,
                         source_slotname,
                         payload,
                         payload->>'kind' as operation,
                         payload->>'schema' as schema,
                         payload->>'table' as table
                  FROM replication.raw_messages
                  WHERE insert_timestamp < (SELECT min(insert_timestamp) + '30s'::interval FROM replication.raw_messages)
                  $db_list_where_clause
                  ORDER BY insert_timestamp,lsn_start
                  LIMIT 1";
    my $query_result = $dbh->selectrow_hashref($read_query);

    return undef unless (defined $query_result);

    if ($query_result->{table} eq 'sql_ddl_statements' and $query_result->{schema} eq 'public' and $query_result->{operation} eq 'insert')
    {
        #
        # Let's parse the json
        my $payload=decode_json($query_result->{payload});
        my %columns;
        # This one needs an explanation: @columns{ array } is a hash slice
        # This directly maps the columnnames in @{$payload->{columnnames}}
        # to the values in @{$payload->{columnvalues}}
        @columns{@{$payload->{columnnames}}}=@{$payload->{columnvalues}};
        # Just to get those 3 columns without fuss
        my $result;
        $result->{query}=$columns{current_query};
        $result->{search_path}=$columns{search_path};
        $result->{database}=$query_result->{database};
        $result->{ctid}=$query_result->{ctid};
        return $result;
    }
    else
    {
        return undef;
    }
}
sub refresh_gui
{
    my $self = shift;
    my $data;
    while (not defined($data))
    {
        $data = $self->get_next_record();
        if (not defined($data))
        {
            my $result = $self->{cui}->dialog(-message => "No query to replay (or it's not a DDL). Do you want to retry ?",
                                              -buttons => ['yes', 'no']   );
            exit (0) unless $result;
        }
        else
        {
            # Build the original query (no rewriting here)
            my $new_search_path=connemara_rewrite::fix_search_path($data->{search_path}, $data->{database});
            my $original_query_text = "SET search_path TO $new_search_path, public;\n";
            $original_query_text .= $data->{query}. "\n";
            $self->{meta}->text("search_path: [" . $data->{search_path} . "]\n======\ndatabase: " . $data->{database} . "\n======\nctid: " . $data->{ctid});
            $self->{original_query_area}->text($original_query_text);
            $self->{rewritten_query_area}->text($original_query_text);
            # And these used for execution verification
            $self->{original_database} = $data->{database};
            $self->{original_search_path} = $data->{search_path};
            $self->{original_query} = $data->{query};
            $self->{original_ctid} = $data->{ctid};
        }
    }
}

sub replay_query
{
    my $self = shift;
    # First check it's still the same query
    #
    # Then in a transaction execute the query and delete the record
    # If it fails, rollback, display the error, and stay there
    my $current_to_replay=$self->get_next_record();
    if (    $self->{original_database} ne $current_to_replay->{database}
         or $self->{original_ctid} ne $current_to_replay->{ctid}
         or $self->{original_search_path} ne $current_to_replay->{search_path}
         or $self->{original_query} ne $current_to_replay->{query})
    {
        my $result = $self->{cui}->dialog(-message => "The query to replay has changed in replication.raw_messages. Something is fishy. Leaving",
                                          -buttons => ['ok']   );
        die
    }
    # Ok, that's the same query. Let's go :)
    my $new_query = $self->{rewritten_query_area}->get();
    # Let's go to console mode temporarily (in case the do fails)
    $self->{cui}->leave_curses( );
    $self->{dbh}->begin_work();
    my $result = $self->{dbh}->do($new_query);
    $self->{dbh}->do("DELETE FROM replication.raw_messages WHERE ctid = '" . $self->{original_ctid} . "'" );
    $self->{dbh}->commit();
    $self->{cui}->reset_curses();
    $self->refresh_gui();
}

sub new
{
    my $class = shift;
    # Get all other parameters
    my (@args)=@_;
    my $self = {};
    bless $self, $class;
    %$self = (%$self , @args); # add parameters passed as args into the hash

    my $cui = new Curses::UI( -color_support => 1, -mouse_support => 0, -clear_on_exit => 1, -debug => 0 );
    $self->{cui} = $cui;

    my $help_window = $cui->add("help_window", "Window",
                        -y => -1,
                        -height => 1,
                    );
    my $main_window = $cui->add(
                         'main_window', 'Window',
                         -y    => 1,
                         -padbottom => 1 # Let some space for the context help
                 );
    my $meta = $main_window->add("meta", "TextViewer",
                             -height => 12,
                             -width => 20,
                             -border =>1,
                             -wrapping =>1);
    my $original_query = $main_window->add("original_query", "TextViewer",
                             -height => 12,
                             -x => 21,
                             -border =>1,
                             -wrapping =>1);
    my $rewritten_query = $main_window->add("rewritten_query", "TextEditor",
                             -y => 12,
                             -border => 1,
                             -wrapping =>1);
    $self->{meta} = $meta;
    $self->{original_query_area} = $original_query;
    $self->{rewritten_query_area} = $rewritten_query;
    my $help_area = $help_window->add('help_area', 'Label',
                               -text => 'C-Q: quit  C-R: refresh  C-E: execute and go to next');


    $rewritten_query->clear_binding('loose-focus');


    $cui->set_binding( sub {exit}, "\cQ");
    $cui->set_binding( sub {exit}, "\cC");
    $cui->set_binding( sub{$self->replay_query()}, "\cE");
    $cui->set_binding( sub{$self->refresh_gui()}, "\cR");

    $rewritten_query->focus();

    $self->parse_conf($self->{conf_file});
    $self->connect_db();
    $self->refresh_gui();
    return $self;
}


package main;

use warnings;
use strict;
use Getopt::Long;



sub help
{
    die(qq{
$0 [--help] \
   --conf configuration_file (/etc/connemara/whatever)
});
}

# Main
my $help;
my $conf_file;

GetOptions ( "help"  => \$help,
             "conf=s" => \$conf_file) or help();

if (not defined $conf_file)
{
    print "Give me a configuration file\n";
    help();
}


my $gui = Gui->new(conf_file => $conf_file);
$gui->{cui}->mainloop;
