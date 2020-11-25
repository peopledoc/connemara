#!/usr/bin/perl
#
use Digest::MD5;
use Carp qw(confess cluck);

package connemara_rewrite;


use strict;
use warnings;

# Replace public with rh2_public
sub fix_schema
{
    my ($schema,$database)=@_;
    # connemara_rewrite must know parent keep_schema instead of passing the value in each call
    if ($main::keep_schema) {
        return $schema;
    }
    return $database . "_" . $schema;
}

# For us, this only tries to fix trivial cases when the schema is directly specified
# in a DDL. For this to work, you'll need to be very strict on how you write your DDLs
# If the function returns undef, it means to ignore the statement

# We also have this constant:
# Ignore DDLs that have a special md5sum
# For now it's only the DO block in version-management-ddl.sql that is ignored here
my @ignored_md5 = ('c84a77b8caf1d8bc02f2a21b2e13d0da');
# And for fast search:
my %ignored_md5 = map {$_ => 1} @ignored_md5;
sub fix_schema_ddl
{
    my ($ddl, $row, $tags)=@_;
    my $fixed_ddl;
    my $database = $row->{database};

    my $ddl_md5=Digest::MD5::md5_hex($ddl);
    if (defined ($ignored_md5{$ddl_md5}))
    {
        print "Ignoring ddl statement <$ddl>, because of known checksum\n"
    }
    elsif ($ddl =~ /^\s*create temp(orary)? table/i)
    {
        print "Skipping <$ddl>, replicating temp tables make no sense\n";
    }

    elsif ($ddl =~ /s*(create|alter|drop)\s+extension/i)
    {
        print "Skipping <$ddl> extensions should already be installed\n";
    }
    elsif ($ddl =~ /^\s*alter\s*table\s*\S*\s*(disable|enable)\s*trigger\s*\S*\s*;?/i)
    {
        print "Skipping <$ddl> we don't care about triggers\n";
    }
    elsif ($ddl =~ /^\s*alter\s*table\s*\S*\s*ADD\b.*\bCHECK\b/i)
    {
        print "Skipping <$ddl> we don't care about CHECK constraints\n";
    }
    elsif ($ddl =~ /^\s*alter\s*table\s*\S*\s*(disable|enable)\s+row\s+level\s+security/i)
    {
        print "Skipping <$ddl> we don't care about RLS\n";
    }
    elsif ($ddl =~ /^\s*alter\s*table\s*\S*.*\bADD\b.*\bCHECK\b/i)
    {
        print "Skipping <$ddl> we don't care about CHECK constraints\n";
    }
    elsif ($ddl =~ /^\s*alter\s*table\s*\S*.*\bVALIDATE\s+CONSTRAINT/i)
    {
        print "Skipping <$ddl> we don't care about validation of CHECK constraints\n";
    }
    elsif ($ddl =~ /^\s*alter\s*table\s*\S*.*\bOWNER\s+TO/i)
    {
        print "Skipping <$ddl> we don't care about ownership\n";
    }
    elsif ($ddl =~ /^\s*(CREATE|ALTER|DROP)(\s+OR\s+REPLACE)?\s+FUNCTION\s+/i)
    {
        print "Skipping <$ddl> we don't care about functions. They'll need a rewrite anyway\n";
    }
    elsif ($ddl =~ /^\s*(create|alter|drop)\s+(or replace\s+)*view/i)
    {
        # Crazy optimistic rewrite
        $fixed_ddl = $ddl;
        $fixed_ddl =~ s/^((?:CREATE|ALTER|DROP)\s+(or replace\s+)*view)\s+(\S+)\.(\S+)/$1 . " " .fix_schema($2,$database) ."." . $3/ie;

    }
    elsif ($ddl =~ /^\s*comment\s+on/i)
    {
        print "Skipping <$ddl> we don't care about comments\n";
    }
    elsif ($ddl =~ /^\s*(CREATE|ALTER|DROP)\s+TABLE/i)
    {
        # Crazy optimistic rewrite
        $fixed_ddl = $ddl;
        $fixed_ddl =~ s/^((?:CREATE|ALTER|DROP)\s+TABLE)\s+(\S+)\.(\S+)/$1 . " " .fix_schema($2,$database) ."." . $3/ie;
        # Let's also try to rewrite the schema if it's a set schema
        $fixed_ddl =~ s/(set\s+schema\s+)(\S+)/$1 . fix_schema($2,$database)/ie;
    }
    elsif ($ddl =~ /^\s*(CREATE|ALTER|DROP)\s+TYPE/i)
    {
        # Crazy optimistic rewrite
        $fixed_ddl = $ddl;
        $fixed_ddl =~ s/^((?:CREATE|ALTER|DROP)\s+TYPE)\s+(\S+)\.(\S+)/$1 . " " .fix_schema($2,$database) ."." . $3/ie;
    }
    elsif ($ddl =~ /^\s*(CREATE|ALTER|DROP)\s+SEQUENCE/i)
    {
        # Crazy optimistic rewrite
        $fixed_ddl = $ddl;
        $fixed_ddl =~ s/^((?:CREATE|ALTER|DROP)\s+SEQUENCE)\s+(\S+)\.(\S+)/$1 . " " .fix_schema($2,$database) ."." . $3/ie;
    }
    else
    {
        print ("Don't know what to do with: <$ddl>, checksum: <$ddl_md5>");
        confess();
    }
    # Special final fix for ALTER TABLE DROP CONSTRAINT: add IF EXISTS in case it's a constraint we haven't created before
    # This will happen for check constraints for instance
    if (defined($fixed_ddl) and $fixed_ddl =~ /^\s*ALTER\s+TABLE(.*)DROP\s+CONSTRAINT(.*)$/i)
    {
        my $to_keep_1 = $1;
        my $to_keep_2 = $2;
        # If the constraint doesn't have IF EXISTS, add it
        if ($fixed_ddl !~ /DROP\s+CONSTRAINT\s+IF\s+EXISTS/i)
        {
            $fixed_ddl = "ALTER TABLE${to_keep_1}DROP CONSTRAINT IF EXISTS${to_keep_2}";
        }
    }
    return $fixed_ddl;
}

# This function rewrites object name in certain cases
# Takes database/schema/table in input, and returns fixed in output
# If database is undef in output, the main function will ignore the record
# You can also, if necessary, modify $row, but this could get ugly
sub fix_object_dml
{
    my ($database, $schema, $table, $row) = @_;
    return ($database, $schema, $table);
};

# This function fixes search path. We always perform DDLs with no schema specified
# in the statements, and only fix the search path in the replay
# That's because we don't have a parsed query in what's captured by the ddl trigger
sub fix_search_path
{
    my ($orig_search_path, $database)=@_;
    my @new_search_path=split(/\s*,\s*/,$orig_search_path);
    # Remove the "$user" entry
    @new_search_path=grep(!/"\$user"/, @new_search_path);
    foreach my $path (@new_search_path)
    {
        $path =~ s/(.*)/${database}_${1}/;
    }
    my $new_search_path=join(', ', @new_search_path);
    return $new_search_path;
}
1;
