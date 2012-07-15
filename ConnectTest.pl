#!/usr/bin/perl

# load module
use DBI;
use Job;
use POSIX qw/strftime/;



# connect to database
my $dbh = DBI->connect("DBI:Pg:dbname=sr_data;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});

print "hello\n";

# clean up
#$dbh->disconnect();


