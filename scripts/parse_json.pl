#!/usr/bin/perl

use JSON::XS;
#use Storable;

sub store {
	my ($data,$f) = @_;
	open(F,">$f") || die "Error openning file '$f' for writing: $!\n";
	print F encode_json($data);
	close(F);
}

my $t = "";
while (<>) { $t .= $_; }

# Remove MongoDB crap
$t =~ s/(?:ISODate|ObjectId)\(("[^"]+")\)/$1/g;
$t =~ s/^MongoDB shell version.*$//mg;
$t =~ s/^connecting to.*$//mg;

# Parse
my $j = decode_json($t);
my $exported = 0;
foreach my $a ( @{$j} ) {
	store($a,"articles_js/$a->{_id}") || next;
	$exported++;
}
print STDERR "Exported $exported articles\n";
