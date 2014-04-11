#!/usr/bin/perl

use JSON::XS;

my $HASH = {};

sub retrieve {
	my ($f) = @_;
	open(F,$f) || die "Could not open file '$f': $!\n";
	my $d = join('',<F>);
	close(F);
	return decode_json($d);
}
sub store {
	my ($d,$f) = @_;
	open(F,">$f") || die "could not open file '$f' for writing: $!\n";
	print F encode_json($d);
	close(F);
}

my $dir = $ARGV[0];
my $hashfile = $ARGV[1];
die "Syntax: $0 DIR OUT.json\n" unless -d $dir;
die "Syntax: $0 DIR OUT.json\n" unless $hashfile;

my $n = -1;
foreach my $f ( <$dir/*> ) {
	$n++;
	if ( ($n % 100) == 0 ) {
		print "$n\t$f\n";
	}

	my $id = $f;
	$id =~ s/^.*_||\.js$//g;
	my $words = retrieve($f);
	foreach my $field ( keys %{$words} ) {
		$HASH->{$field} ||= {};
		foreach my $word ( keys %{$words->{$field}} ) {
			$HASH->{$field}->{$word} ||= [];
			push @{$HASH->{$field}->{$word}}, [$id,$words->{$field}->{$word}];
#			$HASH->{$field}->{$word}++;
		}
	}
#	if ( ($n % 50000) == 0 ) {
#		store($HASH,"HUGE.json");
#	}
}
print "Storing..\n";
store($HASH,$hashfile);
