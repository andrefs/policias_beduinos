#!/usr/bin/perl

use FL3 'pt';
use utf8;

binmode(STDOUT,":utf8");

$atomos = tokenizer('pt')->tokenize("Nós damos");
use Data::Dumper;
#print Dumper($atomos);
foreach $a ( @{$atomos} ) {
	my $word = $a->lc_form;
	print "word: $word\n";
}
