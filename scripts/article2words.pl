#!/usr/bin/perl

use warnings;
use strict;
use utf8::all;

use Storable;
#use File::Find;
use Data::Dumper;
use Lingua::FreeLing3::Utils qw/word_analysis/;
use Lingua::StopWords qw( getStopWords );
use FL3 'pt';

my $root = '/home/codebits/articles';
my $stopwords = getStopWords('pt');

my $c = 1;
#find(\&proc_article, $root);
@ARGV || die "Syntax: $0 FILE\n";
proc_article($ARGV[0]);

sub proc_article {
  my $path = shift;
#  next if $name =~ /^\.+$/;
  my $name = $path;
  $name =~ s/^\/?([^\/]+\/)+//g;
#  my $path = $File::Find::name;
#  print "name: $name ($path)\n";
  print STDERR "Working on $name\n";
  my $a = retrieve $path;
  my $date = $a->{InternalRefs}->{RegisterDate};
  $date =~ s/\.\d+Z$//;
  $date =~ s/[\-\:]//g;
  my $file = $date."_".$name;
  print "file: $file\n";
  my $data = {};
  foreach my $f qw(Title Description Body) {
    my $txt = $a->{$f};
    next if !$txt || $txt =~ /^\s*$/s;
#   my @words = split /\W+/, $txt;
    my $words = tokenizer->tokenize($txt);
    foreach (@{$words}) {
      my $word = $_->form;
      next if (exists $stopwords->{lc($word)});
      next unless $word;
      next if $word !~ /\w/;
      next if length($word) < 2; 	# FIXME: ganda martelada
      my $analysis = word_analysis({ l=>'pt' }, $word);
      my $xxx = shift @$analysis;
      my $lemma = $xxx->{lemma} || $word;
      $data->{$f} ||= {};
      $data->{$f}->{$lemma}++;
    }
    $data->{$f}->{_orig} = $txt;
  }
  store($data,"/home/codebits/words/$file");
  use Data::Dumper;
  print Dumper($data);
}

