package webapp;
use Dancer ':syntax';

our $VERSION = '0.1';

use LWP::Simple qw//;
use Data::Dumper;
use JSON;
use HTML::TagCloud; 
use utf8::all;

my $provider = 'http://127.0.0.1:8080';
#my $words_root = '/Users/smash/codebits/words_js';

sub esc {
  my $data = shift;
  $data =~ s/["']//g;
  return $data;
}

get '/clouds/:start/:end' => sub {
  my $start = param 'start';
  my $end = param 'end';
  print STDERR "req clouds start $start end $end\n";

  my $req = "$provider/getTopics?start=$start&end=$end&cluster=true&contents=5";
  print "GET $req\n";
  my $json = LWP::Simple::get($req);
  return unless $json;

  my $data = from_json $json;
  unless ( @{$data} ) {
    my $req = "$provider/getTopics?start=$start&end=$end&cluster=true&contents=5";
  print "GET $req\n";
    my $json = LWP::Simple::get($req);
    return unless $json;
    $data = from_json $json;
  }

  my @clouds;
  foreach (@$data) {
    push @clouds, _build_cloud($_);
  }

  # Find images
  my $images = "";
  my $count = 0;
  foreach my $entry ( @{$data} ) {
    if ( $entry->{contents} && ref($entry->{contents}) eq "ARRAY" ) {
      foreach my $c ( @{$entry->{contents}} ) {
        if ( $c->{Image} ) {
          $images .= "<a href=\"".$c->{URL}."\" target=_new><img title=\"".esc($c->{Title})."\" src=\"$c->{Image}\"></a>";
          $count++;
          last;
        }
      }
      if ( $count >= 4 ) {
        last;
      }
    }
  }

  return join("<br><hr><br>\n", $images, @clouds);
};

get '/' => sub {
  my $req = "$provider/getTopics?start=20140301&end=20140331&cluster=true&contents=5";
  print "GET $req\n";
  my $json = LWP::Simple::get($req);
  return unless $json;
#print STDERR $json;
  
  my $data = from_json $json;

  my @clouds;
  foreach (@$data) {
    push @clouds, _build_cloud($_);
  }

  # Find images
  my $images = "";
  my $count = 0;
  foreach my $entry ( @{$data} ) {
    if ( $entry->{contents} && ref($entry->{contents}) eq "ARRAY" ) {
      foreach my $c ( @{$entry->{contents}} ) {
        if ( $c->{Image} ) {
          $images .= "<a href=\"".$c->{URL}."\" target=_new><img title=\"".esc($c->{Title})."\" src=\"$c->{Image}\"></a>";
          $count++;
          last;
        }
      }
      if ( $count >= 4 ) {
        last;
      }
    }
  }

  template 'index' => { images => $images, clouds => [@clouds], data =>$data };
};


sub _build_cloud {
  my $data = shift;

  my $cloud = HTML::TagCloud->new;

  foreach (keys %{ $data->{linked} }) {
    next unless $data->{linked}->{$_} > 1;
    $cloud->add_static($_, $data->{linked}->{$_});
  }

  my $html = $cloud->html_and_css(50);
  return $html;
}

true;
