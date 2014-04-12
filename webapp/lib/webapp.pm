package webapp;
use Dancer ':syntax';

our $VERSION = '0.1';

use LWP::Simple qw//;
use Data::Dumper;
use JSON;
use HTML::TagCloud; 
use utf8::all;

my $provider = 'http://144.64.228.184:8080';
my $words_root = '/Users/smash/codebits/words_js';

get '/' => sub {
  my $req = "$provider/getTopics?start=20140301&end=20140331&cluster=true";
  my $json = LWP::Simple::get($req);
  return unless $json;
#print STDERR $json;
  
  my $data = from_json $json;

  my @clouds;
  foreach (@$data) {
    push @clouds, _build_cloud($_);
  }
print STDERR Dumper($data);

  template 'index' => { clouds => [@clouds], data =>$data };
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
