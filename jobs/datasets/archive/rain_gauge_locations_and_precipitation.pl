#!/usr/bin/perl -w
use strict;
use JSON;
#use Data::Dumper;
use Date::Calc qw(Add_Delta_Days Today Days_in_Month);
my($year,$month,$lastday,$startdate,$enddate,$path,$datapath)=($ARGV[0],$ARGV[1],"","","",'/opendata/web/water/rain.gauge/','/home/water/rain.gauge/data/');
if($month){
  $lastday=Days_in_Month($year,$month);
}
else{
  my @var=Today(); pop @var;($year,$month,$lastday)=Add_Delta_Days(@var,1,-1);
}
$month=sprintf("%02d",$month);
$startdate="$year$month"."01";
$enddate= "$year$month$lastday";

my $all_site_detail=cleandata('sites');
open(my $file, '>', $datapath."sites$year$month.json");print $file $all_site_detail;close $file;
my $site_json = decode_json $all_site_detail;
my $csv ='"id","name","longitude","latitude"'."\n";
$csv .= "$_->{id},$_->{name},$_->{longitude},$_->{latitude}\n" for(@{$site_json->{sites}});
open($file, '>', $datapath."sites$year$month.csv");print $file $csv;close $file;

my $full_site_list = decode_json $all_site_detail;#hash->array->hash
my @finaljson=();
$csv='"id","name","date","rainfall"'."\n";
for my $site_details (@{$full_site_list->{sites}}){
	my $site_specific_data= cleandata('site/'.$site_details->{id});#gives site details ID,Lat,Long,Name and channel
	my @allchannels= split(/},{/,$site_specific_data);
	for (@allchannels){
         	if ($_ =~ /Rainfall/ && $_ !~ /Daily/){
            		my $channelurl= $1 if ($_ =~ /id":(\d+)/);
            		$channelurl='site/'.$site_details->{id}.'/channel/'."$channelurl/data/startdate/$startdate/enddate/$enddate";
			my $get_data=cleandata($channelurl);
			my $rainfall_data=decode_json $get_data;
			for (@{$rainfall_data->{datapoints}}){
				$csv .= "$site_details->{id},$site_details->{name},$_->{date},$_->{value}\n";
			}
			$site_details->{"rainfall"}= $rainfall_data->{datapoints};#add rainfall node to json set
			$site_details->{"id"} = $site_details->{id}+0;
			push @finaljson,encode_json($site_details);#add json set to array
		}
	}
}

my $completejson="[".join(',',@finaljson)."]";#create complete json
my ($jsonrainfallfile, $jsonsitesfile, $csvrainfallfile, $csvsitesfile, $jsonzip, $csvzip,
$noteszip) = ($datapath."rainfall$year$month.json",
$datapath."sites$year$month.json",
$datapath."rainfall$year$month.csv", $datapath."sites$year$month.csv", $path."rainfall$year"."_json.zip", $path."rainfall$year"."_csv.zip",
$path."raingaugenotes.zip");


open($file, '>', $jsonrainfallfile); print $file $completejson; close $file;
open($file, '>', $csvrainfallfile); print $file $csv; close $file;
#`zip -ujr $jsonzip $jsonrainfallfile $jsonsitesfile`;
#`zip -ujr $csvzip $csvrainfallfile $csvsitesfile`;
#`wget --user=ftpcityoftoronto --password='X1UaT[uRkV*hV*tIEFJ,' ftp://ftp.flowworks.com/exports/fw_toronto_rg_datanotes.zip -O $noteszip`;

#`mail -s "checvKsubject" "gqi\@toronto.ca" < "Raingauge files have been generated.\n Please check.\n"`;


sub cleandata{
	my $urlvariant=shift;
	my $url="https://developers.flowworks.com/fwapi/v1/E8EF5292-F128-468D-BFBC-AEC1BE7AB8AC";
	my $datafromurl=`curl -s "$url/$urlvariant" -k`;print "$url/$urlvariant \n";#chop $datafromurl;
	$datafromurl = "{".$1 if ($datafromurl =~ /(?=gqi\"\,(.*))/);#chop $data; #remove credentials
	$datafromurl =~ s/\,\"channels\"\:null//g;
	return $datafromurl;
}
