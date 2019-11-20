#!/usr/bin/perl

use strict;
use warnings;
use File::Find;
use Cwd;
use File::Basename;
use File::Path 'mkpath';

if ($#ARGV != 1) { die "Usage: (inDir) (outDir)"; }

my $inDir = $ARGV[0];
my $outDir = $ARGV[1];

if (! -d $inDir) { die "Directory not found: $inDir"; }

my $filterDir = 'com/toshiba/mwcloud/gs';
my $inFileFilter = '.*\.java';

my $startTag = '<div lang="ja">';
my $endTag = '<\/div><div lang="en">';
my $otherTag = '<\/div>';

my $wd = Cwd::getcwd();

if (! -d $outDir) { mkdir $outDir or die "$!" };
chdir $outDir or die "$!";
my $absOutDir = Cwd::getcwd();
chdir "$wd" or die "$!";

chdir "$inDir" or die "$!";
find(\&convert, "$filterDir");

sub convert {
	my $file = $_;
	my $dir = $File::Find::dir;

	if ($file !~ /${inFileFilter}$/) { return; }

	my $outSubDir = "$absOutDir/$dir";
	if (! -d $outSubDir) { mkpath $outSubDir or die "$!" };

	my $inPath = "$file";
	my $outPath = "$outSubDir/$file";

	# print "$inPath $outPath \n";

	open IN, "<$inPath" or die "failed to open file: $!";
	open OUT, ">$outPath" or die "failed to open file: $!";

	my $flag = 1;
	while (my $data = <IN>) {
		chomp $data;
		if ($data =~/${startTag}$/) { $flag = 0 }
		elsif ($data =~ /${endTag}$/) { $flag = 1 }
		elsif ($data =~ /${otherTag}$/) { }
		elsif ($flag) { print OUT "$data\n" }
	}

	close OUT;
	close IN;
}
