#!/bin/bash

usage="[--clean|--help]"

clean=false
help=false
error=false
make_target=lib-release

while [ $# -gt 0 ]
do
	case "$1" in
	--clean)
		clean=true ;;
	--debug)
		;;  # ignore
	--help)
		help=true ;;
	*)
		echo "Unknown args: $@" 1>&2
		help=true
		error=true ;;
	esac
	shift
done

if $help
then
	echo "Usage: $usage" 1>&2
	$error && exit 1
	exit
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)"
lib_dir=$script_dir/..
make_opt="-j $(($(grep -c processor /proc/cpuinfo)*2))"
$clean && make_target=clean

cd "$script_dir" || exit 1

make $make_opt $make_target || exit 1
