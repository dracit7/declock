#!/bin/bash

file=${1:?"Usage: calculator.sh [input_file] [pattern_to_match]"}
pattern=${2:?"Usage: calculator.sh [input_file] [pattern_to_match]"}

grep "$pattern" $file | \
awk -F"$pattern" '{print $2}' | \
awk 'BEGIN {SUM = 0.0000} {SUM += $1} END {printf "%.4f\n", SUM}'