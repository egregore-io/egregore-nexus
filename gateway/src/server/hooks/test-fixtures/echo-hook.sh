#!/bin/sh
IFS= read -r _invocation
printf '%s' '{"action":"continue","metadata":{"runtime":"shell"}}'
