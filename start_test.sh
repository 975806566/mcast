#!/bin/sh

ulimit -n 10240 
# +A 8 \
exec erl \
  +P 1002400 \
  +K true \
  -name mcast@127.0.0.1 \
  +zdbbl 8192 \
  -pa deps/*/ebin \
  -pa ebin \
  -config ./octopus.config \
  -boot start_sasl \
  -setcookie ilink \
  -s mcast start
# -s mod_test start -setcookie TEST_TEST_TEST
