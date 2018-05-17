#!/bin/sh
set -ex
git clone https://github.com/pedromslopes/antidote.git
cd antidote
git checkout aql_oriented_features
make rel
