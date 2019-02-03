#!/bin/sh
set -ex
git clone https://github.com/AntidoteDB/antidote.git
cd antidote
git checkout aql
make rel
