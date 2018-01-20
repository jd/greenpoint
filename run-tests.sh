#!/bin/sh

createdb greenpoint

make sql
make clean-sql
make sql

pytest
