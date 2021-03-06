============
 Greenpoint
============

Greenpoint is a portfolio management tool. It imports all transaction from your
broker(s) and store them into a PostgreSQL database.

It then allows to display your portfolio status and value.

Setup
=====
Use pip::

  pip install .

Once installed, edit `config.yaml` and add your account and the URL of your
PostgreSQL database. The database must exist.

You can then initialize the database with::

  $ createdb greenpoint
  $ export PGDATABASE=greenpoint
  $ make sql

You can then import all transactions::

  $ greenpoint broker import

To update instruments quotes::

  $ greenpoint instrument update

To display your portfolio::

  $ greenpoint portfolio show

To run the Web interface::

  $ greenpoint web

And connect on https://localhost:5000
