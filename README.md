# Zulip stream browser

## Data grabber

A Python script that scrapes the entirety of Zulip (there's surprisingly little!), saves it to Postgres, and then generates some simple summary info.

### Installation

Requires Python 3.4 and Postgres. `pip install -r requirements.txt`, then `python db.py` to create Postgres tables.

### Usage

`ZULIPSTREAM_KEY=apikey python zulipstream.py > zulipdata.json`

You can also set the `ZULIPSTREAM_EMAIL` (Zulip username) and `DATABASE_URL` environment variables to override the defaults.