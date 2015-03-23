import logging
import json
import os
import sys
import time
from urllib.parse import urljoin

import requests

import db

EMAIL = os.environ.get('ZULIPSTREAM_EMAIL', 'streamhistory-bot@students.hackerschool.com')
APIKEY = os.environ['ZULIPSTREAM_KEY']

logging.basicConfig(level=logging.INFO)

def request(method, endpoint, **kwargs):
    kwargs['auth'] = (EMAIL, APIKEY)
    url = urljoin('https://api.zulip.com/v1/', endpoint)
    resp = requests.request(method, url, **kwargs)
    resp.raise_for_status()
    return resp

def get(endpoint, **kwargs):
    try:
        return request('GET', endpoint, **kwargs)
    except requests.HTTPError as e:
        if e.response and e.response.status_code == 429:
            print("Rate-limited on %s" % endpoint)
            time.sleep(2)
            return get(endpoint, **kwargs)
        raise

def get_messages(stream_name=None, anchor=18446744073709551615, num=2000):
    """
    Fetches :num messages from the server, returns a list of dicts.
    If :stream_name is provided, returns messages from the given stream (and, as of 3/2015, this
    works for the full stream history). If no :stream_name is provided, returns messages from all
    *subscribed* streams (and subscriptions are not retroactive: only messages from after you
    subscribed).
    :anchor is the ID of the most recent message to fetch. The default is basically maxint,
    and so will always get the latest messages.
    """
    # print("%s %s" % (stream_name, anchor))
    params = {
        'anchor': anchor,
        'num_before': num,
        'num_after': 0,
        'apply_markdown': 'false',
    }
    if stream_name:
        params['narrow'] = json.dumps([{"negated": False, "operator": "stream", "operand": stream_name}])
    resp = get('messages', params=params)
    time.sleep(0.5)
    return resp.json()['messages']

def get_messages_until(max_known_id, stream_name=None):
    """
    Generator allows iteration over all message dicts with an id greater than :max_known_id.
    """
    num = 200 # Don't get too many messages at first
    anchor = 18446744073709551615
    while True:
        messages = [m for m in get_messages(stream_name, anchor, num) if m['id'] > max_known_id]
        for message in messages:
            yield message
        if len(messages) != num:
            raise StopIteration
        # We should fetch more
        anchor = messages[-1]['id']
        num = 2000

def import_messages(session, max_known_id=0, stream_name=None):
    """
    Gets all messages up to :max_known_id, and saves them to the database.
    :session is a db.Session
    """
    for message in get_messages_until(max_known_id=max_known_id, stream_name=stream_name):
        if not session.query(db.Message).filter_by(id=message['id']).count():
            session.add(db.Message.from_json(message))
    session.flush()

def get_all_stream_names():
    """Returns a set of the names of all existing streams."""
    return set(s['name'] for s in get('streams').json()['streams'])

def subscribe_to_stream(stream_name):
    """Subscribes our bot user to the given stream."""
    logging.info("Subscribe to %s", stream_name)
    resp = request('PATCH', 'users/me/subscriptions', data={
        'add': json.dumps([{'name': stream_name}])
    })
    assert resp.json()['result'] == 'success'

def fetch_new_messages():
    """Brings our local DB up-to-date with Zulip."""
    session = db.Session()
    max_known_id = db.Message.max_id()

    # Find out what streams we're missing
    new_streams = get_all_stream_names() - set(s.name for s in session.query(db.Stream).all())
    for new_stream_name in new_streams:
        stream = db.Stream(name=new_stream_name)
        subscribe_to_stream(new_stream_name)
        session.add(stream)
        import_messages(session, stream_name=new_stream_name)

    # Fetch new messages from previously subscribed streams
    import_messages(session, max_known_id=max_known_id)
    session.commit()

def generate_stream_json():
    """Returns a dict of data of stream stats, suitable for JSON serialization."""
    session = db.Session()
    streams = session.query(db.Stream).all()
    result = []
    for stream in streams:
        timestamp = stream.last_message_timestamp()
        result.append(dict(
            name=stream.name,
            counts=dict(list(d) for d in stream.daily_counts()),
            subjects=[list(d) for d in stream.top_subjects()],
            people=[dict(d) for d in stream.top_users()],
            last_message_timestamp=timestamp.timestamp() if timestamp else None
        ))
    result.sort(key=lambda o: sum(o['counts'].values()), reverse=True)
    return result

if __name__ == '__main__':
    fetch_new_messages()
    data = generate_stream_json()
    json.dump(data, sys.stdout) #, indent=4)
