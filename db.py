import datetime
import os

import sqlalchemy
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

Base = declarative_base()
engine = sqlalchemy.create_engine(os.environ.get('DATABASE_URL', 'postgresql://postgres@localhost/zulip'))
Session = sessionmaker(bind=engine)

class Message(Base):
    __tablename__ = 'messages'

    id = Column(Integer, primary_key=True)

    text = Column(String)
    stream = Column(String, index=True)
    subject = Column(String)
    timestamp = Column(DateTime, index=True)
    sender_id = Column(Integer)
    sender_email = Column(String)
    sender_name = Column(String)
    is_bot = Column(Boolean, default=False)
    avatar_url = Column(String)

    @classmethod
    def from_json(cls, data):
        return cls(
            id=data['id'],
            text=data['content'],
            stream=data['display_recipient'],
            subject=data['subject'],
            sender_id=data['sender_id'],
            sender_email=data['sender_email'],
            sender_name=data['sender_full_name'],
            is_bot='-bot@students' in data['sender_email'] or 'blaggregator' in data['sender_email'],
            timestamp=datetime.datetime.fromtimestamp(data['timestamp']),
            avatar_url=data['avatar_url']
        )

    @classmethod
    def get_or_create(cls, session, data):
        q = list(session.query(cls).filter_by(id=data['id']))
        if q:
            return q[0]
        msg = cls.from_json(data)
        session.add(msg)
        return msg

    @classmethod
    def max_id(cls, stream_name=None):
        """Return the largest ID, either for all messages or a particular stream."""
        query = select([cls.id]).order_by(cls.id.desc()).limit(1)
        if stream_name:
            query = query.where(cls.stream == stream_name)
        result = engine.execute(query)
        return result.fetchone()[0]


class Stream(Base):

    __tablename__ = 'streams'
    name = Column(String, primary_key=True)

    def __repr__(self):
        return '<Stream: {}>'.format(self.name)

    def daily_counts(self, max_age=datetime.timedelta(days=30)):
        return engine.execute(sqlalchemy.text(
            """SELECT EXTRACT(days FROM NOW() - timestamp) AS days_ago, COUNT(*) FROM messages
            WHERE stream = :stream AND timestamp > :oldest AND NOT is_bot
            GROUP BY days_ago
            ORDER BY days_ago ASC"""
            ), stream=self.name, oldest=datetime.datetime.now() - max_age)

    def top_subjects(self, max_age=datetime.timedelta(days=30), limit=3):
        return engine.execute(sqlalchemy.text(
            """SELECT subject, COUNT(*) AS cnt FROM messages
            WHERE stream = :stream AND timestamp > :oldest AND NOT is_bot
            GROUP BY subject
            ORDER BY cnt DESC
            LIMIT :limit"""
            ), stream=self.name, oldest=datetime.datetime.now() - max_age, limit=limit)

    def top_users(self, max_age=datetime.timedelta(days=30), limit=3):
        return engine.execute(sqlalchemy.text(
            """SELECT sender_name AS name, avatar_url, COUNT(*) AS num_messages FROM messages
            WHERE stream = :stream AND timestamp > :oldest AND NOT is_bot
            GROUP BY name, avatar_url
            ORDER BY num_messages DESC
            LIMIT :limit"""
            ), stream=self.name, oldest=datetime.datetime.now() - max_age, limit=limit)

    def last_message_timestamp(self):
        result = engine.execute(
            select([Message.timestamp])
                .where(Message.stream == self.name)
                .order_by(Message.timestamp.desc())
                .limit(1)
        ).first()
        return result[0] if result else None

    def max_id(self):
        return Message.max_id(self.name)

if __name__ == '__main__':
    print("Creating all tables...")
    Base.metadata.create_all(engine)
