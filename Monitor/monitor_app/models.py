from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from app import db

engine = create_engine('sqlite:///database.db', echo=True)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

# Set your classes here.


class User(Base):
    __tablename__ = 'Users'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True)
    email = db.Column(db.String(120), unique=True)
    password = db.Column(db.String(30))

    def __init__(self, name=None, password=None):
        self.name = name
        self.password = password

class Configuration(Base):
    __tablename__ = "Configurations"
    id = db.Column(db.Integer, primary_key=True)
    mongohost = db.Column(db.String(120), unique=True)
    mongouser = db.Column(db.String(120))
    mongopasswd = db.Column(db.String(120))
    pyplnhost = db.Column(db.String(120), unique=True)
    pyplnuser = db.Column(db.String(120))
    pyplnpasswd = db.Column(db.String(120))

    def __init__(self, mongohost=None, mongouser=None, mongopasswd=None, pyplnhost=None, pyplnuser=None, pyplnpasswd=None):
        self.mongohost = mongohost
        self.mongouser = mongouser
        self.mongopasswd = mongopasswd
        self.pyplnhost = pyplnhost
        self.pyplnuser = pyplnuser
        self.pyplnpasswd = pyplnpasswd

# Create tables.
Base.metadata.create_all(bind=engine)
