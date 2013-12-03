import sqlalchemy
from sqlalchemy import Table, Column, ForeignKey, ...
    Date, Float, Integer, String

Base = sqlalchemy.declarative_base()

class Conference(Base):
    __tablename__ = 'conferences'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    subdivision = Column(String)

class Game(Base):
    __tablename__ = 'games'
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    home = Column(Integer, ForeignKey('teams.id'))
    away = Column(Integer, ForeignKey('teams.id'))
    stadium = Column(Integer, ForeignKey('stadiums.id'))
    site = Column(String)

class Player(Base):
    __tablename__ = 'players'
    id = Column(Integer, primary_key=True)
    team = Column(Integer, ForeignKey('teams.id'))
    last_name = Column(String)
    first_name = Column(String)
    number = Column(Integer)
    year = Column(String)
    position = Column(String)
    height = Column(Float)
    weight = Column(Float)
    hometown = Column(String)
    home_state = Column(String)
    home_country = Column(String)
    last_school = Column(String)

class Stadium(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    city = Column(String)
    state = Column(String)
    capacity = Column(Integer)
    surface = Column(String)
    year_opened = Column(Integer)

    def __repr__(self):
        return "<Stadium(name=’%s’, location='%s, %s'>" % (
            self.name, self.city, self.state)

class Team(Base):
    __tablename__ = 'teams'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    conference = Column(Integer, ForeignKey('conferences.id'))
