# p2app/engine/main.py
#
# ICS 33 Spring 2024
# Project 2: Learning to Fly
#
# An object that represents the engine of the application.
#
# This is the outermost layer of the part of the program that you'll need to build,
# which means that YOU WILL DEFINITELY NEED TO MAKE CHANGES TO THIS FILE.

import p2app.events as ev
from p2app.engine.continent import Continent_Event_Manager
from p2app.engine.countries import Country_Event_Manager
from p2app.engine.region import Region_Event_Manager
import os.path

class Engine:
    """An object that represents the application's engine, whose main role is to
    process events sent to it by the user interface, then generate events that are
    sent back to the user interface in response, allowing the user interface to be
    unaware of any details of how the engine is implemented.
    """

    def __init__(self):
        """Initializes the engine"""
        self.cont_manager = None
        self.count_manager = None
        self.regi_manager = None

    def process_event(self, event):
        """A generator function that processes one event sent from the user interface,
        yielding zero or more events in response."""
        if isinstance(event, ev.QuitInitiatedEvent):
            yield  ev.EndApplicationEvent()

        elif isinstance(event, ev.OpenDatabaseEvent):
            path = event.path()

            self.cont_manager = Continent_Event_Manager(path)
            self.count_manager = Country_Event_Manager(path)
            self.regi_manager = Region_Event_Manager(path)

            if not os.path.isfile(path):
                yield ev.DatabaseOpenFailedEvent(f'{path} does not exists')

            elif not self.cont_manager:
                yield ev.DatabaseOpenFailedEvent(f'{path} is not a database')

            elif out := self.cont_manager.not_valid_db():
                yield ev.DatabaseOpenFailedEvent(out)

            elif out := self.count_manager.not_valid_db():
                yield ev.DatabaseOpenFailedEvent(out)

            elif out := self.regi_manager.not_valid_db():
                yield ev.DatabaseOpenFailedEvent(out)

            else:
                yield ev.DatabaseOpenedEvent(path)

        elif isinstance(event, ev.CloseDatabaseEvent):
            yield ev.DatabaseClosedEvent()

        elif 'Continent' in type(event).__name__:
            yield from self.cont_manager.run(event)

        elif 'Country' in type(event).__name__:
            yield from self.count_manager.run(event)

        elif 'Region' in type(event).__name__:
            yield from self.regi_manager.run(event)

        else:
            yield ev.ErrorEvent('Invalid Event.')

        # This is a way to write a generator function that always yields zero values.
        # You'll want to remove this and replace it with your own code, once you start
        # writing your engine, but this at least allows the program to run.
