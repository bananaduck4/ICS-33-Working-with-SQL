import p2app.events.continents as evc
from p2app.engine.database_manager import DB_Manager
from sqlite3 import IntegrityError
from p2app.events import  ErrorEvent

def _convert_to_namedtuple(row : list[tuple]) -> list:
    return [evc.Continent(*item) for item in row]

class Continent_Event_Manager(DB_Manager):
    def __init__(self, database):
        super().__init__(database, 'continent')
        self.req_col = set(evc.Continent(1, 'asd', 'asd')._asdict().keys())

    def not_valid_db(self) -> str | bool:
        if not self._has_table():
            return f'Database is mising a {self.table} table'

        elif self.req_col != self.columns:
            missing = self.req_col - self.columns
            return f'{self.table} is missing the following columns: {missing}'

        return False

    def _save(self, event : evc.SaveNewContinentEvent | evc.SaveContinentEvent) \
            -> evc.ContinentSavedEvent | evc.SaveContinentFailedEvent:
        continent = event.continent()

        if isinstance(event, evc.SaveNewContinentEvent):
            new_change = self._insert(continent)

        elif isinstance(event, evc.SaveContinentEvent):
            new_change = self._update(continent)

        if type(new_change) == IntegrityError:
            return evc.SaveContinentFailedEvent(new_change)

        return evc.ContinentSavedEvent(evc.Continent(*new_change))

    def run(self, event):
        if isinstance(event, evc.StartContinentSearchEvent):
            seach_value = {'continent_code' : event.continent_code(), 'name': event.name()}
            result = _convert_to_namedtuple(self._search(seach_value))
            for c in result:
                yield evc.ContinentSearchResultEvent(c)

        elif isinstance(event, evc.LoadContinentEvent):
            search_value = {'continent_id' : event.continent_id()}
            result = _convert_to_namedtuple(self._search(search_value))
            if result:
                yield evc.ContinentLoadedEvent(result[0])
            else:
                yield ErrorEvent('Fail to load continent')

        elif isinstance(event, evc.SaveNewContinentEvent) or isinstance(event, evc.SaveContinentEvent):
            yield self._save(event)