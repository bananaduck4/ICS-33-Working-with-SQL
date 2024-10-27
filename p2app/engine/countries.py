import p2app.events.countries as evc
from p2app.engine.database_manager import DB_Manager
from sqlite3 import IntegrityError
from p2app.events import  ErrorEvent

def _convert_to_namedtuple(row : list[tuple]) -> list:
    return [evc.Country(*item) for item in row]

class Country_Event_Manager(DB_Manager):
    def __init__(self, database):
        super().__init__(database, 'country')
        self.req_col = set(evc.Country(1, 'ASD', 'ASD', 1, 'asd', 'asd')._asdict().keys())

    def not_valid_db(self) -> str | bool:
        if not self._has_table():
            return f'Database is mising a {self.table} table'

        elif self.req_col != self.columns:
            missing = self.req_col - self.columns
            return f'{self.table} is missing the following columns: {missing}'

        return False

    def _save(self, event : evc.SaveCountryEvent | evc.SaveNewCountryEvent) \
        -> evc.CountrySavedEvent | evc.SaveCountryFailedEvent:
        country = event.country()

        if isinstance(event, evc.SaveNewCountryEvent):
            new_change = self._insert(country)

        elif isinstance(event, evc.SaveCountryEvent):
            new_change = self._update(country)

        if type(new_change) == IntegrityError:
            return evc.SaveCountryFailedEvent(new_change)

        return evc.CountrySavedEvent(evc.Country(*new_change))

    def run(self, event) -> list:
        if isinstance(event, evc.StartCountrySearchEvent):
            search_value = {'country_code': event.country_code(), 'name': event.name()}
            result = _convert_to_namedtuple(self._search(search_value))
            for c in result:
                yield evc.CountrySearchResultEvent(c)

        elif isinstance(event, evc.LoadCountryEvent):
            search_value = {'country_id' : event.country_id()}
            result = _convert_to_namedtuple(self._search(search_value))
            if result:
                yield evc.CountryLoadedEvent(result[0])
            else:
                yield ErrorEvent('Fail to load country')

        elif isinstance(event, evc.SaveCountryEvent) or isinstance(event, evc.SaveNewCountryEvent):
            yield self._save(event)