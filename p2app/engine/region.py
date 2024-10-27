from p2app.engine.database_manager import DB_Manager
import p2app.events.regions as evr
from p2app.events import ErrorEvent
from sqlite3 import IntegrityError

def _convert_to_namedtuple(row : list[tuple]) -> list:
    return [evr.Region(*item) for item in row]

class Region_Event_Manager(DB_Manager):
    def __init__(self, database):
        super().__init__(database, 'region')
        self.req_col = set(evr.Region(1, 'ASD', 'ASD', 'ASD', 1, 1, 'ASD', 'ASd')._asdict().keys())

    def not_valid_db(self) -> str | bool:
        if not self._has_table():
            return f'Database is mising a {self.table} table'

        elif self.req_col != self.columns:
            missing = self.req_col - self.columns
            return f'{self.table} is missing the following columns: {missing}'

        return False

    def _save(self, event: evr.SaveNewRegionEvent | evr.SaveRegionEvent) \
            -> evr.RegionSavedEvent | evr.SaveRegionFailedEvent:
        region = event.region()

        if isinstance(event, evr.SaveNewRegionEvent):
            new_change = self._insert(region)

        elif isinstance(event, evr.SaveRegionEvent):
            new_change = self._update(region)

        if type(new_change) == IntegrityError:
            return evr.SaveRegionFailedEvent(new_change)

        return evr.RegionSavedEvent(evr.Region(*new_change))

    def run(self, event) -> list:
        if isinstance(event, evr.StartRegionSearchEvent):
            searc_val = {'region_code': event.region_code(), 'local_code': event.local_code(), 'name' : event.name()}
            result = _convert_to_namedtuple(self._search(searc_val))
            for r in result:
                yield evr.RegionSearchResultEvent(r)

        elif isinstance(event, evr.LoadRegionEvent):
            search_val = {'region_id': event.region_id()}
            result = _convert_to_namedtuple(self._search(search_val))
            if result:
                yield evr.RegionLoadedEvent(result[0])
            else:
                yield ErrorEvent('Fail to load region')

        elif isinstance(event, evr.SaveRegionEvent) or isinstance(event, evr.SaveNewRegionEvent):
            yield self._save(event)
