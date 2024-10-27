import sqlite3 as s

def extract_asdict(tup) -> list:
    return tuple(tup._asdict().keys()), tuple(x if x != '' else None for x in tup._asdict().values())

def get_value_statements(tup) -> str:
    key, value = extract_asdict(tup)

    value_condition = ', '.join([f'{key} = (:{key})' for key in key[1:]])
    values = dict(zip(key[1:], value[1:]))
    return value_condition, values

def dict_to_condition(dic : dict) -> str:
    condition = ' AND '.join(f'{key} = (:{key})' for key, value in dic.items() if value)
    return condition

def get_error_msg(error : s.IntegrityError):
    if 'FOREIGN' in error.__str__():
        return 'ID does not exists in database.'
    else:
        reason = error.__str__().split()[-1].split('.')[-1].split('_')
        reason = ' '.join(reason).title()
        if 'UNIQUE' in error.__str__():
            return f'{reason} must be unique.'
        elif 'NULL' in error.__str__():
            return f'{reason} cannot be empty'

class DB_Manager:
    def __init__(self, database : str, table : str):
        self.database = database
        self.table = table
        self.connection = None
        self.columns = None

        if self:
            self._get_col()

    def __bool__(self):
        self.connection = s.connect(self.database)

        try:
            self.connection.execute("PRAGMA integrity_check")

        except s.DatabaseError:
            self.connection.close()
            return False

        else:
            return True

    def _has_table(self):
        self._connect()
        table = self.connection.execute(f"SELECT name FROM sqlite_master WHERE name = (?)",
                                        (self.table,))

        if not table.fetchone():
            self.connection.close()
            return False
        return True

    def _get_col(self):
        self._connect()
        col = self.connection.execute(f'PRAGMA table_info({self.table});')
        self.columns = {c[1] for c in col.fetchall()}
        self.connection.close()


    def _database_select(self, condition : str, value : dict) -> list[tuple]:
        self._connect()

        out = self.connection.execute(f'''
                    SELECT *
                    FROM {self.table}
                    WHERE {condition};''', value)

        out = out.fetchall()
        self.connection.close()
        return out

    def _search(self, search_value : dict) -> tuple:
        condition = dict_to_condition(search_value)

        return self._database_select(condition, search_value)

    def _insert(self, tup) -> tuple | s.IntegrityError:
        self._connect()

        key, value = extract_asdict(tup)
        insert_val = ['?' for i in range(len(value[1:]))]
        insert_val = ', '.join(insert_val)

        try:
            self.connection.execute(f'''
                INSERT INTO {self.table} {key[1:]}
                VALUES ({insert_val});''', value[1:])
            self.connection.commit()
            self.connection.close()

        except s.IntegrityError as e:
            self.connection.close()
            return s.IntegrityError(get_error_msg(e))

        condition = f'{key[1]} = (:code)'
        return self._database_select(condition, {'code' : value[1]})[0]

    def _update(self, tup) -> tuple | s.IntegrityError:
        self._connect()

        key, value = extract_asdict(tup)

        set_values, values = get_value_statements(tup)
        set_condition = f'{key[0]} = {value[0]}'

        try:
            self.connection.execute(f'''
                UPDATE {self.table}
                SET {set_values}
                WHERE {set_condition};''', values)
            self.connection.commit()
            self.connection.close()

        except s.IntegrityError as e:
            self.connection.close()
            return s.IntegrityError(get_error_msg(e))

        condition = f'{key[0]} =(:id)'
        return self._database_select(condition, {'id': value[0]})[0]

    def _connect(self):
        self.connection = s.connect(self.database)
        self.connection.execute('PRAGMA foreign_keys = ON;')