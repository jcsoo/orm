from collections import namedtuple
from table import Record
import sql

class DBTest(object):
   def query(self, statement, *args, **kw):
      return statement

class DB(object):
   def __init__(self, pool, commit_on_exit=False):
      self.pool = pool
      self.conn = None
      self.in_context = True
      self.commit_on_exit = commit_on_exit
      self.tables = {}
      self.debug = False

   def __len__(self):
      return len(self.tables)

   def __getitem__(self, k):
      return self.tables[k]

   def __setitem__(self, k, v):
      self.tables[k] = v

   def open(self):
      self.conn = self.pool.getconn()

   def rollback(self):
      if self.conn:
         self.conn.rollback()

   def commit(self):
      if self.conn:
         self.conn.commit()
      
   def close(self):
      if self.conn:
         self.pool.putconn(self.conn)
         self.conn = None

   def __enter__(self):
      self.in_context = True
      return self

   def __exit__(self, exc_type, exc_value, exc_traceback):
      if exc_type:
         self.close()
         self.in_context = False
         raise exc_type, exc_value, exc_traceback
      else:
         if self.commit_on_exit:
            self.commit()
         self.close()
         self.in_context = False

   def cursor(self, *args, **kw):
      if not self.in_context:
         raise Exception('Cursor only available when used in a context manager')
      if not self.conn:
         self.open()
      return self.conn.cursor()

   def execute(self, statement, *args, **kw):
      c = self.cursor()
      s = sql.fill(statement, *args, **kw)
      if self.debug:
         print s
      c.execute(s)
      return c

   def query_rows(self, statement, *args, **kw):
      cursor = self.execute(statement, *args, **kw)
      description, rows = cursor.description, cursor.fetchall()
      return [f[0] for f in description], rows


   def query_value(self, statement, *args, **kw):
      cursor = self.execute(statement, *args, **kw)
      description, rows = cursor.description, cursor.fetchall()
      return rows[0][0]

   def query_dicts(self, statement, *args, **kw):
      fields, results = self.query_rows(statement, *args, **kw)      
      return [dict(zip(fields, r)) for r in results]

   def query_dict(self, statement, *args, **kw):
      fields, rows = self.query_rows(statement, *args, **kw)
      if rows:
         return dict(zip(fields, rows[0]))

   def query(self, statement, *args, **kw):
      cursor = self.execute(statement, *args, **kw)
      if cursor.description is None:
         return None
      cls = kw.get('_factory',None)
      if cls is None:
         cls = namedtuple('Record',[f[0] for f in cursor.description])
         return [cls(*r) for r in cursor.fetchall()]
      else:
         return [cls(self, *r) for r in cursor.fetchall()]

   def query_one(self, statement, *args, **kw):
      rows = self.query(statement, *args, **kw)
      if rows:
         return rows[0]

   def query_lazy(self, *args, **kw):
      rows = self.query(*args, **kw)
      for r in rows:
         yield r