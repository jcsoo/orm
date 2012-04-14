import sys

from collections import namedtuple
from table import Table, Record
import sql

class DBTest(object):

   def query(self, statement, *args, **kw):
      return statement

   def query_one(self, statement, *args, **kw):
      return statement

class DB(object):
   def __init__(self, conn=None, pool=None):
      self.conn = conn
      self.pool = pool
      self.tables = {}
      self.modules = {}
      self.debug = False
      self.query_count = 0
      self.literal = sql.literal
      self.fill = sql.fill
      self.escape = sql.escape
      self.escape_field = sql.escape_field

   def __len__(self):
      return len(self.tables)

   def __getitem__(self, k):
      return self.tables[k]

   def __setitem__(self, k, v):
      self.tables[k] = v

   def log_debug(self, v):
      if self.debug:
         sys.stderr.write('%s\n' % v)
      return v
      
   def open(self):
      if self.conn is None and self.pool:
         self.conn = self.pool.getconn()
      return self.conn

   def rollback(self):
      if self.conn:
         self.conn.rollback()

   def commit(self):
      if self.conn:
         self.conn.commit()
      
   def close(self):
      if self.conn:
         if self.pool:
            self.pool.putconn(self.conn)
         self.conn = None

   def __enter__(self):
      return self

   def __exit__(self, exc_type, exc_value, exc_traceback):
      if exc_type:
         self.close()
         raise exc_type, exc_value, exc_traceback
      else:
         self.commit()
         self.close()

   def cursor(self, *args, **kw):      
      return self.open().cursor()

   def execute(self, statement, *args, **kw):
      self.query_count += 1
      c = self.cursor()
      c.execute(self.log_debug(sql.fill(statement, *args, **kw)))
      return c

   def query_rows(self, statement, *args, **kw):
      cursor = self.execute(statement, *args, **kw)
      description, rows = cursor.description, cursor.fetchall()
      return [f[0] for f in description], rows

   def query_value(self, statement, *args, **kw):
      cursor = self.execute(statement, *args, **kw)
      description, rows = cursor.description, cursor.fetchall()
      if rows:
         return rows[0][0]

   def query(self, statement, *args, **kw):
      cursor = self.execute(statement, *args, **kw)
      if cursor.description is None:
         return None
      fields = [f[0] for f in cursor.description]
      cls = kw.get('_factory',None)
      if cls is None:
         cls = dict
      return [cls(zip(fields,r)) for r in cursor.fetchall()]

   def query_one(self, statement, *args, **kw):
      rows = self.query(statement, *args, **kw)
      if rows:
         return rows[0]

   def query_lazy(self, *args, **kw):
      for r in self.query(*args, **kw):
         yield r

      

      
