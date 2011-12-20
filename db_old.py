import sql

def to_dict(data):
   if data is None:
      return None
   d = {}
   for k,v in data.items():
      d[str(k)] = v
   return d

def parse_url(s):
   from urlparse import urlparse
   u = urlparse(s)
   d = {}
   if u.netloc:
      d['host'] = u.netloc
   if u.username:
      d['user'] = u.username
   if u.password:
      d['password'] = u.password
   if u.path:
      d['database'] = u.path[1:]
   return d

def connect(s):
   scheme, data = parse_url(s)
   if scheme == 'postgres':
      import psycopg2

      return psycopg2.connect(**data)

class DB(object):
   class DBException(Exception):
      def __init__(self, message, statement, parameters):
         self.message = message
         self.statement = statement
         self.parameters = parameters

      def __str__(self):
         v = 'SQL Error:  %s\nStatement:  %s' % (self.message, self.statement)
         if self.parameters:
            v += '\nParameters: %s' % self.parameters
         return v

   def __init__(self, connection):
      self.connection = connection
      self.sql = sql.SQL()      
      self.debug = False
      for k in ['escape','escape_field','fill','literal']:
         setattr(self, k, getattr(self.sql,k))

   def begin(self):
      self.connection.begin()

   def rollback(self):
      self.connection.rollback()

   def commit(self):
      self.connection.commit()

   def cursor(self):
      return self.connection.cursor()

   def close(self):
      self.connection.close()
      self.connection = None

   def __enter__(self):
      return self

   def __exit__(self, exc_type, exc_value, exc_traceback):
      if exc_type:
         self.rollback()
         raise exc_type, exc_value, exc_traceback
      else:
         self.commit()

   def execute(self, statement, *args, **kw):
      c = self.cursor()      
      if kw:
         s = self.sql.fill(statement, *args,  **kw)
      else:
         s = statement
      if self.debug:
         print s
      try:
         c.execute(s)
         return c
      except Exception, e:
         c.close()
         raise

   def execute_many(self, statement, *args):
      c = self.cursor()
      for a in args:
         self.execute(s, **a)

   def query_rows(self, statement, *args, **kw):
      cursor = self.execute(statement, *args, **kw)
      description, rows = cursor.description, cursor.fetchall()
      return [f[0] for f in description], rows

   def query_one(self, statement, *args, **kw):
      fields, rows = self.query_rows(statement, *args, **kw)
      if rows:
         return dict(zip(fields, rows[0]))

   def query_value(self, statement, *args, **kw):
      cursor = self.execute(statement, *args, **kw)
      description, rows = cursor.description, cursor.fetchall()
      return rows[0][0]

   def query(self, statement, *args, **kw):
      fields, results = self.query_rows(statement, *args, **kw)      
      return [dict(zip(fields, r)) for r in results]

   def select(self, **kw):
      statement = self.sql.select(**kw)
      return self.query(statement, **kw)

   def select_one(self, **kw):
      statement = self.sql.select(**kw)
      return self.query_one(statement, **kw)

   def select_children(self, **kw):
      key = kw['_key']      
      statement = self.sql.select(**kw)
      d = {}
      for r in self.query(statement, **kw):
         k = r.get(key)
         l = d.get(k,[])
         l.append(r)
         d[k] = l
      return d      

   def insert_rows(self, table, rows):
      if rows is None:
         return
      for r in rows:
         self.insert(table, r)

   def insert(self, table, data, returning=None):
      statement = self.sql.insert(table, data, returning=returning)
      if returning:
         return self.query(statement, **to_dict(data))
      else:
         return self.execute(statement, **to_dict(data))

   def update(self, table, data, id=None, id_field='id', _where=None, returning=None):
      statement = self.sql.update(table, data, id, id_field, _where, returning)
      if returning:
         return self.query(statement, **to_dict(data))
      else:
         return self.execute(statement, **to_dict(data))

   def delete(self, *args, **kw):
      statement = self.sql.delete(*args, **kw)
      if 'returning' in kw:
         return self.query(statement, kw)        
      else:
         return self.execute(statement, kw)        
   
   def get(self, table, id, id_field='id'):
      if type(id) == type([]):
         return self.query(self.sql.get(table, id))
      else:
         return self.query_one(self.sql.get(table, id))

   def put(self, table, data, id_field='id'):
      for s in self.sql.put(table, data, id_field):
         self.execute(s)

   def get_children(self, table, parent_field, parent):
      return self.query('select * from %s where %s=%s' % (table, self.escape_field(parent_field),self.escape(parent)))

   def insert_children(self, table, parent_field, parent, children):
      if children is None:
         return
      for c in children:
         d = dict(c)
         d[parent_field] = parent
         self.insert(table, d)

   def delete_children(self, table, parent_field, parent):
      return self.execute('delete from %s where %s=%s' % (table, self.escape_field(parent_field),self.escape(parent)))

   def put_children(self, table, parent_field, parent, children):
      self.delete_children(table, parent_field, parent)
      self.insert_children(table, parent_field, parent, children)
                           
