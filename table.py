import sql

def register_type(tclass, rclass=None):
   if rclass is None:
      rclass = Record
   tclass.rclass = rclass

class Record(object):
   def __init__(self, db, table, *args, **kw):
      self._db = db
      self._table = table
      fields = self._table.fields
      if args:
         if len(args) != len(fields):
            raise Exception('Incorrect number of arguments to __init__ - %d expected, %d received' % (len(fields), len(args)))
         for i in range(len(fields)):
            setattr(self, fields[i], args[i])
      else:
         for k in fields:
            setattr(self, k, None)
         for k,v in kw.items():
            setattr(self, k, v)

   @property
   def _id(self):
      pk = self._table.pk
      if type(pk) == tuple:
         return tuple([getattr(self,k) for k in pk])
      else:
         return getattr(self, pk)

   def __repr__(self):
      return self.__str__()

   def __str__(self):      
      return self.__class__.__name__+ '(' + ', '.join(['%s=%s' % (k,repr(getattr(self,k))) for k in self._table.fields if k[0] != '_' and getattr(self,k) is not None])+')'

   def as_dict(self):
      d = {}
      for k in self._table.fields:
         d[k] = getattr(self,k)
      return d

   def update(self, data):
      self._table.update(data, _id=self._id)
      for k in self._table.fields:
         if k in data:
            setattr(self, k, data[k])

   def updated(self, data):
      self.update(data)
      return self._table[self._id]

   def delete(self):
      return self._table.delete(_id=self._id)

class Table(object):
   rclass = Record
   table = None
   fields = []
   pk = None
   pk_seq = None

   def __init__(self, db):
      self.db = db

   def __str__(self):
      return self.__class__.__name__

   def __getitem__(self, k):
      v = self.get(k)
      if v is None:
         raise KeyError(k)
      else:
         return v

   def record(self, db, *args, **kw):
      return self.rclass(db, self, *args)

   def get(self, k, default=None):
      r = self.select_one(_where=sql.id_clause(self.pk,k))
      if r is None:
         r = default
      return r

   def filter(self, kw):
      flist = [('=',k,kw.pop(k)) for k in self.fields if k in kw]
      if '_id' in kw:
         flist.append(sql.id_clause(self.pk, kw.pop('_id')))
      for k in kw:
         if hasattr(self, k):
            f = getattr(self,k)(kw.pop(k))
            if f:
               flist.append(f)
      return flist

   def where(self, kw):      
      w = []
      w_list = kw.get('_where')
      if w_list:
         w.append(w_list)
      f_list = self.filter(kw)
      if f_list:
         w.append(f_list)         
      if len(w) == 1:
         return w[0]
      elif len(w) > 1:
         return tuple(['and'] + w)

   def select(self, **kw):
      q = {'_from' : sql.escape_field(self.table)}
      q.update(kw)
      q['_where'] = self.where(q)
      return self.db.query(sql.select(**q),_factory=self.record)

   def select_one(self, **kw):
      r = self.select(**kw)
      if r:
         if len(r) == 1:
            return r[0]  
         else:
            return Exception('Got %d results, expected one' % len(r))

   def insert(self, data, returning=None):
      pk = self.pk
      if self.pk_seq and data.get('pk') is None:
         data[pk] = self.next_id()

      return self.db.query(sql.insert(self.table, data, returning))

   def update(self, data, **kw):
      q = {}
      q.update(kw)
      q['_where'] = self.where(q)
      print sql.update(self.table, data, **q)
      return self.db.query(sql.update(self.table, data, **q))
   
   def delete(self, **kw):
      q= {}
      q.update(kw)
      q['_where'] = self.where(q)
      return self.db.query(sql.delete(self.table, **q))

   def next_id(self):
      if self.pk_seq:
         return self.db.query_value('select nextval({pk_seq})',pk_seq=self.pk_seq)

