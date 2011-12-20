import sql

def register_type(tclass, rclass):
   rclass._tclass = tclass
   tclass.rclass = rclass

class Record(object):
   _tclass = None
   def __init__(self, db, *args, **kw):
      self._db = db      
      fields = self._tclass.fields
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
      pk = self._tclass.pk
      if type(pk) == tuple:
         return tuple([getattr(self,k) for k in pk])
      else:
         return getattr(self, pk)

   def __repr__(self):
      return self.__str__()

   def __str__(self):      
      return self.__class__.__name__+ '(' + ', '.join(['%s=%s' % (k,repr(getattr(self,k))) for k in self._tclass.fields if k[0] != '_' and getattr(self,k) is not None])+')'

   def update(self, data):
      for k in self._tclass.fields:
         if k in data:
            setattr(self, k, data[k])
      self._tclass(self._db).update(data, _id=self._id)

   def updated(self, data):
      self.update(data)
      return self._tclass(self._db)[self._id]

   def delete(self):
      return self._tclass(self._db).delete(_id=self._id)

class Table(object):
   _rclass = Record
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

   def get(self, k, default=None):
      r = self.select_one(_where=sql.id_clause(k,self.pk))
      if r is None:
         r = default
      return r

   def filter(self, **kw):
      flist = [sql.escape_field(k)+'='+sql.escape(kw.pop(k)) for k in self.fields if k in kw]
      for k in kw:
         if hasattr(self, k):
            f = getattr(self,k)(kw.pop(k))
            if f:
               flist.append(f)
      return flist

   def select(self, **kw):
      q = {'_from' : self.table}
      q.update(kw)
      q.setdefault('_where_list',[]).extend(self.filter(**kw))
      return self.db.query(sql.select(**q),_factory=self.rclass)

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
      if '_id' in kw:
         kw['_id_field'] = self.pk
      kw.setdefault('_where_list',[]).extend(self.filter(**kw))        
      return self.db.query(sql.update(self.table, data, **kw))
   
   def delete(self, **kw):
      if '_id' in kw:
         kw['_id_field'] = self.pk
      kw.setdefault('_where_list',[]).extend(self.filter(**kw))        
      return self.db.query(sql.delete(self.table, **kw))

   def next_id(self):
      if self.pk_seq:
         return self.db.query_value('select nextval({pk_seq})',pk_seq=self.pk_seq)

