import sql

def register_type(tclass, rclass=None):
   if rclass is None:
      rclass = Record
   tclass.rclass = rclass

class NotExist(object):
   pass

class Record(object):
   def __init__(self, db, table, *args, **kw):
      self._db = db
      self._table = table
      self._cache = {}
      fields = self._table.fields
      if args:
         if len(args) != len(fields):
            print 'fields',fields
            print 'args',args
            raise Exception('Incorrect number of arguments to __init__ - %d expected, %d received' % (len(fields), len(args)))
         for i in range(len(fields)):
            setattr(self, fields[i], args[i])
      else:
         for k in fields:
            setattr(self, k, None)
         for k,v in kw.items():
            setattr(self, k, v)

   def __getitem__(self, k):
      v = self.get(k, NotExist)
      if v == NotExist:
         if hasattr(self, k):
            return getattr(self, k)
         else:
            raise KeyError(k)
      return v

   def __setitem__(self, k, v):
      self.set(k,v)

   def __contains__(self, key):
      return self.has_key(key)

   def get(self, k, default=None):
      return getattr(self, k, default)

   def set(self, k, v):
      setattr(self, k, v)

   def keys(self):
      return self._table.fields

   def items(self):
      return [(k,getattr(self,k)) for k in self.keys()]

   def has_key(self, key):
      return hasattr(self, key)


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
   name = None
   table = None
   fields = []
   pk = None
   pk_seq = None

   def __init__(self, db, table=None):
      self.db = db
      if table:
         self.table = table

   def __str__(self):
      return self.__class__.get_name()

   @classmethod
   def get_name(cls):
      return cls.name or cls.__name__

   def __getitem__(self, k):
      v = self.get(k)
      if v is None:
         raise KeyError(k)
      else:
         return v
      
   def _get_id(self, r):
      pk = self.pk
      if type(pk) == tuple:
         return tuple([r[k] for k in pk])
      else:
         return r[k]
      
   def record(self, db, *args, **kw):
      return self.rclass(db, self, *args)

   def get(self, k, default=None):
      r = self.select_one(_where=sql.id_clause(self.pk,k))
      if r is None:
         r = default
      return r

   def get_or_new(self, k, default=None):
      if k is None:
         if default is None:
            return {}
         else:
            return default
      else:
         return self[k]
   
   def filter(self, kw):
      keys = kw.keys()
      flist = []
      for k in keys:
         if hasattr(self, k):
            f = getattr(self,k)(kw[k])
            if f:
               flist.append(f)
         elif k == '_id':
            flist.append(sql.id_clause(self.pk, kw.pop('_id')))
            continue
         elif k in self.fields:
            flist.append(('=',k,kw[k]))
            continue
      return flist


   def filter_old(self, kw):
      flist = [('=',k,kw.pop(k)) for k in self.fields if k in kw]
      if '_id' in kw:
         flist.append(sql.id_clause(self.pk, kw.pop('_id')))
      for k in kw:
         if hasattr(self, k):
            f = getattr(self,k)(kw[k])
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

   def select_value(self, **kw):
      q = {'_from' : sql.escape_field(self.table)}
      q.update(kw)
      q['_where'] = self.where(q)
      return self.db.query_value(sql.select(**q))
      

   def select_lazy(self, **kw):
      q = {'_from' : sql.escape_field(self.table)}
      q.update(kw)
      q['_where'] = self.where(q)
      return self.db.query_lazy(sql.select(**q),_factory=self.record)

   def select_page(self, **kw):
      page = kw.get('_page',1)
      page_size = kw.get('_page_size',None)
      order = kw.get('_order',None)
      if page_size:
         q = dict(kw)
         q['_offset'] = (page-1)*page_size
         q['_limit'] = page_size
         records = self.select_lazy(**q)
         record_count = self.count(**q)
         record_first = q['_offset']
         record_last = min(record_count, q['_offset'] + page_size)

         page_count = record_count / page_size
         if record_count % page_size:
            page_count += 1         
      else:
         records = self.select_lazy(**kw)
         record_count = self.count(**kw)
         record_first = 1
         record_last = record_count
         page_size = record_count
         page_count = 1

      return {
         '_page' : page, '_page_size' : page_size, '_page_count' : page_count, '_order' : order, '_multiple_pages' : (page_count > 1),
         '_record_first' : record_first,'_record_last' : record_last,'_record_count' : record_count, 
         '_records' : records}

   def count(self, **kw):
      d = dict(kw)
      d['_fields'] = 'count(*)'
      d.pop('_order',None)
      d.pop('_limit',None)
      d.pop('_offset',None)
      return self.select_value(**d)

   def select_lazy(self, **kw):
      results = self.select(**kw)
      for r in results:
         yield r

   def insert(self, data, returning='*'):
      if isinstance(data, dict):
         d = dict([(k,data[k]) for k in self.fields if k in data])
      else:
         d = dict([(k,getattr(data,k)) for k in self.fields if hasattr(data,k)])      
      pk = self.pk
      if self.pk_seq and d.get(pk,None) is None:
         d[pk] = self.next_id()
      return self.db.query_one(sql.insert(self.table, d, returning))

   def update(self, data, **kw):
      q = {}
      q.update(kw)
      q['_where'] = self.where(q)
      return self.db.query(sql.update(self.table, data, **q))
   
   def delete(self, **kw):
      q = {}
      q.update(kw)
      q['_where'] = self.where(q)
      return self.db.query(sql.delete(self.table, **q))

   def next_id(self):
      if self.pk_seq:
         return self.db.query_value('select nextval({pk_seq})',pk_seq=self.pk_seq)

   def add_tags(self, tags, field, **kw):
      tags = sql.to_list(tags)
      for r in self.select(**kw):
         t_arr = r[field]
         if t_arr is None:
            t_arr = []
         modified = False
         for t in tags:
            if t not in t_arr:
               t_arr.append(t)
               modified = True
         if modified:
            self.update({field : t_arr},_id=r._id)

   def remove_tags(self, tags, field, **kw):
      tags = sql.to_list(tags)
      for r in self.select(**kw):
         t_arr = r[field]
         if not t_arr:
            continue      
         modified = False
         for t in tags:
            if t in t_arr:
               t_arr.remove(t)
               modified = True
         if modified:
            self.update({field : t_arr},_id=r._id)      


class VersionedTable(Table):
   live_class = None
   staging_class = None
   test_class = None

   def __init__(self, db):
      super(VersionedTable, self).__init__(db)
      if self.live_class:
         self.live_table = self.live_class(db)
      else:
         self.live_table = None
      if self.staging_class:
         self.staging_table = self.staging_class(db)
      else:
         self.staging_table = None
      if self.test_class:
         self.test_table = self.test_class(db)
      else:
         self.test_table = None

   def _materialize_record(self, r):
      if self.live_class:
         self.live_table.delete(id=r.id)
         if r.on_live:
            self.live_table.insert(r.as_dict())
      if self.staging_class:
         self.staging_table.delete(id=r.id)
         if r.on_staging:
            self.staging_table.insert(r)
      if self.test_class:
         self.test_table.delete(id=r.id)
         if r.on_test:
            self.test_table.insert(r)

   def _dematerialize_record(self, r):
      if self.live_table and r.on_live:
         self.live_table.delete(vid=r.vid)
      if self.staging_table and r.on_staging:
         self.staging_table.delete(vid=r.vid)
      if self.test_table and r.on_test:
         self.test_table.delete(vid=r.vid)
            
   def _select_affected(self, **kw):
      kw['_fields'] = '*'
      return self.select(**kw)

   def insert(self, data, **kw):      
      r = super(VersionedTable, self).insert(data, **kw)
      self._materialize_record(r)
      return r

   def update(self, data, **kw):
      r = super(VersionedTable, self).update(data, **kw)
      for m in self._select_affected(**kw):
         self._materialize_record(m)
      return r
   
   def delete(self, **kw):
      for m in self._select_affected(**kw):
         self._dematerialize_record(m)
      return super(VersionedTable, self).delete(**kw)

