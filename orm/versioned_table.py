from table import Table

class VersionedTable(Table):
   views = []
   live_class = None
   staging_class = None
   test_class = None

   def __init__(self, db):
      super(VersionedTable, self).__init__(db)
      for v in self.views:
         cls = getattr(self, '%s_class' % v)
         if cls:
            setattr(self, '%s_table' % v, cls(db))                  

   def _materialize_record(self, r):
      for v in self.views:
         t = getattr(self, '%s_table' % v)
         t.delete(id=r.id)
         if getattr(r,'on_%s' % v):
            t.insert(r)

   def _dematerialize_record(self, r):
      for v in self.views:
         if getattr(r,'on_%s' % v):
            getattr(self, '%s_table' % v).delete(vid=r.vid)
            
   def _select_affected(self, **kw):
      kw = dict(kw)
      kw['_fields'] = '*'
      return self.select(**kw)

   def insert(self, data, **kw):      
      data = dict(data)
      if not data.get('id'):
         data['id'] = self.next_id(self.id_seq)
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

   def is_active(self, v):
      if v:
         return ['(on_staging is true or on_live is true)']
      else:
         return ['not (on_staging is true or on_live is true)']
