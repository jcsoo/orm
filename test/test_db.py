import pg, db, sql, table

pool = pg.make_pool(1,10,database='hello')

class Document(table.Record):
   @property
   def related_author(self):
      return Documents(self._db).select(_author=self.author,_order='released desc',_limit=10)

class Documents(table.Table):
   table = 'documents'
   fields = ('path','status','template','section','slug','title','subhead','summary','author','byline','thumbnail','link','source','source_link','categories','keywords','released','revised')
   pk = 'path'

   def _author(self, value, **kw):
      return 'author ilike %s' % sql.escape('%%%s%%' % value.lower())

table.register_type(Documents,Document)


def test_documents():
   with db.DB(pool) as d:
      documents = Documents(d)
      for r in documents.select(_order='released desc',_limit=5):
         print r.path
         #print sql.compile_clause(sql.id_clause(Documents.pk,r.path))
         print '  ',documents[r.path]
      return
      print '---'
      for r in documents.select(_order='released desc',_limit=50):
         print r.title.encode('utf8')
         print '  ',r.byline,r.released



if __name__ == '__main__':
   test_documents()
