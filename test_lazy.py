import pg, db, sql, table

class Documents(table.Table):
   table = 'documents'
   fields = ('path','status','template','section','slug','title','subhead','summary','author','byline','thumbnail','link','source','source_link','categories','keywords','released','revised')
   pk = 'path'

   def _author(self, value, **kw):
      return 'author ilike %s' % sql.escape('%%%s%%' % value.lower())


pool = pg.make_pool(1,10,database='hello')

def test_lazy():
   with db.DB(pool) as d:
      result = d.query_lazy('select * from documents order by released desc limit 5')
      for r in result:
         print r.path, r.title

def test_lazy_select():
   with db.DB(pool) as d:
      documents = Documents(d)
      results = documents.select_lazy(_order='released desc',_limit=10)
      print 'got results'
      for r in results:
         print r.path, r.title

def test_count():
   with db.DB(pool) as d:
      documents = Documents(d)
      print documents.count()

def test_page():
   with db.DB(pool) as d:
      documents = Documents(d)
      p = documents.select_page(_order='released desc',_page=1,_page_size=5)
      print p
      print 'Page ',p['_page']
      for r in p['_records']:
         print r.released, r.title
      
      print '---'
      p = documents.select_page(_order='released desc',_page=2,_page_size=5)
      print p
      print 'Page ',p['_page']
      for r in p['_records']:
         print r.released, r.title

      print '---'
      p = documents.select_page(_order='released desc',section='Number of The Day',_page=4,_page_size=100)
      print p



if __name__ == '__main__':
   test_page()
