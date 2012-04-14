import unittest
from orm.pg import connect_pool
from orm.db import DB



class DBTestCase(unittest.TestCase):
   def setUp(self):
      self.pool = connect_pool(0,1,database='hello')
      self.db = DB(pool=self.pool)

   def tearDown(self):
      self.pool.closeall()

   def test_query(self):
      rows = self.db.query('select 1 as value')
      self.assertEquals(len(rows),1)
      self.assertEquals(rows[0]['value'],1)

   def test_queryOne(self):
      r = self.db.query_one('select 1 as value')
      self.assertEquals(r['value'],1)

   def test_queryValue(self):
      v = self.db.query_value('select 1 as value')
      self.assertEquals(v,1)

   def test_queryLazy(self):
      result = self.db.query_lazy('select 1 as value')
      self.assertEquals(self.db.query_count,0)
      for r in result:
         self.assertEquals(r['value'],1)

   def test_open_close(self):
      self.assertEquals(self.db.conn,None)
      self.db.open()
      self.assertNotEquals(self.db.conn,None)
      self.db.close()
      self.assertEquals(self.db.conn,None)

   def test_commit(self):
      self.db.execute('drop table if exists test_table')
      self.db.execute('create table if not exists test_table (id int primary key, name varchar);')
      self.db.commit()
      self.db.execute("insert into test_table (id,name) values (1,'Jonathan')")
      self.assertEquals('Jonathan',self.db.query_value('select name from test_table where id=1'))
      self.db.commit()
      self.assertEquals('Jonathan',self.db.query_value('select name from test_table where id=1'))
      self.db.execute('drop table if exists test_table')

   def test_rollback(self):
      self.db.execute('drop table if exists test_table1')
      self.db.execute('create table if not exists test_table1 (id int primary key, name varchar);')
      self.db.commit()
      self.db.execute("insert into test_table1 (id,name) values (1,'Jonathan')")
      self.assertEquals('Jonathan',self.db.query_value('select name from test_table1 where id=1'))
      self.db.rollback()
      self.assertEquals(None,self.db.query_one('select name from test_table1 where id=1'))
      self.db.execute('drop table if exists test_table1')

   def test_context(self):
      self.db.execute('drop table if exists test_table2')
      self.db.execute('create table if not exists test_table2 (id int primary key, name varchar);')
      self.db.commit()
      self.db.close()
      with self.db as d:
         d.execute("insert into test_table2 (id,name) values (1,'Jonathan')")
         self.assertEquals('Jonathan',d.query_value('select name from test_table2 where id=1'))
      self.assertEquals('Jonathan',self.db.query_value('select name from test_table2 where id=1'))

   def test_context_rollback(self):
      self.db.execute('drop table if exists test_table3')
      self.db.execute('create table if not exists test_table3 (id int primary key, name varchar);')
      self.db.commit()
      self.db.close()
      try:
         with self.db as d:         
            d.execute("insert into test_table3 (id,name) values (1,'Jonathan')")
            self.assertEquals('Jonathan',d.query_value('select name from test_table3 where id=1'))
            raise Exception('test')
      except:
         pass
      self.assertEquals(None,self.db.query_value('select name from test_table3 where id=1'))



if __name__ == '__main__':
   unittest.main()
