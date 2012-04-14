import unittest
from orm.db import DBTest
from orm.table import *

d = DBTest()

class TestRecord(Record):
   pass

class TestTable(Table):
   fields = ('id','first_name','last_name')
   table = 'test_table'
   pk = 'test_id'   


class TestRecord2(Record):
   pass

class TestTable2(Table):
   fields = ('id','first_name','last_name')
   table = 'test_table_2'
   pk = ('a','b')

class RecordTestCase(unittest.TestCase):
   def testEscape(self):
      pass


class TableTestCase(unittest.TestCase):
   def setUp(self):
      self.t = TestTable(d)
      self.t2 = TestTable2(d)

   def testSelect(self):
      self.assertEquals(self.t.select(),'select * from "test_table"')
      self.assertEquals(self.t.select(id=1),'select * from "test_table" where "id"=1')
      self.assertEquals(self.t.select(id=1,_where='xyz'),'select * from "test_table" where (xyz AND "id"=1)')
      self.assertEquals(self.t.select(_id=1),'select * from "test_table" where "test_id"=1')
      self.assertEquals(self.t.select(_id=[1,2]),'select * from "test_table" where "test_id" IN (1,2)')
      self.assertEquals(self.t.select(_id=(1,2)),'select * from "test_table" where "test_id" IN (1,2)')
      self.assertEquals(self.t2.select(_id=(1,2)),'select * from "test_table_2" where ("a"=1 AND "b"=2)')

   def testInsert(self):
      self.assertEquals(self.t.insert({'id' : 123, 'first_name':'123','last_name':'456'}),'insert into "test_table" ("first_name","last_name","id") values (\'123\',\'456\',123) returning *')

   def testUpdate(self):
      self.assertEquals(self.t.update({}),'update "test_table"')
      self.assertEquals(self.t.update({'abc':123,'def':456}),'update "test_table" set "abc"=123, "def"=456')
      self.assertEquals(self.t.update({'abc':123,'def':456},_where={'id':1}),'update "test_table" set "abc"=123, "def"=456 where "id"=1')
      self.assertEquals(self.t.update({'abc':123,'def':456},_id=[1,2]),'update "test_table" set "abc"=123, "def"=456 where "test_id" IN (1,2)')
      self.assertEquals(self.t.update({'abc':123,'def':456},_where={'id':1},_returning='*'),'update "test_table" set "abc"=123, "def"=456 where "id"=1 returning *')
      self.assertEquals(self.t2.update({'abc':123,'def':456},_id=(1,2)),'update "test_table_2" set "abc"=123, "def"=456 where ("a"=1 AND "b"=2)')

   def testDelete(self):
      self.assertEquals(self.t.delete(_where={}),'delete from "test_table"')
      self.assertEquals(self.t.delete(_where={'id':1}),'delete from "test_table" where "id"=1')
      self.assertEquals(self.t.delete(_id=1),'delete from "test_table" where "test_id"=1')
      self.assertEquals(self.t.delete(_id=(1,2)),'delete from "test_table" where "test_id" IN (1,2)')
      self.assertEquals(self.t.delete(_where={'id':1},_returning='*'),'delete from "test_table" where "id"=1 returning *')
      self.assertEquals(self.t2.delete(_id=(1,2)),'delete from "test_table_2" where ("a"=1 AND "b"=2)')

if __name__ == '__main__':
   unittest.main()
