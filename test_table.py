import unittest
from db import DBTest
from table import *

d = DBTest()

class TestRecord(Record):
   pass

class TestTable(Table):
   fields = ('id','first_name','last_name')
   table = 'test_table'
   pk = 'test_id'   

register_type(TestTable, TestRecord)

class RecordTestCase(unittest.TestCase):
   def testEscape(self):
      pass


class TableTestCase(unittest.TestCase):
   def setUp(self):
      self.t = TestTable(d)

   def testSelect(self):
      self.assertEquals(self.t.select(),'select * from test_table')
      self.assertEquals(self.t.select(id=1),'select * from test_table where ("id"=1)')

if __name__ == '__main__':
   unittest.main()
