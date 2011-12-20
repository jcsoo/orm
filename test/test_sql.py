import unittest
import datetime, decimal
from sql import *

class SQLTestCase(unittest.TestCase):
   def testEscape(self):
      self.assertEquals(escape(None),"null")
      self.assertEquals(escape(''),"null")
      self.assertEquals(escape('',False),"''")
      self.assertEquals(escape(u''),"null")
      self.assertEquals(escape(u'',False),"''")
      self.assertEquals(escape('abc'),"'abc'")
      self.assertEquals(escape("a'bc"),"'a\'bc'")
      self.assertEquals(escape(123),"123")
      self.assertEquals(escape(123L),"123")
      self.assertEquals(escape(123.456),"123.456")
      self.assertEquals(escape(decimal.Decimal('123.456')),"123.456")
      self.assertEquals(escape(datetime.date(2011,12,13)),"'2011-12-13'")
      self.assertEquals(escape(datetime.datetime(2011,12,13,1,2,3,123456)),"'2011-12-13 01:02:03.123456'")     

      self.assertEquals(escape([1,2,3]),"ARRAY[1,2,3]")
      self.assertEquals(escape((1,2,3)),"(1,2,3)")

   def testEscapeField(self):
      self.assertEquals(escape_field("abc"),'"abc"')
      
   def testEncoding(self):
      self.assertEquals(escape('\xe2\x86\x92'),"'\xe2\x86\x92'")
      self.assertEquals(escape(u'\u2192'),"'\xe2\x86\x92'")

   def testFill(self):
      d = {'s' : 'abc', 'n' : 123, 'a' : [1,2,3], 't' : (1,2,3)}
      self.assertEquals(fill('{0}','x'),"'x'")
      self.assertEquals(fill('{s}',**d),"'abc'")
      self.assertEquals(fill('{n}',**d),"123")
      self.assertEquals(fill('{a}',**d),"ARRAY[1,2,3]")
      self.assertEquals(fill('{t}',**d),"(1,2,3)")

   def testWhere(self):
      self.assertEquals(build_where(None),None)
      self.assertEquals(build_where([]),None)
      self.assertEquals(build_where({}),None)
      self.assertEquals(build_where('abc'),'where abc')
      self.assertEquals(build_where(['abc','def']),'where (abc AND def)')
      self.assertEquals(build_where(('=','abc','def')),'where "abc"=\'def\'')
      self.assertEquals(build_where(('=','abc',(1,2,3))),'where "abc" IN (1,2,3)')
      self.assertEquals(build_where({'abc' : 123,'def':456}),'where ("abc"=123 AND "def"=456)')
      self.assertEquals(build_where(['xyz',{'abc' : 123,'def':456}]),'where (xyz AND ("abc"=123 AND "def"=456))')
      self.assertEquals(build_where(['xyz',('<','z',1),{'abc' : 123,'def':456}]),'where (xyz AND "z"<1 AND ("abc"=123 AND "def"=456))')
      self.assertEquals(build_where(['xyz',('<','z',1),{'abc' : 123,'def':(1,2,3)}]),'where (xyz AND "z"<1 AND ("abc"=123 AND "def" IN (1,2,3)))')


   def testClause(self):
      self.assertEquals(compile_clause(None),None)
      self.assertEquals(compile_clause('abc'),'abc')
      self.assertEquals(compile_clause(('not','abc')),'NOT (abc)')
      self.assertEquals(compile_clause(('and','abc','def')),'(abc AND def)')
      self.assertEquals(compile_clause(('and','abc',('or','def','ghi'))),'(abc AND (def OR ghi))')
      self.assertEquals(compile_clause(('=','a',1)),'"a"=1')
      self.assertEquals(compile_clause(('=','a',(1,2,3))),'"a" IN (1,2,3)')
      self.assertEquals(compile_clause(('!=','a',(1,2,3))),'"a" NOT IN (1,2,3)')
      self.assertEquals(compile_clause({'a':1,'b':2}),'("a"=1 AND "b"=2)')
      self.assertEquals(compile_clause({'a':1,'b':(3,4,5)}),'("a"=1 AND "b" IN (3,4,5))')
      self.assertEquals(compile_clause(['abc','def']),'(abc AND def)')
      self.assertEquals(compile_clause(['abc',('or','def')]),'(abc AND def)')
      self.assertEquals(compile_clause(['abc',('or','def','ghi')]),'(abc AND (def OR ghi))')

   def testSelect(self):
      self.assertEquals(select(_from='abc',_where={'id':1},_order='name'),'select * from abc where "id"=1 order by name')
      self.assertEquals(select(_from='abc',_where={'id':(1,2,3)},_order='name'),'select * from abc where "id" IN (1,2,3) order by name')
      self.assertEquals(select(_from='abc',_where='id=1'),'select * from abc where id=1')

   def testInsert(self):
      self.assertEquals(insert('abc',{'a':1,'b':2}),'insert into "abc" ("a","b") values (1,2)')

   def testUpdate(self):
      self.assertEquals(update('abc',{'a':1,'b':2},_where={'id':1}),'update "abc" set "a"=1, "b"=2 where "id"=1')
      self.assertEquals(update('abc',{'a':1,'b':2},_where=('<','id',3)),'update "abc" set "a"=1, "b"=2 where "id"<3')
      self.assertEquals(update('abc',{'a':1,'b':2},_returning='*'),'update "abc" set "a"=1, "b"=2 returning *')

   def testDelete(self):
      self.assertEquals(delete('abc'),'delete from "abc"')
      self.assertEquals(delete('abc',_where={'id':1}),'delete from "abc" where "id"=1')
      self.assertEquals(delete('abc',_where=('<','id',3)),'delete from "abc" where "id"<3')
      self.assertEquals(delete('abc',_returning='*'),'delete from "abc" returning *')

   def testID(self):
      self.assertEquals(compile_clause(id_clause('id',1)),'"id"=1')
      self.assertEquals(compile_clause(id_clause('id',(1,2,3))),'"id" IN (1,2,3)')
      self.assertEquals(compile_clause(id_clause(('a','b'),(1,2))),'("a"=1 AND "b"=2)')
      self.assertEquals(compile_clause(id_clause(('a','b'),[(1,2),(3,4)])),'(("a"=1 AND "b"=2) OR ("a"=3 AND "b"=4))')


if __name__ == '__main__':
   unittest.main()
                        
