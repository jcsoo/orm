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
      self.assertEquals(build_where(['abc','def']),'where (abc) AND (def)')
      self.assertEquals(build_where(('abc','def')),'where "abc"=\'def\'')
      self.assertEquals(build_where(('abc',(1,2,3))),'where "abc" in (1,2,3)')
      self.assertEquals(build_where({'abc' : 123,'def':456}),'where ("abc"=123) AND ("def"=456)')
      self.assertEquals(build_where(['xyz',{'abc' : 123,'def':456}]),'where (xyz) AND (("abc"=123) AND ("def"=456))')
      self.assertEquals(build_where(['xyz',('z',1,'<'),{'abc' : 123,'def':456}]),'where (xyz) AND ("z"<1) AND (("abc"=123) AND ("def"=456))')






if __name__ == '__main__':
   unittest.main()
