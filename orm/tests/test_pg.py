import unittest
from orm.pg import connect, connect_pool

class PGTestCase(unittest.TestCase):
   def doTestConnection(self, c):
      cur = c.cursor()
      cur.execute('select 1')
      rows = cur.fetchall()
      self.assertEquals(len(cur.description),1)
      self.assertEquals(len(rows),1)
      self.assertEquals(rows[0][0],1)

   def testConnect(self):
      self.doTestConnection(connect(database='hello'))
      
   def testConnectURL(self):
      self.doTestConnection(connect(url='postgres://localhost/hello'))

   def testConnectPool(self):
      p = connect_pool(0,1,database='hello')
      c = p.getconn()
      self.doTestConnection(c)
      p.putconn(c)

   def testConnectPoolURL(self):
      p = connect_pool(0,1,url='postgres://localhost/hello')
      c = p.getconn()
      self.doTestConnection(c)
      p.putconn(c)


if __name__ == '__main__':
   unittest.main()

