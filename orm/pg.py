import psycopg2, psycopg2.pool
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

def parse_url(s):   
   from urlparse import urlparse
   u = urlparse(s)
   d = {}
   if u.netloc:
      d['host'] = u.netloc
   if u.username:
      d['user'] = u.username
   if u.password:
      d['password'] = u.password
   if u.path:
      d['database'] = u.path[1:]
   return d


def make_pool_url(minconn, maxconn, url):
   return psycopg2.pool.SimpleConnectionPool(minconn,maxconn,**parse_url(url))

def make_pool(minconn, maxconn, **kw):
   return psycopg2.pool.SimpleConnectionPool(minconn,maxconn,**kw)
