import decimal, re
import psycopg2, psycopg2.pool
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)


def cast_money(value, cur):
   if value is None:
      return None
   value = value.replace('$','').replace(',','')
   m = re.match(r'^(-?)\d+\.\d+$', value)
   if m:
      return decimal.Decimal(value)
   else:
      raise psycopg2.InterfaceError('Bad money representation: %s' % value)

MONEY = psycopg2.extensions.new_type((790,),'MONEY',cast_money)
psycopg2.extensions.register_type(MONEY)

def parse_url(s):   
   from urlparse import urlparse
   u = urlparse(s)
   d = {}
   if u.netloc:
      n = u.netloc
      if '@' in n:
         n_arr = n.split('@')
         d['host'] = n_arr[1]
         if ':' in n_arr[0]:
            p_arr = n_arr[0].split(':')
            d['user'] = p_arr[0]
            d['password'] = p_arr[1]
         else:
            d['user'] = n_arr[0]
      else:
         d['host'] = u.netloc
   if u.path:
      d['database'] = u.path[1:]
   return d


def make_pool_url(minconn, maxconn, url):
   return psycopg2.pool.SimpleConnectionPool(minconn,maxconn,**parse_url(url))

def make_pool(minconn, maxconn, **kw):
   return psycopg2.pool.SimpleConnectionPool(minconn,maxconn,**kw)
