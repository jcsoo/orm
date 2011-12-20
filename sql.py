import re, sys, datetime, decimal

FILL_RE = re.compile('{(.*?)}')

class SQLLiteral(object):
   def __init__(s):
      self.s = s

   def __str__(self):
      return self.s

def literal(s):
   return SQLLiteral(s)

def escape_field(v):
   if v.find('"') >= 0:
      raise Exception('Invalid field identifier: %s' % v)
   return '"%s"' % v

def escape(v,blank_null=True):     
   t = type(v)
   if v is None:
      return 'null'
   elif isinstance(v,SQLLiteral):
      return v.s
   elif t in (int, long):
      return '%d' % v
   elif t in (float, decimal.Decimal):
      return str(v)
   elif t == list:
      return 'ARRAY[%s]' % ','.join([escape(x,blank_null) for x in v])      
   elif t == tuple:
      return '(%s)' % ','.join([escape(x,blank_null) for x in v])
   elif t == unicode:
      if blank_null and v == u'':
         return 'null'
      return escape(v.encode('utf8'),blank_null)
   elif t == type(''):
      if blank_null and v == '':
         return 'null'
      return "'" + v.replace("'","\'")+"'"
   else:
      return escape(unicode(v),blank_null)

def fill(statement, *args, **data):   
   if not args and not data:
      return statement
   d = dict(data)
   for i in range(len(args)):
      d[str(i)] = args[i]

   return FILL_RE.sub(lambda v: escape(d.get(v.group(1))), statement)

def to_list(v):
   if v is None:
      return []
   elif type(v) == list:
      return v
   else:
      return [v]

def to_tuple(v):
   if type(v) == tuple:
      return v
   else:
      return (v,)

def make_clause(c):
   if type(c) == tuple:
      k,v = c[:2]
      if len(c) > 2:
         op = c[2]
      else:
         if type(v) == tuple:
            op = ' in '
         else:
            op = '='
      return escape_field(k)+op+escape(v)
   elif type(c) == dict:
      return join_clauses('AND',c.items())
   else:
      return c

def join_clauses(c_op, c_list):
   if len(c_list) == 1:
      return make_clause(c_list[0])
   else:
      return (' '+c_op+' ').join(['(%s)' % make_clause(c) for c in c_list])


def build_where(w, w_op='AND'):
   where_clause = join_clauses(w_op, to_list(w))
   if where_clause:
      return 'where ' + where_clause


def select(**kw):
   kw = dict(kw)
   s = ['select']

   s.append(kw.pop('_fields','*'))

   if kw.get('_from'):
      s.append('from %s' % kw.pop('_from'))

   #if kw.get('_where') is not None or kw.get('_filter') is not None:
   #   s.append(build_where(kw.pop('_where',''),kw.pop('_where_op','AND'),kw.pop('_filter',[])))

   s.append(build_where(kw))

   if kw.get('_group'):
      s.append('group by %s' % kw.pop('_group'))   

   if kw.get('_order'):
      s.append('order by %s' % kw.pop('_order'))

   if kw.get('_limit'):
      s.append('limit %s' % kw.pop('_limit'))

   if kw.get('_offset'):
      s.append('offset %s' % kw.pop('_offset'))

   #if kw.keys():
   #   raise Exception('Unknown keyword(s): %s' % kw.keys())

   return ' '.join([c for c in s if c])

def insert(table, data, returning=None):
   fields, values = [], []
   for k,v in data.items():
      fields.append(escape_field(k))
      values.append(escape(v))
   q = "insert into %s (%s) values (%s)" % (table, ','.join(fields),','.join(values))
   if returning:
      q += ' returning %s' % returning
   return q

def update(table, data, **kw):
   s = ['update %s set' % table]
   for k,v in data.items():
      s.append(escape_field(k)+'='+escape(v))
   s.append(build_where(kw))
   if kw.get('_returning'):
      s.append('returning %s' % kw['returning'])
   return ' '.join([c for c in s if c])   

def delete(table, **kw):
   s = ['delete from %s' % table]
   s.append(build_where(kw))
   if kw.get('_returning'):
      s.append('returning %s' % kw['returning'])
   return ' '.join([c for c in s if c])   



def update_1(table, data, id=None, id_field='id', _where=None, _where_list=None, returning=None):
   s = []
   for k,v in data.items():
      s.append(escape_field(k)+'='+escape(v))
   if id:
      w = 'where '+id_clause(id, id_field)
   elif _where:
      w = build_where(_where, w_filter=_filter)
   else:
      raise Exception('id or where clause must be specified')

   q = "update %s set %s %s" % (table, ','.join(s), w)
   if returning:
      q += ' returning %s' % returning
   return q


def delete_1(table, id=None, id_field='id', _where=None, _where_list=None,returning=None):
   if id:
      w = 'where ' + id_clause(id, id_field)
   elif _where or _filter:
      w = build_where(_where,w_filter=_filter)
   else:
      raise Exception('id or filter or where clause must be specified')

   q = 'delete from %s %s' % (table, w)
   if returning:
      q += ' returning %s' % returning
   return q

