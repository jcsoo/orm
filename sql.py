import re, sys, datetime, decimal

FILL_RE = re.compile('{(.*?)}')

class Literal(object):
   def __init__(s):
      self.s = s

   def __str__(self):
      return self.s

def literal(s):
   return Literal(s)

def escape_field(v):
   if v.find('"') >= 0:
      raise Exception('Invalid field identifier: %s' % v)
   return '"%s"' % v

def escape(v,blank_null=True):     
   t = type(v)
   if v is None:
      return 'null'
   elif isinstance(v,Literal):
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

def nest(v):
   return '('+v+')'

def compile_clause(c):
   print c
   if c is None:
      return None
   if type(c) == tuple:
      op, rest = c[0],c[1:]
      if op in ('not',):
         if len(rest) > 1:
            raise Exception("More than one argument passed to 'not' operator: %s" % rest)
         return op.upper()+' '+nest(compile_clause(rest[0]))
      elif op in ('and','or'):
         rest = [compile_clause(r) for r in rest if r]
         if rest:
            return nest((' '+op.upper()+' ').join(rest))
      elif op == '=':
         if len(rest) > 2:
            raise Exception("More than two arguments passed to '=' operator: %s" % rest)
         if type(rest[1]) == tuple:
            op = ' IN '
         return nest(escape_field(rest[0]) + op + escape(rest[1]))
      elif op == '!=':
         if len(rest) > 2:
            raise Exception("More than two arguments passed to '!=' operator: %s" % rest)
         if type(rest[1]) == tuple:
            op = ' NOT IN '
         return nest(escape_field(rest[0]) + op + escape(rest[1]))
      else:
         if len(rest) > 2:
            raise Exception("More than two arguments passed to '%s' operator: %s" % (op,rest))
         return nest(escape_field(rest[0]) + op + escape(rest[1]))
   elif type(c) == list:
      if c:
         return compile_clause(tuple(['and'] + c))
   elif type(c) == dict:
      if c:
         return compile_clause(tuple(['and'] + [('=',k,v) for k,v in c.items()]))
   else:
      return c

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


def build_where(w):
   where_clause = compile_clause(w)
   if where_clause:
      return 'where ' + where_clause


def select(**kw):
   kw = dict(kw)
   s = ['select']

   s.append(kw.get('_fields','*'))

   if kw.get('_from'):
      s.append('from %s' % kw.get('_from'))

   if kw.get('_where'):
      s.append(build_where(kw.get('_where')))

   if kw.get('_group'):
      s.append('group by %s' % kw.get('_group'))   

   if kw.get('_order'):
      s.append('order by %s' % kw.get('_order'))

   if kw.get('_limit'):
      s.append('limit %s' % kw.get('_limit'))

   if kw.get('_offset'):
      s.append('offset %s' % kw.get('_offset'))

   return ' '.join([c for c in s if c])

def insert(table, data, returning=None):
   fields, values = [], []
   for k,v in data.items():
      fields.append(escape_field(k))
      values.append(escape(v))
   q = "insert into %s (%s) values (%s)" % (escape_field(table), ','.join(fields),','.join(values))
   if returning:
      q += ' returning %s' % returning
   return q

def update(table, data, **kw):
   s = ['update %s set' % escape_field(table)]
   s.append(','.join([escape_field(k)+'='+escape(v) for k,v in data.items()]))
   if kw.get('_where'):
      s.append(build_where(kw.get('_where')))
   if kw.get('_returning'):
      s.append('returning %s' % kw['_returning'])
   return ' '.join([c for c in s if c])   

def delete(table, **kw):
   s = ['delete from %s' % escape_field(table)]
   if kw.get('_where'):
      s.append(build_where(kw.get('_where')))
   if kw.get('_returning'):
      s.append('returning %s' % kw['_returning'])
   return ' '.join([c for c in s if c])   
