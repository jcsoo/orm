from sql import *

def nest(v):
   print 'nest',v
   return '('+v+')'

def compile(c):
   print 'compile',c
   if type(c) == tuple:
      op, rest = c[0],c[1:]
      if op in ('not',):
         if len(rest) > 1:
            raise Exception("More than one argument passed to 'not' operator: %s" % rest)
         return op.upper()+' '+nest(compile(rest[0]))
      elif op in ('and','or'):
         return nest((' '+op.upper()+' ').join([compile(r) for r in rest]))
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
   elif type(c) == dict:
      return compile(tuple(['and'] + [('=',k,v) for k,v in c.items()]))
   else:
      return c

