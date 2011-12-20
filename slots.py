class Record(object):
   __slots__ = ()
   def __init__(self, *args, **kw):
      slots = self.__slots__
      if args:
         if len(args) != len(slots):
            raise Exception('Incorrect number of arguments to __init__ - %d expected, %d received' % (len(slots), len(args)))
         for i in range(len(slots)):
            setattr(slots[i],args[i])
      else:
         for k in self.__slots__:
            setattr(self, k, None)
         for k,v in kw.items():
            setattr(self, k, v)

   def __str__(self):
      return self.__class__.__name__+ '(' + ', '.join(['%s=%s' % (k,repr(getattr(self,k))) for k in self.__slots__ if getattr(self,k) is not None])+')'

class User(Record):
   __slots__ = ('id','type','status','email','password','first_name','last_name')

   @property
   def full_name(self):
      return self.first_name+' '+self.last_name

u = User(id=1,type='User',status='Active',email='jcsoo@agora.com',password='test',first_name='Jonathan',last_name='Soo')
print u
print u.id
print u.first_name
print u.full_name

