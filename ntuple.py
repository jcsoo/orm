from collections import namedtuple

User = namedtuple('User','id type status email password first_name last_name',verbose=True)
default_user = User(*([None]*len(User._fields)))

u = User(id=1,type='User',status='Active',email='jcsoo@agora.com',password='test',first_name='Jonathan',last_name='Soo')
print u
k = default_user._replace(id=2,type='User',status='Active',email='kara@agora.com')
print k
