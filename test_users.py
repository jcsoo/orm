import pg, db, sql
from table import Record, Table, register_type

class Users(Table):
   table = 'login_users'
   fields = ('id','type','status','email','password','first_name','last_name')
   pk = 'id'
   pk_seq = 'login_users_id_seq'


#register_type(Users)

class UserRoles(Table):
   table = 'login_user_roles'
   fields = ('user','role')
   pk = ('user','role')

#register_type(UserRoles)


pool = pg.make_pool(1,10,database='hello')

def test_users():
   with db.DB(pool) as d:
      users = Users(d)
      print users.select()
      
      users.update({'status' : 'Inactive'},_id=1)
      u = users[1]
      print u._id
      u.update({'status' : 'Unknown'})
      u = users[u._id]
      print u
      u = u.updated({'status' : 'Test'})
      print u
      u.delete()
      print users.select()
      d.rollback()
      print users.select()
      n = users.insert({'email' : 'test@agora.com'},returning='id')
      print n
      print users.select()
      d.rollback()

def test_user_roles():
   with db.DB(pool) as d:
      roles = UserRoles(d)
      roles.select()
      roles.insert({'user' : 1, 'role' : 'Tester'})
      roles.insert({'user' : 1, 'role' : 'Visitor'})
      roles.insert({'user' : 2, 'role' : 'Tester'})
      print roles.select()
      r = roles[(1,'Tester')]
      print r
      print r._id
      r.update({'role' : 'TestABC'})
      print roles.select()
      print 'DELETE'
      r.delete()
      print roles.select()
      print 'should have no role TestABC'
      roles.delete(user=1)
      print roles.select()
      print roles.select(user=1)
      print roles.select_one(user=1)
      
if __name__ == '__main__':
   test_user_roles()

