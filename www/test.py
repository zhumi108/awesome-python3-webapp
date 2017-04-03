import orm, sys
from models import User, Blog, Comment
import asyncio

loop = asyncio.get_event_loop()

#创建实例
async def test():
	await orm.create_pool(loop=loop, host='localhost', port=3306, user='root', password='password', db='awesome')

	#创建一位用户
	new_user1 = User(id='00001', admin=True, name='Test1', email='test1@example.com', password='1234567890', image='about:blank', created_at=10000000)
	new_user2 = User(id='00002', admin=False, name='Test2', email='test2@example.com', password='234567890', image='about:blank', created_at=20000000)
	new_user3 = User(id='00003', admin=False, name='Test3', email='test3@example.com', password='34567890', image='about:blank', created_at=30000000)
	new_user4 = User(id='00004', admin=False, name='Test4', email='test4@example.com', password='4567890', image='about:blank', created_at=40000000)
	new_user5 = User(id='00005', admin=False, name='Test5', email='test5@example.com', password='567890', image='about:blank', created_at=50000000)
	
	await new_user1.save()
	await new_user2.save()
	await new_user3.save()
	await new_user4.save()
	await new_user5.save()

	results = await User.findAll()
	print('results: %s' % results)

loop.run_until_complete(test())
loop.close()
if loop.is_closed():
	sys.exit(0)