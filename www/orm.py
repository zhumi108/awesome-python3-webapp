import asyncio,  aiomysql
import logging; logging.basicConfig(level=logging.INFO)

def log(sql, args=()):
    logging.info('SQL: %s' % sql)

#创建一个全局连接池,每个http请求都可以从连接池中直接获取数据库连接,从而不必频繁地打开和关闭数据库连接,而是能复用就尽量复用
@asyncio.coroutine
def create_pool(loop, **kw):
	logging.info('create database connection pool...')
	#连接池由全局变量__pool存储, 缺省情况下将编码设置为utf8，自动提交事务：
	global __pool
	__pool = yield from aiomysql.create_pool(
		# **kw参数可以包含所有连接需要用到的关键字参数
		# 默认本机IP
		host=kw.get('host', 'localhost'),
		port=kw.get('port', 3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset', 'utf8'),
		autocommit=kw.get('autocommit', True),
		# 默认最大连接数为10
		maxsize=kw.get('maxsize', 10),
		minsize=kw.get('minsize', 1),
		# 接收一个event_loop实例
		loop=loop
		)

#创建执行select语句的函数,需要传入SQL语句和SQL参数
@asyncio.coroutine
def select(sql, args, size=None):
	log(sql, args)
	global __pool

	# -*- yield from 将会调用一个子协程，并直接返回调用的结果
	#  yield from从连接池中返回一个连接
	with (yield from __pool) as conn:
		# DictCursor is a cursor which returns results as a dictionary
		cur = yield from conn.cursor(aiomysql.DictCursor)

		# 执行SQL语句
		# SQL语句的占位符为?，MySQL的占位符为%s
		yield from cur.execute(sql.replace('?', '%s'), args or ())
		if size:
			rs = yield from cur.fetchmany(size)
		else:
			rs = yield from cur.fetchall()
		yield from cur.close()
		logging.info('rows returned: %s' % len(rs))
		return rs

#Insert, Updata, Delete语句
@asyncio.coroutine
def execute(sql, args):
	logging.info(sql)
	with (yield from __pool) as conn:
		try:
			# execute类型的SQL操作返回的结果只有行号，所以不需要用DictCursor
			cur = yield from conn.cursor()
			yield from cur.execute(sql.replace('?', '%s'), args)
			affected = cur.rowcount
			yield from cur.close()
		except Exception as e:
			raise
		return affected

# 根据输入的参数生成占位符列表
def create_args_string(num):
	L = []
	for n in range(num):
		L.append('?')
	 
	# 以','为分隔符，将列表合成字符串
	return (','.join(L))

# -*-定义Model的元类
 
# 所有的元类都继承自type
# ModelMetaclass元类定义了所有Model基类(继承ModelMetaclass)的子类实现的操作
 
# -*-ModelMetaclass的工作主要是为一个数据库表映射成一个封装的类做准备：
# ***读取具体子类(user)的映射信息
# 创造类的时候，排除对Model类的修改
# 在当前类中查找所有的类属性(attrs)，如果找到Field属性，就将其保存到__mappings__的dict中，同时从类属性中删除Field(防止实例属性遮住类的同名属性)
# 将数据库表名保存到__table__中
 
# 完成这些工作就可以在Model中定义各种数据库的操作方法
class ModelMetaclass(type):

	# __new__控制__init__的执行，所以在其执行之前
	# cls:代表要__init__的类，此参数在实例化时由Python解释器自动提供(例如下文的User和Model)
	# bases：代表继承父类的集合
	# attrs：类的方法集合
	def __new__(cls, name, bases, attrs):
		#排除Model类本身:
		if name == 'Model':
			return type.__new__(cls, name, bases, attrs)
		#获取table名称
		tableName = attrs.get('__table__', None) or name
		logging.info('found model: %s (table: %s)' % (name, tableName))
		#获取所有的Field和主键名:
		mappings = dict()
		fields = []
		primaryKey = None
		for k, v in attrs.items():
			if isinstance(v, Field):
				logging.info('found mapping: %s ==> %s' % (k, v))
				mappings[k] = v
				#找到主键
				if v.primary_key:
					# 如果此时类实例的以存在主键，说明主键重复了
					if primaryKey:
						raise RuntimeError('Duplicate primary key for field: %s' % k)
					primaryKey = k
				else:
					fields.append(k)
		if not primaryKey:
			raise RuntimeError('Primary key not found.')
		for k in mappings.keys():
			attrs.pop(k)

		# 保存除主键外的属性名为``（运算出字符串）列表形式
		escaped_fields = list(map(lambda f: '`%s`' % f, fields))
		attrs['__mappings__'] = mappings # 保存属性和列的映射关系
		attrs['__table__'] = tableName # 保存表名
		attrs['__primary_key__'] = primaryKey # 主键属性名
		attrs['__fields__'] = fields # 除主键外的属性名
		# 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
		# ``反引号功能同repr()
		attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
		attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
		attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
		return type.__new__(cls, name, bases, attrs)
		


# 定义ORM所有映射的基类：Model
# Model类的任意子类可以映射一个数据库表
# Model类可以看作是对所有数据库表操作的基本定义的映射
 
 
# 基于字典查询形式
# Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__，能够实现属性操作
# 实现数据库操作的所有方法，定义为class方法，所有继承自Model都具有数据库操作方法
class Model(dict, metaclass=ModelMetaclass):
	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)

	def __setattr__(self, key, value):
		self[key] = value

	def getValue(self, key):
		# 内建函数getattr会自动处理
		return getattr(self, key, None)

	def getValueOrDefault(self, key):
		value = getattr(self, key, None)
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s: %s' % (key, str(value)))
				setattr(self, key, value)
		return value

	@classmethod
	# 类方法有类变量cls传入，从而可以用cls做一些相关的处理。并且有子类继承时，调用该类方法时，传入的类变量cls是子类，而非父类
	@asyncio.coroutine
	def findAll(cls, where=None, args=None, **kw):
		'''find objects by where clause'''
		sql = [cls.__select__]

		if where:
			sql.append('where')
			sql.append(where)

		if args is None:
			args = []

		orderBy = kw.get('orderBy', None)
		if orderBy:
			sql.append('orderBy')
			sql.append(orderBy)

		limit = kw.get('limit', None)
		if limit is not None:
			sql.append('limit')
			if isinstance(limit, int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit, tuple) and len(limit) == 2:
				sql.append('?, ?')
				args.extend(limit)
			else:
				raise ValueError('Invalid limit value: %s' % str(limit))
		rs = yield from select(' '.join(sql), args)
		return [cls(**r) for r in rs]

	@classmethod
	@asyncio.coroutine
	def findNumber(cls, selectField, where=None, args=None):
		'''find number by select and where.'''
		sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
		if where:
			sql.append('where')
			sql.append(where)
		rs = yield from select(' '.join(sql), args, 1)
		if len(rs) == 0:
			return None
		return rs[0]['_num_']

	@classmethod
	@asyncio.coroutine
	def find(cls, pk):
		'''find object by primary key'''
		rs = yield from select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
		if len(rs) == 0:
			return None
		return cls(**rs[0])

	@asyncio.coroutine
	def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = yield from execute(self.__insert__, args)
		if rows != 1:
			logging.warn('failed to insert record: affected rows: %s' % rows)

	@asyncio.coroutine
	def update(self):
		args = list(map(self.getValue, self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows = yield from execute(self.__updata__, args)
		if rows != 1:
			logging.warn('failed to update by primary key: affected rows: %s' %rows)
 
	@asyncio.coroutine
	def remove(self):
		args = [self.getValue(self.__primary_key__)]
		rows = yield from execute(self.__updata__, args)
		if rows != 1:
			logging.warn('failed to remove by primary key: affected rows: %s' %rows)


# 定义Field类，负责保存(数据库)表的字段名和字段类型
class Field(object):
	def __init__(self, name, column_type, primary_key, default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default

	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


# -*- 定义不同类型的衍生Field -*-
# -*- 表的不同列的字段的类型不一样
class StringField(Field):
	def __init__(self, name=None, primary_key=False, default=None, column_type='varchar(100)'):
		super().__init__(name, column_type, primary_key, default)

class BooleanField(Field):
	def __init__(self, name=None, default=None):
		super().__init__(name, 'boolean', False, default)

class IntergerField(Field):
	def __init__(self, name=None, primary_key=False, default=0):
		super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):
	def __init__(self, name=None, primary_key=False, default=0.0):
		super().__init__(name, 'real', primary_key, default)

class TextField(Field):
	def __init__(self, name=None, default=None):
		super().__init__(name, 'Text', False, default)


if __name__ == '__main__':

	class User(Model):
		id = IntergerField('id', primary_key=True)
		name = StringField('username')
		email = StringField('email')
		password = StringField('password')

	user = User(id=12345, name='Jeremy', email='zhumi108@126.com', password='password')
	user.save()
	print(user)



