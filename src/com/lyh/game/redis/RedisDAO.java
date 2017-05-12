package com.lyh.game.redis;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.persistence.Index;
import javax.persistence.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.lyh.game.lib.sql.DbEntity;
import com.lyh.game.lib.sql.ISqlDAO;

/** ClassName:RedisDao <br/>
 * Function: TODO (redis存储). <br/>
 * Reason: TODO (). <br/>
 * 一个对象可能有几个绑定 所以存储时要小心 Date: 2015-11-23 下午3:54:10 <br/>
 * 还有排行这些没有做
 * 
 * @author lyh
 * @version
 * @see */
@Component
public class RedisDAO implements ISqlDAO<DbEntity> {
	private static final Logger logger = LoggerFactory.getLogger(RedisDAO.class);
	
	
	private static final String redis = "myRedis:";
	
	/** 冒号 **/
	public static final String COCON = ":";
	
	// /** 等号 **/
	// public static final String EQUAL = "=";
	
	/** 过期时间(天) **/
	public static final int EXPIRE_TIME = 30 * 86400;
	
	// @Autowired
	// private EntityDAO entityDAO;
	
	@Autowired
	private SetRedisDAO setRedisDAO;
	
	@Autowired
	private StringRedisDAO<DbEntity> stringRedisDAO;
	
	
	/******************************* String **********************************************/
	
	/** getSetKey:(). <br/>
	 * TODO().<br/>
	 * 获得set的key值
	 * 
	 * @author lyh
	 * @param obj
	 * @param key
	 * @return */
	public static String getSetKey(Class<?> obj, String key, Object val) {
		
		return obj.getSimpleName() + COCON + key + COCON + val;
	}
	
	public static String getSetKey(Class<?> obj, List<String> key, List<Object> val) {
		String str = obj.getSimpleName();
		for (int i = 0; i < key.size(); i++) {
			str += COCON + key.get(i) + COCON + val.get(i);
		}
		return str;
	}
	
	/** isInRedis:(). <br/>
	 * TODO().<br/>
	 * 是否在reids里面
	 * 
	 * @author lyh
	 * @param <T>
	 * @param id
	 * @return */
	public <T> boolean isInRedis(Class<T> c, final long id) {
		String val = stringRedisDAO.redisGetFromString(c, "" + id);
		return val != null;
	}
	
	@Override
	public Long save(DbEntity transientInstance) {
		if (transientInstance == null) {
			logger.debug("redis can not save" + transientInstance);
			return -1L;
		}
		try {
			
			String json = JSON.toJSONString(transientInstance);
			String key = "" + transientInstance.getId();
			// 对象存入redis<id,val>
			stringRedisDAO.redisSaveToString(transientInstance, key, json, EXPIRE_TIME);
			Annotation defAnnotation = transientInstance.getClass().getAnnotation(Table.class);
			Table tInfo = (Table) defAnnotation;
			if (tInfo != null && tInfo.indexes() != null && tInfo.indexes().length > 0) {
				for (Index in : tInfo.indexes()) {
					Field fi = transientInstance.getClass().getDeclaredField(in.columnList());
					
					if (fi != null) {
						fi.setAccessible(true);
						Object obj = fi.get(transientInstance);
						String str = obj.toString();
						// Pattern pattern = Pattern.compile("[0-9]*");
						// boolean isMatch = pattern.matcher(str).matches();
						setRedisDAO.redisSaveToSet(getSetKey(transientInstance.getClass(), in.columnList(), str), "" + transientInstance.getId(), EXPIRE_TIME);
					}
				}
			}
			
			// 用set存入行号
			// redisSaveToSet(redis+val.getClass().getName() +COCON+key , key);
			logger.debug("redis can save" + key + ":val:" + transientInstance);
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/** saveMongo:(). <br/>
	 * TODO().<br/>
	 * 保存redis-mongo
	 * 
	 * @author lyh
	 * @param transientInstance
	 * @return */
	public Long saveMongo(DbEntity transientInstance) {
		if (transientInstance == null) {
			logger.debug("redis can not save" + transientInstance);
			return -1L;
		}
		try {
			String json = JSON.toJSONString(transientInstance);
			String key = "" + transientInstance.getId();
			// 对象存入redis<id,val>
			stringRedisDAO.redisSaveToString(transientInstance, key, json, EXPIRE_TIME);
			Field[] fields = transientInstance.getClass().getDeclaredFields();
			if (fields != null && fields.length > 0) {
				for (Field fie : fields) {
					Indexed meta = fie.getAnnotation(Indexed.class);
					if (meta != null) {
						fie.setAccessible(true);
						Object obj = fie.get(transientInstance);
						if (obj != null) {
							String str = obj.toString();
							
							// Pattern pattern = Pattern.compile("[0-9]*");
							// boolean isMatch = pattern.matcher(str).matches();
							setRedisDAO.redisSaveToSet(getSetKey(transientInstance.getClass(), fie.getName(), str), "" + transientInstance.getId(), EXPIRE_TIME);
						}
					}
				}
			}
			
			// 用set存入行号
			// redisSaveToSet(redis+val.getClass().getName() +COCON+key , key);
			logger.debug("redis can save" + key + ":val:" + transientInstance);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("redis can save:" + transientInstance.getId(), e);
		}
		return null;
	}
	
	@Override
	public void delete(DbEntity persistentInstance) {
		// TODO Auto-generated method stub
		try {
			if (persistentInstance == null) {
				return;
			}
			stringRedisDAO.deleteFromString(persistentInstance, "" + persistentInstance.getId());
			// 存入索引
			Annotation defAnnotation = persistentInstance.getClass().getAnnotation(Table.class);
			Table tInfo = (Table) defAnnotation;
			if (tInfo != null && tInfo.indexes() != null && tInfo.indexes().length > 0) {
				for (Index in : tInfo.indexes()) {
					
					Field fi = persistentInstance.getClass().getDeclaredField(in.columnList());
					
					if (fi != null) {
						fi.setAccessible(true);
						Object obj = fi.get(persistentInstance);
						String str = obj.toString();
						// Pattern pattern = Pattern.compile("[0-9]*");
						// boolean isMatch = pattern.matcher(str).matches();
						setRedisDAO.deleteFromSet(getSetKey(persistentInstance.getClass(), in.columnList(), str), "" + persistentInstance.getId());
						// setRedisDAO.redisSaveToSet(getSetKey(db.getClass(), in.columnList(), str), ""+val.getId(), EXPIRE_TIME);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("redis can delete:" + persistentInstance.getId(), e);
		}
	}
	
	public void deleteMongo(DbEntity persistentInstance) {
		// TODO Auto-generated method stub
		try {
			if (persistentInstance == null) {
				return;
			}
			stringRedisDAO.deleteFromString(persistentInstance, "" + persistentInstance.getId());
			// 存入索引
			Field[] fie = persistentInstance.getClass().getDeclaredFields();
			
			if (fie != null && fie.length > 0) {
				for (Field fi : fie) {
					
					Indexed meta = fi.getAnnotation(Indexed.class);
					if (meta != null) {
						fi.setAccessible(true);
						Object obj = fi.get(persistentInstance);
						String str = obj.toString();
						// Pattern pattern = Pattern.compile("[0-9]*");
						// boolean isMatch = pattern.matcher(str).matches();
						setRedisDAO.deleteFromSet(getSetKey(persistentInstance.getClass(), fi.getName(), str), "" + persistentInstance.getId());
						// setRedisDAO.redisSaveToSet(getSetKey(db.getClass(), in.columnList(), str), ""+val.getId(), EXPIRE_TIME);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("redis can delete:" + persistentInstance.getId(), e);
		}
	}
	
	@Override
	public void updateFinal(DbEntity instance) {
		// TODO Auto-generated method stub
		this.save(instance);
	}
	
	@Override
	public <T> T findById(Class<T> entity, long id) {
		String val = stringRedisDAO.redisGetFromString(entity, "" + id);
		T t = null;
		if (val != null) {
			t = JSON.parseObject(val, entity);
		}
		return t;
	}
	
	@Override
	public <T> List<T> findAll(Class<T> c) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public <T> List<T> findByProperty(Class<T> c, String propertyName, Object value) {
		Set<String> setV = setRedisDAO.redisGetFromSet(getSetKey(c, propertyName, value));// 得到行的id号
		if (setV != null && setV.size() > 0) {
			try {
				List<T> list = new ArrayList<T>();
				for (String str : setV) {
					T t = findById(c, Long.parseLong(str));
					if (t != null) {
						list.add(t);
					} else {
						logger.debug("find not from redis::" + str);
					}
				}
				return list;
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("findByPropertyFromRedis::" + c.getName() + ":propertyName:" + propertyName);
			}
		}
		return null;
	}
	
	@Override
	public <T> List<T> findByProperties(Class<T> c, List<String> propertyName, List<Object> value) {
		// TODO Auto-generated method stub
		Set<String> setV = setRedisDAO.redisGetFromSet(getSetKey(c, propertyName, value));// 得到行的id号
		if (setV != null && setV.size() > 0) {
			try {
				List<T> list = new ArrayList<T>();
				for (String str : setV) {
					T t = findById(c, Long.parseLong(str));
					if (t != null) {
						list.add(t);
					} else {
						logger.debug("find not from redis::" + str);
					}
				}
				return list;
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("findByPropertyFromRedis::" + c.getName() + ":propertyName:" + propertyName);
			}
		}
		return null;
	}
	
	@Override
	public <T> List<T> findByHql(String hql) {
		// TODO Auto-generated method stub
		return null;
	}
	
	//
	// /**
	// * beforeUpdate:(). <br/>
	// * TODO().<br/>
	// * 更新之前
	// *
	// * @author lyh
	// * @param db
	// * @throws IllegalAccessException
	// * @throws IllegalArgumentException
	// * @throws SecurityException
	// * @throws NoSuchFieldException
	// */
	// protected boolean beforeUpdate(DbEntity db) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
	// if (USE_REDIS) {
	// if (isInRedis(db.getClass(), db.getId())) {
	// redisSave(db);
	// // // 发送到世界服保存
	// // updateToServer(db.getClass().getSimpleName(), db.getId());
	// }
	// return true;
	// }
	// return false;
	// }
	
	//
	// /**
	// * afterFindByProperty:(). <br/>
	// * TODO().<br/>
	// * 查找属性之后
	// *
	// * @author lyh
	// * @param c
	// * @param propertyName
	// * @param value
	// * @throws IllegalAccessException
	// * @throws IllegalArgumentException
	// * @throws SecurityException
	// * @throws NoSuchFieldException
	// */
	// protected <T> void afterFindByProperty(List<T> tList, Class<T> c, String propertyName, Object value) throws NoSuchFieldException, SecurityException, IllegalArgumentException,
	// IllegalAccessException {
	// if (tList != null && USE_REDIS) {// 存入redis,存入set
	// for (T t : tList) {
	// redisSave((DbEntity) t);
	// setRedisDAO.redisSaveToSet(RedisDAO.getSetKey(c, propertyName, value), "" + ((DbEntity) t).getId(), RedisDAO.EXPIRE_TIME);
	// }
	// }
	// }
	
	// /**
	// * beforeFindByProperty:(). <br/>
	// * TODO().<br/>
	// * 查找属性之前
	// *
	// * @author lyh
	// * @param c
	// * @param propertyName
	// * @param value
	// * @return
	// */
	// protected <T> List<T> beforeFindByProperties(Class<T> c, String propertyName[], Object value[]) {
	// List<T> tList = null;
	// // 先找redis
	// if (USE_REDIS) {
	// tList = findByPropertiesFromRedis(c, propertyName, value);
	// }
	//
	// return tList;
	//
	// }
	//
	// /**
	// * afterFindByProperty:(). <br/>
	// * TODO().<br/>
	// * 查找属性之后
	// *
	// * @author lyh
	// * @param c
	// * @param propertyName
	// * @param value
	// * @throws IllegalAccessException
	// * @throws IllegalArgumentException
	// * @throws SecurityException
	// * @throws NoSuchFieldException
	// */
	// protected <T> void afterFindByProperties(List<T> tList, Class<T> c, String propertyName[], Object value[]) throws NoSuchFieldException, SecurityException, IllegalArgumentException,
	// IllegalAccessException {
	// if (tList != null && USE_REDIS) {// 存入redis,存入set
	// for (T t : tList) {
	// redisSave((DbEntity) t);
	// setRedisDAO.redisSaveToSet(RedisDAO.getSetKey(c, propertyName, value), "" + ((DbEntity) t).getId(), RedisDAO.EXPIRE_TIME);
	// }
	// }
	// }
	//
}
