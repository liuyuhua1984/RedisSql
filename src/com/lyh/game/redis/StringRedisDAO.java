
package com.lyh.game.redis;  

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Component;

import redis.clients.jedis.JedisCluster;

/** 
 * ClassName:SetRedisDAO <br/> 
 * Function: TODO (字符串存储,操作类). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2015-12-2 下午3:48:42 <br/> 
 * @author   lyh 
 * @version   
 * @see       
 */
@Component
public class StringRedisDAO<T> {
	
	private static final Logger logger = LoggerFactory.getLogger(StringRedisDAO.class);
	
	/** redis **/
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
//	@Autowired
//	private JedisCluster jedisCluster;
	/** 冒号 **/
	public static final String COCON = ":";
	/**
	 * save:(). <br/>
	 * TODO().<br/>
	 * 把对象转成符串存入redis
	 * 
	 * @author lyh
	 * @param key
	 * @param val
	 */
	public void redisSaveToString(T t,final String key, final String val,int timeout) {
		if (key == null || val == null) {
			logger.debug("redis can not save" + key);
			return;
		}
		
		String strKey = t.getClass().getSimpleName() +COCON+key;
		ValueOperations<String, String>  setVp = redisTemplate.opsForValue();
		logger.debug(key+"::保存之前数量::"+setVp.get(strKey));
		setVp.set(strKey, val, timeout, TimeUnit.SECONDS);
		//setVp.set(strKey, val);
		logger.debug(strKey+"::保存之后数量::"+setVp.get(strKey));
//		//设置过期时间
//		redisTemplate.expire(strKey, timeout, TimeUnit.DAYS);
		
		//jedisCluster.setex(strKey,timeout, val);

		
		logger.debug("redis can save" + strKey + ":val:" + val);
	}
	
	
	/** 
	 * redisGetFromSet:(). <br/> 
	 * TODO().<br/> 
	 * 从redis得到string对象
	 * @author lyh 
	 * @param key
	 * @return     
	 */  
	public String redisGetFromString(Class<?> c,final String key){
		ValueOperations<String, String> setVp = redisTemplate.opsForValue();
		String strKey = c.getSimpleName() +COCON+key;
		String val = setVp.get(strKey); 
		//String val = jedisCluster.get(strKey);
		//logger.debug(strKey+"::查询之后数量::"+setVp.get(strKey));
		return val;
	} 
	
 
	/** 
	 * deleteFromSet:(). <br/> 
	 * TODO().<br/> 
	 * 从set里面删除一个对象
	 * @author lyh  
	 */  
	public void deleteFromString(T t,final String key){
	//	ValueOperations<String, String> setVp = redisTemplate.opsForValue();
		String strKey = t.getClass().getSimpleName() +COCON+key;
		//logger.debug(strKey+"::删除之前数量::"+setVp.get(strKey));
		redisTemplate.delete(strKey);
		//logger.debug(strKey+"::删除之后数量::"+setVp.get(strKey));
		//long del = jedisCluster.del(strKey);
		//logger.debug("redis删除:"+del);
	}
	
}
  