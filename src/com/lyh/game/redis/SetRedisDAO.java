package com.lyh.game.redis;  

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.stereotype.Component;

import redis.clients.jedis.JedisCluster;

/** 
 * ClassName:SetRedisDAO <br/> 
 * Function: TODO (set容器,操作类). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2015-12-2 下午3:48:42 <br/> 
 * @author   lyh 
 * @version   
 * @see       
 */
@Component
public class SetRedisDAO {
	
	private static final Logger logger = LoggerFactory.getLogger(SetRedisDAO.class);
	
	/** redis **/
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
//	@Autowired
//	private JedisCluster jedisCluster;
	/**
	 * save:(). <br/>
	 * TODO().<br/>
	 * 把对象存入set
	 * 
	 * @author lyh
	 * @param key
	 * @param val
	 */
	public void redisSaveToSet(final String key, final String val,int timeout) {
		if (key == null || val == null) {
			logger.debug("redis can not save" + key);
			return;
		}
		
		SetOperations<String, String>  setVp = redisTemplate.opsForSet();
		logger.debug(key+"::set保存之前::"+setVp.members(key));
		setVp.add(key , val);
		logger.debug(key+"::set保存之后::"+setVp.members(key));
		redisTemplate.expire(key, timeout, TimeUnit.DAYS);
		
		logger.debug("redis can save" + key + ":val:" + val);
		
//		jedisCluster.sadd(key, val);
//		jedisCluster.expire(key, timeout);
	}
	
	
	/** 
	 * redisGetFromSet:(). <br/> 
	 * TODO().<br/> 
	 * 从set里得到不重复的对象(一般存的是行号)
	 * @author lyh 
	 * @param key
	 * @return
	 */  
	public Set<String> redisGetFromSet(final String key){
		SetOperations<String, String>  setVp = redisTemplate.opsForSet();	
		Set<String> set = setVp.members(key);
		
//		Set<String> set = jedisCluster.smembers(key);
		logger.debug(key+"::set查询之后::"+set.toString());
		return set;
	}
	
	/** 
	 * isInSet:(). <br/> 
	 * TODO().<br/> 
	 * 是否在set容器里
	 * @author lyh 
	 * @param key
	 * @param val
	 * @return 
	 */  
	public boolean isInSet(final String key,final String val){
		SetOperations<String, String>  setVp = redisTemplate.opsForSet();
		return setVp.isMember(key, val);
		
		//return jedisCluster.sismember(key, val);
	}
	
	/** 
	 * deleteFromSet:(). <br/> 
	 * TODO().<br/> 
	 * 从set里面删除一个对象
	 * @author lyh  
	 */  
	public void deleteFromSet(final String key,final String val){
		SetOperations<String, String>  setVp = redisTemplate.opsForSet();
		//logger.debug(key+"::set删除之前::"+setVp.members(key));
		setVp.remove(key, val);
		if (setVp.size(key) <= 0){
			this.deleteFromSet(key);
		}
		//logger.debug(key+"::set删除之后::"+setVp.members(key)+":::"+lon);
		
//		long lon =jedisCluster.srem(key, val);
//		if (jedisCluster.scard(key) <= 0){//key里面没有值了,删除对象
//			this.deleteFromSet(key);
//		}
	}
	
	
	/** 
	 * deleteFromSet:(). <br/> 
	 * TODO().<br/> 
	 * 把set里的对象全部删除
	 * @author lyh 
	 * @param key 
	 */
	public void deleteFromSet(final String key){
		redisTemplate.delete(key);
		//jedisCluster.del(key);
//		SetOperations<String, String> setVp = redisTemplate.opsForSet();
//		logger.debug(key+"::删除redisset::"+setVp.members(key));
	}
}
  