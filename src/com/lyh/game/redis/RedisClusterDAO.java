
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
 * ClassName:RedisClusterDAO <br/> 
 * Function: TODO (jedisCluster的封装). <br/> 
 * Reason:   TODO (). <br/> 
 * Date:     2017年3月31日 下午4:34:38 <br/> 
 * @author   lyh 
 * @version   
 * @see       
 */
@Component
public class RedisClusterDAO {
	private static final Logger logger = LoggerFactory.getLogger(RedisClusterDAO.class);
//	@Autowired
//	private JedisCluster jedisCluster;
	
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	/** 
	 * save:(). <br/> 
	 * TODO().<br/> 
	 * jedis保存
	 * @author lyh 
	 * @param key
	 * @param value
	 * @param seconds 过期时间(秒)
	 */  
	public void save(String key,String value,int seconds){
		//jedisCluster.setex(key, seconds, value);
		ValueOperations<String, String>  setVp = redisTemplate.opsForValue();
		setVp.set(key, value, seconds, TimeUnit.SECONDS);
	}
	
	
	/** 
	 * save:(). <br/> 
	 * TODO().<br/> 
	 * jedis保存
	 * @author lyh 
	 * @param key
	 * @param value 
	 */  
	public void save(String key,String value){
		//jedisCluster.set(key, value);
		
		ValueOperations<String, String>  setVp = redisTemplate.opsForValue();
		setVp.set(key, value);
		
	}
	
	/** 
	 * get:(). <br/> 
	 * TODO().<br/> 
	 * 获取jedis内容
	 * @author lyh 
	 * @param key
	 * @return 
	 */  
	public String  get(String key){
		//return jedisCluster.get(key);
		ValueOperations<String, String> setVp = redisTemplate.opsForValue();
		String val = setVp.get(key); 
		return val;
	}
	
	
	/** 
	 * remove:(). <br/> 
	 * TODO().<br/> 
	 * 删除key
	 * @author lyh 
	 * @param key 
	 */  
	public void remove(String key){
		redisTemplate.delete(key);
		
	}
	///以后继续
	
}
  