package com.lyh.game.redis;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

/**
 * ClassName:JedisClusterFactory <br/>
 * Function: TODO (). <br/>
 * Reason: TODO (). <br/>
 * Date: 2017年3月4日 下午5:46:06 <br/>
 * 到此配置完成
 * 
 * 使用时，直接注入即可， 如下所示：
 * 
 * 
 * 
 * @Autowired
 *            
 *            JedisCluster jedisCluster;
 * 
 * 
 *            6.用法示例：
 * 
 *            jedisCluster.set("aaa", "aaaaaa_value"); jedisCluster.expire("aaa", 100);
 * 
 *            System.out.println(jedisCluster.get("aaa"));
 * @author lyh
 * @version
 * @see
 */
public class JedisClusterFactory implements FactoryBean<JedisCluster>, InitializingBean {
	private final static Logger log = LoggerFactory.getLogger(JedisClusterFactory.class);
	
	private Resource addressConfig;
	private String addressKeyPrefix;
	private JedisCluster jedisCluster;
	private Integer timeout;
	private Integer maxRedirections;
	private GenericObjectPoolConfig genericObjectPoolConfig;
	/** 密码 **/
	private String password;
	private Pattern p = Pattern.compile("^.+[:]\\d{1,5}\\s*$");
	
	@Override
	public JedisCluster getObject() throws Exception {
		return jedisCluster;
	}
	
	@Override
	public Class<? extends JedisCluster> getObjectType() {
		return (this.jedisCluster != null ? this.jedisCluster.getClass() : JedisCluster.class);
	}
	
	@Override
	public boolean isSingleton() {
		return true;
	}
	
	private Set<HostAndPort> parseHostAndPort() throws Exception {
		try {
			Properties prop = new Properties();
			prop.load(this.addressConfig.getInputStream());
			
			Set<HostAndPort> haps = new HashSet<HostAndPort>();
			for (Object key : prop.keySet()) {
				
				if (!((String) key).startsWith(addressKeyPrefix)) {
					continue;
				}
				
				String val = (String) prop.get(key);
				
				boolean isIpPort = p.matcher(val).matches();
				
				if (!isIpPort) {
					throw new IllegalArgumentException("ip 或 port 不合法");
				}
				String[] ipAndPort = val.split(":");
				
				HostAndPort hap = new HostAndPort(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
				haps.add(hap);
			}
			
			return haps;
		} catch (IllegalArgumentException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new Exception("解析 jedis 配置文件失败", ex);
		}
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		
		Set<HostAndPort> haps = this.parseHostAndPort();
		
		// jedisCluster = new JedisCluster(haps, timeout, maxRedirections, genericObjectPoolConfig);
		jedisCluster = new JedisCluster(haps, timeout, timeout, maxRedirections, password, genericObjectPoolConfig);
		// JedisCluster(hostAndPort, timeout, timeout, redirects, password, poolConfig)
	}
	
	public void setAddressConfig(Resource addressConfig) {
		this.addressConfig = addressConfig;
	}
	
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	
	public void setMaxRedirections(int maxRedirections) {
		this.maxRedirections = maxRedirections;
		
	}
	
	public void setAddressKeyPrefix(String addressKeyPrefix) {
		this.addressKeyPrefix = addressKeyPrefix;
	}
	
	public void setGenericObjectPoolConfig(GenericObjectPoolConfig genericObjectPoolConfig) {
		this.genericObjectPoolConfig = genericObjectPoolConfig;
	}
	
	public void setPassword(String password) {
		this.password = password;
		log.error("进来了**********************");
	}
	
	public void destroy() {
		
		if (jedisCluster != null) {
			try {
				jedisCluster.close();
			} catch (Exception ex) {
				log.warn("Cannot properly close Jedis cluster", ex);
			}
			
		}
	}
}