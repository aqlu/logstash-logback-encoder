/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.logstash.logback.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * Created by aqlu on 14/12/25.
 * out put log to ElasticSearch and Redis
 * <p>
 * Configurations example:
 * <p/>
 * <pre>
 * &lt;appender name="esAppender" class="net.logstash.logback.appender.ESAndRedisAppender"&gt;
 *     &lt;nodeAddresses&gt;192.168.65.131:9300&lt;/nodeAddresses&gt;   &lt;!-- ES node addresses, required; format is ip:port, multiple split by "," --&gt;
 *     &lt;clusterName&gt;elasticsearch&lt;/clusterName&gt;             &lt;!-- ES clusterName, default is 'elasticsearch' --&gt;
 *     &lt;index&gt;&lt;/index&gt;                                      &lt;!-- ES index, default is a logstash style index,like 'logstash-YYYY.MM.DD' --&gt;
 *     &lt;type&gt;events&lt;/type&gt;                                  &lt;!-- ES index, default is 'events' --&gt;
 *     &lt;bulkSize&gt;100&lt;/bulkSize&gt;                             &lt;!-- Size of ES bulk request, default is 100 --&gt;
 *     &lt;interval&gt;1000&lt;/interval&gt;                            &lt;!-- each write period, unit is millisecond, default 1000 --&gt;
 *     &lt;redis_host&gt;192.168.1.1&lt;/redis_host&gt;                 &lt;!-- redis host ip, required --&gt;
 *     &lt;redis_port&gt;6379&lt;/redis_port&gt;                        &lt;!-- redis port, default 6379 --&gt;
 *     &lt;layout class="net.logstash.logback.layout.LogstashLayout"&gt;
 *         &lt;includeContext&gt;false&lt;/includeContext&gt;
 *         &lt;includeMdc&gt;false&lt;/includeMdc&gt;
 *         &lt;includeCallerInfo&gt;false&lt;/includeCallerInfo&gt;
 *         &lt;customFields&gt;{"source": "your source"}&lt;/customFields&gt;
 *     &lt;/layout&gt;
 * &lt;/appender&gt;
 * </pre>
 * <p/>
 * </p>
 */
public class ESAndRedisAppender extends ElasticsearchAppender {

    public static final int DEFAULT_MAX_IDLE = 1;

    public static final int DEFAULT_MAX_TOTAL = 1;

    public static final int DEFAULT_MAX_WAIT_MILLIS = 1000;

    public static final String KEY_PREFIX = "LOGSTASH_";

    private JedisPool pool;

    private String redis_host;

    private int redis_port = Protocol.DEFAULT_PORT;

    private int redis_database = Protocol.DEFAULT_DATABASE;

    private int redis_maxIdle = DEFAULT_MAX_IDLE;

    private int redis_maxTotal = DEFAULT_MAX_TOTAL;

    private int redis_maxWaitMills = DEFAULT_MAX_WAIT_MILLIS;

    private int redis_timeout = Protocol.DEFAULT_TIMEOUT;

    private String redis_password;

    ESAndRedisParamMBean paramMBean = new ESAndRedisParamMBean();

    @Override
    public void start() {
        if (isStarted())
            return;

        super.start();

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setJmxEnabled(true);
        jedisPoolConfig.setJmxNamePrefix("logstash-redis-pool");
        jedisPoolConfig.setMaxIdle(redis_maxIdle);
        jedisPoolConfig.setMaxTotal(redis_maxTotal);
        jedisPoolConfig.setMaxWaitMillis(redis_maxWaitMills);
        pool = new JedisPool(jedisPoolConfig, redis_host, redis_port, redis_timeout, redis_password, redis_database);

        String objectName = "com.qianmi:name=ESAndRedisParamMBean_" + this.getType();
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(objectName);

            if (mbs.isRegistered(name)) {
                addWarn("***** MBean " + objectName + " is exists, begin to unregist.");
                mbs.unregisterMBean(name);
            }

            StandardMBean mbean = new StandardMBean(paramMBean, ParamMBean.class, false);
            mbs.registerMBean(mbean, name);

            addInfo("***** register MBean " + objectName + " success");
        } catch (Exception e) {
            addWarn("***** register MBean " + objectName + " failed!", e);
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (pool != null) {
            pool.destroy();
        }
    }

    @Override
    protected void endProcess(List<ILoggingEvent> eventList) {
        super.endProcess(eventList);
        this.push(eventList);
    }

    /**
     * push log to redis
     *
     * @param eventList logs for json format
     */
    private void push(List<ILoggingEvent> eventList) {

        if (eventList == null || System.currentTimeMillis() > paramMBean.getOutputDeadlineTime()) {
            return;
        }

        Jedis client = null;

        try {
            client = pool.getResource();

            String[] jsonLogs = new String[eventList.size()];
            int i = 0;
            for (ILoggingEvent event : eventList) {
                String logStr = this.getLayout().doLayout(event);
                jsonLogs[i] = logStr;
                i++;
            }

            String key = KEY_PREFIX + this.getType();
            client.rpush(key, jsonLogs);

        } catch (Exception e) {
            if (client != null) {
                pool.returnBrokenResource(client);
                client = null;
            }

            addInfo("record log to redis failed." + e.getMessage());
        } finally {
            if (client != null) {
                pool.returnResource(client);
            }
        }
    }

    public String getRedis_host() {
        return redis_host;
    }

    public void setRedis_host(String redis_host) {
        this.redis_host = redis_host;
    }

    public int getRedis_port() {
        return redis_port;
    }

    public void setRedis_port(int redis_port) {
        this.redis_port = redis_port;
    }

    public int getRedis_database() {
        return redis_database;
    }

    public void setRedis_database(int redis_database) {
        this.redis_database = redis_database;
    }

    public int getRedis_maxIdle() {
        return redis_maxIdle;
    }

    public void setRedis_maxIdle(int redis_maxIdle) {
        this.redis_maxIdle = redis_maxIdle;
    }

    public int getRedis_maxTotal() {
        return redis_maxTotal;
    }

    public void setRedis_maxTotal(int redis_maxTotal) {
        this.redis_maxTotal = redis_maxTotal;
    }

    public int getRedis_maxWaitMills() {
        return redis_maxWaitMills;
    }

    public void setRedis_maxWaitMills(int redis_maxWaitMills) {
        this.redis_maxWaitMills = redis_maxWaitMills;
    }

    public int getRedis_timeout() {
        return redis_timeout;
    }

    public void setRedis_timeout(int redis_timeout) {
        this.redis_timeout = redis_timeout;
    }

    public String getRedis_password() {
        return redis_password;
    }

    public void setRedis_password(String redis_password) {
        this.redis_password = redis_password;
    }
}
