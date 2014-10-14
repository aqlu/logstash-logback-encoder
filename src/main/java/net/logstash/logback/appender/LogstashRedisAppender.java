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
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import net.logstash.logback.layout.LogstashLayout;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * Created by aqlu on 2014-10-13.
 * output log to redis
 * <p>
 * Configurations example:
 * <pre>
 * &lt;appender name="redisAppender" class="net.logstash.logback.appender.LogstashRedisAppender"&gt;
 *     &lt;host&gt;172.19.65.153&lt;/host&gt;
 *     &lt;port&gt;6379&lt;/port&gt;
 *     &lt;key&gt;logstash_intf_log&lt;/key&gt;
 *     &lt;layout class="net.logstash.logback.layout.LogstashLayout"&gt;
 *         &lt;includeContext&gt;false&lt;/includeContext&gt;
 *         &lt;includeMdc&gt;false&lt;/includeMdc&gt;
 *         &lt;includeCallerInfo&gt;false&lt;/includeCallerInfo&gt;
 *         &lt;customFields&gt;{"source": "your source", "type": "your type"}&lt;/customFields&gt;
 *     &lt;/layout&gt;
 * &lt;/appender&gt;
 * </pre>
 * </p>
 */
public class LogstashRedisAppender extends UnsynchronizedAppenderBase<ILoggingEvent>  {
    private JedisPool pool;

    private String host;

    private int port = Protocol.DEFAULT_PORT;

    private int database = Protocol.DEFAULT_DATABASE;

    private String key;

    private int timeout = Protocol.DEFAULT_TIMEOUT;

    private String password;

    private Layout<ILoggingEvent> layout = new LogstashLayout();

    @Override
    protected void append(ILoggingEvent eventObject) {
        Jedis client = null;
        try {
            client = pool.getResource();
            String json = layout.doLayout(eventObject);
            client.rpush(key, json);
        } catch (Exception e) {
            addInfo("record log to redis failed.");

            if (client != null) {
                pool.returnBrokenResource(client);
                client = null;
            }
        } finally {
            if (client != null) {
                pool.returnResource(client);
            }
        }
    }

    @Override
    public void start() {
        super.start();
        pool = new JedisPool(new JedisPoolConfig(), host, port, timeout, password, database);
    }

    @Override
    public void stop() {
        super.stop();
        pool.destroy();
    }

    public JedisPool getPool() {
        return pool;
    }

    public void setPool(JedisPool pool) {
        this.pool = pool;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Layout<ILoggingEvent> getLayout() {
        return layout;
    }

    public void setLayout(Layout<ILoggingEvent> layout) {
        this.layout = layout;
    }
}
