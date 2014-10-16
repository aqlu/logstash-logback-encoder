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
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import net.logstash.logback.layout.LogstashLayout;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by aqlu on 2014-10-13.
 * output log to redis
 * <p>
 * Configurations example:
 * 
 * <pre>
 * &lt;appender name="redisAppender" class="net.logstash.logback.appender.LogstashRedisAppender"&gt;
 *     &lt;host&gt;172.19.65.153&lt;/host&gt;          &lt;!-- redis host, required --&gt;
 *     &lt;port&gt;6379&lt;/port&gt;                   &lt;!-- redis port, default is 6379 --&gt;
 *     &lt;key&gt;logstash_intf_log&lt;/key&gt;        &lt;!-- redis key, required --&gt;
 *     &lt;enableBatch&gt;true&lt;/enableBatch&gt;     &lt;!-- true is batch write to redis; default is true --&gt;
 *     &lt;batchSize&gt;100&lt;/batchSize&gt;          &lt;!-- batch size, default 100 --&gt;
 *     &lt;period&gt;500&lt;/period&gt;                &lt;!-- each write period redis, default 500 ms --&gt;
 *     &lt;layout class="net.logstash.logback.layout.LogstashLayout"&gt;
 *         &lt;includeContext&gt;false&lt;/includeContext&gt;
 *         &lt;includeMdc&gt;false&lt;/includeMdc&gt;
 *         &lt;includeCallerInfo&gt;false&lt;/includeCallerInfo&gt;
 *         &lt;customFields&gt;{"source": "your source", "type": "your type"}&lt;/customFields&gt;
 *     &lt;/layout&gt;
 *     &lt;appender-ref ref="fileAppender" /&gt;       &lt;!-- output to this appender when output to redis failed; if you are not configured, then ignore this log, optional; --&gt;
 * &lt;/appender&gt;
 * 
 * &lt;appender name="fileAppender" class="ch.qos.logback.core.rolling.RollingFileAppender"&lt;
 *     &lt;file&gt;/you/log/path/log-name.ing&lt;/file&lt;
 *     &lt;rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy"&gt;
 *         &lt;fileNamePattern&gt;/you/log/path/log-name.%d{yyyy-MM-dd_HHmm}.log.%i&lt;/fileNamePattern&gt;
 *         &lt;MaxHistory&gt;30&lt;/MaxHistory&gt;
 *     &lt;/rollingPolicy&gt;
 *     &lt;encoder class="net.logstash.logback.encoder.LogstashEncoder"&gt;
 *         &lt;includeContext&gt;false&lt;/includeContext&gt;
 *         &lt;includeMdc&gt;false&lt;/includeMdc&gt;
 *         &lt;includeCallerInfo&gt;false&lt;/includeCallerInfo&gt;
 *         &lt;customFields&gt;{"source": "your source", "type": "your type"}&lt;/customFields&gt;
 *     &lt;/encoder&gt;
 * &lt;/appender&gt;
 * </pre>
 * 
 * </p>
 */
public class LogstashRedisAppender extends UnsynchronizedAppenderBase<ILoggingEvent> implements
        AppenderAttachable<ILoggingEvent>, Runnable {

    private AppenderAttachableImpl<ILoggingEvent> aai = new AppenderAttachableImpl<ILoggingEvent>();

    private Queue<ILoggingEvent> events = new ConcurrentLinkedQueue<ILoggingEvent>();

    private Layout<ILoggingEvent> layout = new LogstashLayout();

    private ScheduledExecutorService executor;

    private int messageIndex = 0;

    private int appenderCount = 0;

    private JedisPool pool;

    /* configurable params */

    private String host;

    private int port = Protocol.DEFAULT_PORT;

    private int database = Protocol.DEFAULT_DATABASE;

    private String key;

    private int timeout = Protocol.DEFAULT_TIMEOUT;

    private String password;

    private int batchSize = 100;

    private long period = 500; // ms

    private boolean enableBatch = true;

    private boolean daemonThread = true;

    public void run() {
        String[] batchLogs = new String[batchSize];
        ILoggingEvent[] batchEvents = new ILoggingEvent[batchSize];

        try {
            ILoggingEvent event;
            while ((event = events.poll()) != null) {
                batchEvents[messageIndex] = event;
                batchLogs[messageIndex] = layout.doLayout(event);
                messageIndex++;

                if (messageIndex >= batchSize) {
                    push(batchLogs);
                }
            }

            if (messageIndex > 0) {
                push(Arrays.copyOf(batchLogs, messageIndex));
            }

        } catch (Exception e) {
            addInfo("record log to redis failed.", e);
            if (appenderCount > 0) {
                this.appendLoopOnAppenders(Arrays.copyOf(batchEvents, messageIndex));
            }
        } finally {
            messageIndex = 0;
        }
    }

    @Override
    protected void append(ILoggingEvent event) {
        try {
            if (enableBatch) {
                events.add(event);
            } else {
                push(layout.doLayout(event));
            }
        } catch (Exception e) {
            addInfo("record log to redis failed.", e);

            if (appenderCount > 0) {
                this.appendLoopOnAppenders(event);
            }

        }
    }

    /**
     * push log to redis
     * @param jsonLogs logs for json format
     */
    private void push(String... jsonLogs) throws Exception {
        Jedis client = null;
        try {
            client = pool.getResource();
            client.rpush(key, jsonLogs);
        } catch (Exception e) {
            if (client != null) {
                pool.returnBrokenResource(client);
                client = null;
            }

            throw e;
        } finally {
            if (client != null) {
                pool.returnResource(client);
            }
        }
    }

    private void appendLoopOnAppenders(ILoggingEvent... events) {
        for (ILoggingEvent event : events) {
            this.aai.appendLoopOnAppenders(event);
        }
    }

    @Override
    public void start() {
        super.start();
        pool = new JedisPool(new JedisPoolConfig(), host, port, timeout, password, database);

        executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("RedisAppender", daemonThread));
        executor.scheduleWithFixedDelay(this, period, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        super.stop();
        executor.shutdown();
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

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public boolean isEnableBatch() {
        return enableBatch;
    }

    public void setEnableBatch(boolean enableBatch) {
        this.enableBatch = enableBatch;
    }

    public boolean isDaemonThread() {
        return daemonThread;
    }

    public void setDaemonThread(boolean daemonThread) {
        this.daemonThread = daemonThread;
    }

    @Override
    public void addAppender(Appender<ILoggingEvent> newAppender) {
        appenderCount++;
        addInfo("Attaching appender named [" + newAppender.getName() + "] to LogstashRedisAppender.");
        aai.addAppender(newAppender);
    }

    @Override
    public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
        return aai.iteratorForAppenders();
    }

    @Override
    public Appender<ILoggingEvent> getAppender(String name) {
        return aai.getAppender(name);
    }

    @Override
    public boolean isAttached(Appender<ILoggingEvent> appender) {
        return aai.isAttached(appender);
    }

    @Override
    public void detachAndStopAllAppenders() {
        aai.detachAndStopAllAppenders();
    }

    @Override
    public boolean detachAppender(Appender<ILoggingEvent> appender) {
        return aai.detachAppender(appender);
    }

    @Override
    public boolean detachAppender(String name) {
        return aai.detachAppender(name);
    }
}
