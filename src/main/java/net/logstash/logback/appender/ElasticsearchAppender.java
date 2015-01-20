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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by aqlu on 14/12/17.
 * out put log to ElasticSearch
 * <p>
 * Configurations example:
 * <p/>
 * <pre>
 * &lt;appender name="esAppender" class="net.logstash.logback.appender.ElasticsearchAppender"&gt;
 *     &lt;nodeAddresses&gt;192.168.65.131:9300&lt;/nodeAddresses&gt;   &lt;!-- ES node addresses, required; format is ip:port, multiple split by "," --&gt;
 *     &lt;clusterName&gt;elasticsearch&lt;/clusterName&gt;             &lt;!-- ES clusterName, default is 'elasticsearch' --&gt;
 *     &lt;index&gt;&lt;/index&gt;                                      &lt;!-- ES index, default is a logstash style index,like 'logstash-YYYY.MM.DD' --&gt;
 *     &lt;type&gt;events&lt;/type&gt;                                  &lt;!-- ES index, default is 'events' --&gt;
 *     &lt;bulkSize&gt;100&lt;/bulkSize&gt;                             &lt;!-- Size of ES bulk request, default is 100 --&gt;
 *     &lt;interval&gt;1000&lt;/interval&gt;                            &lt;!-- each write period, unit is millisecond, default 1000 --&gt;
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
public class ElasticsearchAppender extends UnsynchronizedAppenderBase<ILoggingEvent> implements Runnable {
    private TransportClient client;

    private Layout<ILoggingEvent> layout;

    private String nodeAddresses;

    private String clusterName = "elasticsearch";

    private String index;

    private String type = "events";

    private int bulkSize = 100;

    private int interval = 1000;

    BlockingQueue<ILoggingEvent> queue;
    Future<?> task;

    long lastDispatchTime = System.currentTimeMillis();

    @Override
    public void run() {
        List<ILoggingEvent> eventList = new ArrayList<ILoggingEvent>(bulkSize);

        while (this.isStarted()) {
            try {
                ILoggingEvent event = queue.poll(interval, TimeUnit.MILLISECONDS);
                if (event != null) {
                    eventList.add(event);

                    if (eventList.size() >= bulkSize || System.currentTimeMillis() > lastDispatchTime + interval) {
                        dispatchEvents(eventList);
                    }
                } else if (eventList.size() > 0) {
                    dispatchEvents(eventList);
                }
            } catch (Exception ie) {
                assert true;
            }
        }

        addInfo("Worker thread will flush remaining events before exiting. ");

        for (ILoggingEvent event : queue) {
            eventList.add(event);
        }

        if (eventList.size() > 0) {
            dispatchEvents(eventList);
        }
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (event == null || !isStarted())
            return;
        event.prepareForDeferredProcessing();
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            addError("Interrupted while appending event to SocketAppender", e);
        }

    }

    protected void endProcess(List<ILoggingEvent> eventList){}


    private void dispatchEvents(List<ILoggingEvent> eventList) {

        try {
            BulkRequestBuilder request = client.prepareBulk();

            for (ILoggingEvent event : eventList) {
                String logStr = layout.doLayout(event);
                request.add(client.prepareIndex(getIndexName(), type, UUID.randomUUID().toString()).setSource(logStr));
            }

            BulkResponse response = request.execute().actionGet();

            if (response.hasFailures()) {
                addInfo("push events to ES failed! response:" + response.buildFailureMessage());
            }

            endProcess(eventList);
        } finally {
            eventList.clear();
            this.lastDispatchTime = System.currentTimeMillis();
        }
    }

    private String getIndexName() {
        if (index != null && index.length() > 0) {
            return index;
        } else {
            return String.format("logstash-%s", new SimpleDateFormat("yyyy.MM.dd").format(new Date()));
        }
    }

    /**
     * {@inheritDoc}
     */
    public void start() {
        if (isStarted()) {
            addWarn(name + " is already start");
            return;
        }
        int errorCount = 0;

        if (layout == null) {
            errorCount++;
            addError("No layout was configured for appender " + name + ".");
        }

        if (nodeAddresses == null || nodeAddresses.length() == 0){
            errorCount ++;
            addError("ES node addresses cannot be empty.");
        }

        if (errorCount == 0) {
            try {
                Settings settings = ImmutableSettings.settingsBuilder()
                        .put("client.transport.sniff", true)
                        .put("client", true)
                        .put("data", false)
                        .put("cluster.name", clusterName)
                        .build();

                client = new TransportClient(settings);

                String[] nodeArr = nodeAddresses.split(",");
                for(String node : nodeArr) {
                    String[] hostIp = node.split(":");
                    if(hostIp.length > 1) {
                        String host = hostIp[0];
                        int port = Integer.parseInt(hostIp[1]);
                        client.addTransportAddress(new InetSocketTransportAddress(host, port));
                    }
                }

            } catch (Exception ex) {
                addError("connect to elasticsearch failed! nodeAddresses:" + nodeAddresses + ", cluster:" + clusterName);
                errorCount++;
            }
        }

        if (errorCount == 0) {
            queue = new ArrayBlockingQueue<ILoggingEvent>(bulkSize * 2);
            task = this.getContext().getExecutorService().submit(this);
            layout.start();
            super.start();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        if (!isStarted())
            return;
        if (client != null)
            client.close();
        if (task != null)
            task.cancel(true);
        super.stop();
    }

    public String getNodeAddresses() {
        return nodeAddresses;
    }

    public void setNodeAddresses(String nodeAddresses) {
        this.nodeAddresses = nodeAddresses;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public Layout<ILoggingEvent> getLayout() {
        return layout;
    }

    public void setLayout(Layout<ILoggingEvent> layout) {
        this.layout = layout;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
}
