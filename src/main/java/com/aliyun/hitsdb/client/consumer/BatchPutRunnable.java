package com.aliyun.hitsdb.client.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.callback.BatchPutDetailsCallback;
import com.aliyun.hitsdb.client.callback.BatchPutSummaryCallback;
import com.aliyun.hitsdb.client.callback.http.HttpResponseCallbackFactory;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpAddressManager;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.semaphore.SemaphoreManager;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.value.request.Point;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class BatchPutRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchPutRunnable.class);

    /**
     * 缓冲队列
     */
    private final DataQueue dataQueue;

    /**
     * Http客户端
     */
    private final HttpClient hitsdbHttpClient;

    /**
     * 批量提交回调
     */
    private final AbstractBatchPutCallback<?> batchPutCallback;

    /**
     * 消费者队列控制器。
     * 在优雅关闭中，若消费者队列尚未结束，则CountDownLatch用于阻塞close()方法。
     */
    private final CountDownLatch countDownLatch;

    /**
     * 每批次数据点个数
     */
    private int batchSize;

    /**
     * 批次提交间隔，单位：毫秒
     */
    private int batchPutTimeLimit;

    /**
     * 回调包装与构造工厂
     */
    private final HttpResponseCallbackFactory httpResponseCallbackFactory;

    private final HiTSDBConfig config;

    private final SemaphoreManager semaphoreManager;

    private final HttpAddressManager httpAddressManager;

    private RateLimiter rateLimiter;

    private boolean readyClose = false;;

    public BatchPutRunnable(DataQueue dataQueue, HttpClient httpclient, HiTSDBConfig config, CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        this.dataQueue = dataQueue;
        this.hitsdbHttpClient = httpclient;
        this.semaphoreManager = hitsdbHttpClient.getSemaphoreManager();
        this.httpAddressManager = hitsdbHttpClient.getHttpAddressManager();
        this.batchPutCallback = config.getBatchPutCallback();
        this.batchSize = config.getBatchPutSize();
        this.batchPutTimeLimit = config.getBatchPutTimeLimit();
        this.config = config;
        this.countDownLatch = countDownLatch;
        this.rateLimiter = rateLimiter;
        this.httpResponseCallbackFactory = hitsdbHttpClient.getHttpResponseCallbackFactory();
    }

    @Override
    public void run() {
        // 线程变量sb，paramsMap，waitPoint，readyClose 每个线程只有一组这样的变量。
        StringBuilder sb = null;
        if (HiTSDBConfig.Builder.ProducerThreadSerializeSwitch) {
            sb = new StringBuilder(2048 * batchSize);
        }

        Map<String, String> paramsMap = new HashMap<String, String>();
        if (this.batchPutCallback != null) {
            if (batchPutCallback instanceof BatchPutCallback) {
            } else if (batchPutCallback instanceof BatchPutSummaryCallback) {
                paramsMap.put("summary", "true");
            } else if (batchPutCallback instanceof BatchPutDetailsCallback) {
                paramsMap.put("details", "true");
            }
        }

        while (true) {
            if (readyClose) {
                break;
            }

            List<Point> pointList = null;
            //根据配置情况（按内容大小、按条数）获取待发送列表
            if(config.isBatchPutByContentSizeSwitch()) {
                pointList = getSendPointListByContentSize();
            } else {
                pointList = getSendPointListByBatchSize();
            }
            if (pointList == null || pointList.size() == 0) {
                continue;
            }

            // 序列化
            String strJson = serialize(pointList, sb);

            // 发送
            sendHttpRequest(pointList, strJson, paramsMap);

            try {
                Thread.sleep(batchPutTimeLimit/3);
            } catch (InterruptedException e) {
                LOGGER.info("Thread sleep interrupted");
                readyClose = true;
            }
        }

        if (readyClose) {
            this.countDownLatch.countDown();
            return;
        }
    }


    /**
     * add by lizhiyang 2019-01-05
     * 按照内容大小获取待发送列表
     */
    private List<Point> getSendPointListByContentSize() {
        long t0 = System.currentTimeMillis();
        long t1;
        int waitTimeLimit = batchPutTimeLimit / 3;
        List<Point> pointList = new ArrayList<Point>(batchSize);
        //按照内容发送
        int maxSize = config.getBatchPutByContentSize();
        //数组前后“[]”，长度为2
        int totalLength = 2;
        do {
            try {
                Point point = dataQueue.receive(waitTimeLimit);
                if(point != null) {
                    int size = getPointSize(point);
                    if(size > 0) {
                        //最后的1表示json对象之间的“,”
                        totalLength = totalLength + size + 1;
                        if(totalLength <= maxSize) {
                            pointList.add(point);
                        } else {
                            //超出数量，再放回去,等待下一次获取
                            try {
                                dataQueue.send(point);
                            } catch (Exception e) {
                                LOGGER.error("back send point to queue exception {}, point is {}", e, point);
                            }
                            break;
                        }
                    }
                }
                t1 = System.currentTimeMillis();
            } catch (InterruptedException e) {
                readyClose = true;
                LOGGER.info("The thread {} is interrupted", Thread.currentThread().getName());
                break;
            }
        } while(t1 - t0 <= batchPutTimeLimit);
        return pointList;
    }

    /**
     * add by lizhiyang 2019-01-05
     * 按照批量发送个数获取待发送列表
     */
    private List<Point> getSendPointListByBatchSize() {
        long t0 = System.currentTimeMillis();
        long t1;
        int waitTimeLimit = batchPutTimeLimit / 3;
        List<Point> pointList = new ArrayList<Point>(batchSize);
        for (int i = pointList.size(); i < batchSize; i++) {
            try {
                Point point = dataQueue.receive(waitTimeLimit);
                if (point != null) {
                    if (this.rateLimiter != null) {
                        this.rateLimiter.acquire();
                    }
                    pointList.add(point);
                }
                t1 = System.currentTimeMillis();
                if (t1 - t0 > batchPutTimeLimit) {
                    break;
                }
            } catch (InterruptedException e) {
                readyClose = true;
                LOGGER.info("The thread {} is interrupted", Thread.currentThread().getName());
                break;
            }
        }
        return pointList;
    }

    private String getAddressAndSemaphoreAcquire() {
        String address;
        while (true) {
            address = httpAddressManager.getAddress();
            boolean acquire = this.semaphoreManager.acquire(address);
            if (!acquire) {
                continue;
            } else {
                break;
            }
        }

        return address;
    }


    private void sendHttpRequest(List<Point> pointList, String strJson, Map<String, String> paramsMap) {
        String address = getAddressAndSemaphoreAcquire();

        // 发送
        if (this.batchPutCallback != null) {
            FutureCallback<HttpResponse> postHttpCallback = this.httpResponseCallbackFactory
                    .createBatchPutDataCallback(
                            address,
                            this.batchPutCallback,
                            pointList,
                            config,
                            config.getBatchPutRetryCount()
                    );

            try {
                hitsdbHttpClient.postToAddress(address, HttpAPI.PUT, strJson, paramsMap, postHttpCallback);
            } catch (Exception ex) {
                this.semaphoreManager.release(address);
                this.batchPutCallback.failed(address, pointList, ex);
            }
        } else {
            FutureCallback<HttpResponse> noLogicBatchPutHttpFutureCallback = this.httpResponseCallbackFactory
                    .createNoLogicBatchPutHttpFutureCallback(
                            address,
                            pointList,
                            config,
                            config.getBatchPutRetryCount()
                    );
            try {
                hitsdbHttpClient.postToAddress(address, HttpAPI.PUT, strJson, noLogicBatchPutHttpFutureCallback);
            } catch (Exception ex) {
                this.semaphoreManager.release(address);
                noLogicBatchPutHttpFutureCallback.failed(ex);
            }
        }
    }

    private String serialize(List<Point> pointList, StringBuilder sb) {
        // 复用StringBuilder
        if (HiTSDBConfig.Builder.ProducerThreadSerializeSwitch) {
            sb.setLength(0);
            sb.append('[');
            for (Point point : pointList) {
                sb.append(point.toJSON());
                sb.append(",");
            }
            sb.setCharAt(sb.length() - 1, ']');
            return sb.toString();
        } else {
            return JSON.toJSONString(pointList, SerializerFeature.DisableCircularReferenceDetect);
        }

    }

    /**
     * 获取Point对象占用字节大小
     * @param point
     * @return
     * @throws UnsupportedEncodingException
     */
    private int getPointSize(Point point) {
        String content = "";
        if(HiTSDBConfig.Builder.ProducerThreadSerializeSwitch) {
            content = point.toJSON();
        } else {
            content = JSON.toJSONString(point, SerializerFeature.DisableCircularReferenceDetect);
        }
        try {
            return content.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("{} getPointSize UnsupportedEncodingException", content);
            return -1;
        }
    }

}