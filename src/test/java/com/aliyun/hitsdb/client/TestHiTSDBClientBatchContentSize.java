package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestHiTSDBClientBatchContentSize {

    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        BatchPutCallback pcb = new BatchPutCallback(){
            final int retryTimes = 3;
            final AtomicInteger num = new AtomicInteger();
            
            @Override
            public void failed(String address,List<Point> points,Exception ex) {
                System.err.println("业务回调出错！" + points.size() + " error!");
                Iterator<Point> iter = points.iterator();
                while(iter.hasNext()) {
                    Point point = iter.next();
                    if(point.retry() > retryTimes) {
                        iter.remove();
                    } else {
                        tsdb.put(point);
                    }
                }
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<Point> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("已处理" + count + "个点");
            }
            
        };

        HiTSDBConfig config = HiTSDBConfig
                // 配置地址，第一个参数可以是域名，IP。
                .address("10.200.41.96", 4242)
                // 只读开关，默认为false。当readonly设置为true时，异步写开关会被关闭。
                .readonly(false)
                // 网络连接池大小，默认为64。
                .httpConnectionPool(64)
                // HTTP等待时间，单位为秒，默认为90秒。
                .httpConnectTimeout(90)
                // IO 线程数，默认为1。
                .ioThreadCount(3)
                // 异步写开关。默认为true。推荐异步写。
                .asyncPut(true)
                // 异步写相关，客户端缓冲队列长度，默认为10000。
                .batchPutBufferSize(20000)
                // 异步写相关，缓冲队列消费线程数，默认为1。
                .batchPutConsumerThreadCount(1)
                // 异步写相关，每次批次提交给客户端点的个数，默认为500。
                .batchPutSize(100)
                .batchPutByContentSizeSwitch(true)
                .batchPutByContentSize(1024*8)
                // 异步写相关，每次等待最大时间限制，单位为ms，默认为300。
                .batchPutTimeLimit(3000)
                // 异步写相关，写请求队列数，默认等于连接池数。可根据读写次数的比例进行配置。
                .putRequestLimit(100)
                .listenBatchPut(pcb)
                // 构造HiTSDBConfig对象
                .config();
        tsdb = HiTSDBClientFactory.connect(config);
    }
    
    @After
    public void after() {
        try {
            System.out.println("将要关闭");
            //tsdb.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPutData() {
        String json = "[{\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000001_11\",\"payType\":\"25\",\"payChannelCode\":\"001\",\"payTypeParentCode\":\"00102\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":11}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000002_3\",\"payType\":\"65\",\"payChannelCode\":\"003\",\"payTypeParentCode\":\"00301\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":3}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000003_1\",\"payType\":\"62\",\"payChannelCode\":\"003\",\"payTypeParentCode\":\"00302\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":1}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000004_1\",\"payType\":\"40\",\"payChannelCode\":\"005\",\"payTypeParentCode\":\"00501\",\"status\":\"3\"},\"timestamp\":1546581460000,\"value\":1}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000005_32\",\"payType\":\"27\",\"payChannelCode\":\"001\",\"payTypeParentCode\":\"00102\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":32}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000006_10\",\"payType\":\"67\",\"payChannelCode\":\"003\",\"payTypeParentCode\":\"00301\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":10}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000007_3\",\"payType\":\"75\",\"payChannelCode\":\"002\",\"payTypeParentCode\":\"00201\",\"status\":\"3\"},\"timestamp\":1546581460000,\"value\":3}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000008_2\",\"payType\":\"60\",\"payChannelCode\":\"003\",\"payTypeParentCode\":\"00302\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":2}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"4\",\"payChannel\":\"10\",\"uniqId\":\"20190104135740217029862630000009_27\"},\"timestamp\":1546581450000,\"value\":27}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"5\",\"payChannel\":\"10\",\"uniqId\":\"20190104135740217029862630000010_54\"},\"timestamp\":1546581450000,\"value\":54}, {\"metric\":\"onebcc_trade_log\",\"tags\":{\"errMsg\":\"UEFZRVJST1I-\",\"uniqId\":\"20190104135740217029862630000011_1\",\"errCode\":\"1\"},\"timestamp\":1546581450000,\"value\":1}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000012_1\",\"payType\":\"27\",\"payChannelCode\":\"001\",\"payTypeParentCode\":\"00102\",\"status\":\"3\"},\"timestamp\":1546581460000,\"value\":1}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000013_4\",\"payType\":\"42\",\"payChannelCode\":\"005\",\"payTypeParentCode\":\"00501\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":4}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000014_4\",\"payType\":\"40\",\"payChannelCode\":\"005\",\"payTypeParentCode\":\"00501\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":4}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"2\",\"payChannel\":\"10\",\"uniqId\":\"20190104135740217029862630000015_20\"},\"timestamp\":1546581450000,\"value\":20}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000016_2\",\"payType\":\"25\",\"payChannelCode\":\"001\",\"payTypeParentCode\":\"00102\",\"status\":\"3\"},\"timestamp\":1546581460000,\"value\":2}, {\"metric\":\"onebcc_trade_log\",\"tags\":{\"errMsg\":\"IG9yZGVyIHN1Y2Nlc3MgcGF5IGlucHJvY2Vzcw--\",\"uniqId\":\"20190104135740217029862630000017_1\",\"errCode\":\"10003\"},\"timestamp\":1546581450000,\"value\":1}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000018_2\",\"payType\":\"62\",\"payChannelCode\":\"003\",\"payTypeParentCode\":\"00302\",\"status\":\"3\"},\"timestamp\":1546581460000,\"value\":2}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"5\",\"payChannel\":\"3\",\"uniqId\":\"20190104135740217029862630000019_2\"},\"timestamp\":1546581460000,\"value\":2}, {\"metric\":\"onebcc_trade_log\",\"tags\":{\"errMsg\":\"MTAxIOavj_S4quS6jOe7tOeggeS7hemZkOS9v_eUqOS4gOasoe_8jOivt_WIt_aWsOWGjeivlQ--\",\"uniqId\":\"20190104135740217029862630000020_1\",\"errCode\":\"02\"},\"timestamp\":1546581450000,\"value\":1}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"4\",\"payChannel\":\"8\",\"uniqId\":\"20190104135740217029862630000021_1\"},\"timestamp\":1546581460000,\"value\":1}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"2\",\"payChannel\":\"3\",\"uniqId\":\"20190104135740217029862630000022_2\"},\"timestamp\":1546581450000,\"value\":2}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000023_1\",\"payType\":\"75\",\"payChannelCode\":\"002\",\"payTypeParentCode\":\"00201\",\"status\":\"4\"},\"timestamp\":1546581450000,\"value\":1}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000024_17\",\"payType\":\"70\",\"payChannelCode\":\"002\",\"payTypeParentCode\":\"00202\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":17}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"4\",\"payChannel\":\"8\",\"uniqId\":\"20190104135740217029862630000025_5\"},\"timestamp\":1546581450000,\"value\":5}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"5\",\"payChannel\":\"0\",\"uniqId\":\"20190104135740217029862630000026_33\"},\"timestamp\":1546581450000,\"value\":33}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"4\",\"payChannel\":\"0\",\"uniqId\":\"20190104135740217029862630000027_2\"},\"timestamp\":1546581460000,\"value\":2}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"2\",\"payChannel\":\"0\",\"uniqId\":\"20190104135740217029862630000028_7\"},\"timestamp\":1546581450000,\"value\":7}, {\"metric\":\"onebcc_trade_log\",\"tags\":{\"errMsg\":\"5pSv5LuY5Lit\",\"uniqId\":\"20190104135740217029862630000029_1\",\"errCode\":\"03\"},\"timestamp\":1546581450000,\"value\":1}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"2\",\"payChannel\":\"5\",\"uniqId\":\"20190104135740217029862630000030_1\"},\"timestamp\":1546581460000,\"value\":1}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000031_45\",\"payType\":\"77\",\"payChannelCode\":\"002\",\"payTypeParentCode\":\"00201\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":45}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"4\",\"payChannel\":\"3\",\"uniqId\":\"20190104135740217029862630000032_2\"},\"timestamp\":1546581450000,\"value\":2}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"5\",\"payChannel\":\"3\",\"uniqId\":\"20190104135740217029862630000033_11\"},\"timestamp\":1546581450000,\"value\":11}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000034_3\",\"payType\":\"77\",\"payChannelCode\":\"002\",\"payTypeParentCode\":\"00201\",\"status\":\"3\"},\"timestamp\":1546581460000,\"value\":3}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"5\",\"payChannel\":\"14\",\"uniqId\":\"20190104135740217029862630000035_1\"},\"timestamp\":1546581460000,\"value\":1}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"5\",\"payChannel\":\"8\",\"uniqId\":\"20190104135740217029862630000036_2\"},\"timestamp\":1546581450000,\"value\":2}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"4\",\"payChannel\":\"9\",\"uniqId\":\"20190104135740217029862630000037_1\"},\"timestamp\":1546581450000,\"value\":1}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000038_1\",\"payType\":\"70\",\"payChannelCode\":\"002\",\"payTypeParentCode\":\"00202\",\"status\":\"3\"},\"timestamp\":1546581460000,\"value\":1}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"2\",\"payChannel\":\"8\",\"uniqId\":\"20190104135740217029862630000039_3\"},\"timestamp\":1546581450000,\"value\":3}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"5\",\"payChannel\":\"9\",\"uniqId\":\"20190104135740217029862630000040_2\"},\"timestamp\":1546581450000,\"value\":2}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"2\",\"payChannel\":\"10\",\"uniqId\":\"20190104135740217029862630000041_3\"},\"timestamp\":1546581460000,\"value\":3}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"5\",\"payChannel\":\"14\",\"uniqId\":\"20190104135740217029862630000042_4\"},\"timestamp\":1546581450000,\"value\":4}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"4\",\"payChannel\":\"5\",\"uniqId\":\"20190104135740217029862630000043_3\"},\"timestamp\":1546581450000,\"value\":3}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"5\",\"payChannel\":\"5\",\"uniqId\":\"20190104135740217029862630000044_4\"},\"timestamp\":1546581450000,\"value\":4}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"2\",\"payChannel\":\"5\",\"uniqId\":\"20190104135740217029862630000045_1\"},\"timestamp\":1546581450000,\"value\":1}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"4\",\"payChannel\":\"0\",\"uniqId\":\"20190104135740217029862630000046_4\"},\"timestamp\":1546581450000,\"value\":4}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000047_38\",\"payType\":\"75\",\"payChannelCode\":\"002\",\"payTypeParentCode\":\"00201\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":38}, {\"metric\":\"onebcc_pay_trade\",\"tags\":{\"uniqId\":\"20190104135740217029862630000048_16\",\"payType\":\"72\",\"payChannelCode\":\"002\",\"payTypeParentCode\":\"00202\",\"status\":\"3\"},\"timestamp\":1546581450000,\"value\":16}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"5\",\"payChannel\":\"10\",\"uniqId\":\"20190104135740217029862630000049_3\"},\"timestamp\":1546581460000,\"value\":3}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"4\",\"payChannel\":\"10\",\"uniqId\":\"20190104135740217029862630000050_1\"},\"timestamp\":1546581460000,\"value\":1}, {\"metric\":\"onebcc_order_success\",\"tags\":{\"orderSource\":\"4\",\"payChannel\":\"4\",\"uniqId\":\"20190104135740217029862630000051_1\"},\"timestamp\":1546581450000,\"value\":1}]";
        List<Point> list = JSON.parseObject(json, new TypeReference<List<Point>>(){});
        System.out.println(list.size());
        for(Point point : list) {
            tsdb.put(point);
        }
    }

    @Test
    public void testPoint() throws IOException {
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("payType", "123");
        tags.put("money", "100");
        Point point = Point.metric("test").tag(tags).timestamp(System.currentTimeMillis()).value(20).build();
        tsdb.put(point);
        System.in.read();
    }

}
