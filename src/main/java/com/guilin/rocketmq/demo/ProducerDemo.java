package com.guilin.rocketmq.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2016/8/10.
 */
public class ProducerDemo {

    public static void main(String[] args) throws MQClientException,
            InterruptedException {
        /**
         * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例<br>
         * 注意：ProducerGroupName需要由应用来保证唯一<br>
         * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键，
         * 因为服务器会回查这个Group下的任意一个Producer
         */
        final DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.setNamesrvAddr("192.168.70.104:9876");
        producer.setInstanceName("Producer");
        System.out.println(producer.getDefaultTopicQueueNums());
        producer.setDefaultTopicQueueNums(10);
        System.out.println(producer.getDefaultTopicQueueNums());


        /**
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可<br>
         * 注意：切记不可以在每次发送消息时，都调用start方法
         */
        producer.start();

        /**
         * 下面这段代码表明一个Producer对象可以发送多个topic，多个tag的消息。
         * 注意：send方法是同步调用，只要不抛异常就标识成功。但是发送成功也可会有多种状态，<br>
         * 例如消息写入Master成功，但是Slave不成功，这种情况消息属于成功，但是对于个别应用如果对消息可靠性要求极高，<br>
         * 需要对这种情况做处理。另外，消息可能会存在发送失败的情况，失败重试由应用来处理。
         */
//        for (int i = 0; i < 100; i++) {
            try {

                String path = ProducerDemo.class.getClassLoader().getResource("ip").getPath();

                Collection<File> collection = FileUtils.listFiles(new File(path), null, false);

                int count = 0;

                for (File file : collection) {
                    String name = file.getName();
                    List<String> list = FileUtils.readLines(file);
                    for (String str : list) {
                        String[] ips = str.split("\t");
                        if(ips!=null && ips.length>0){
                            for(String ip:ips){
                                Map<String,Object> map = new HashMap<>();
                                map.put("timestamp",System.currentTimeMillis());
                                map.put("ip",ip);

                                System.out.println(JSON.toJSONString(map));

                                Message msg = new Message("rocketmqTopic",//topic
                                        "TagA",//tag
                                        name,//key
                                        JSON.toJSONBytes(map)
                                );
                                SendResult sendResult = producer.send(msg);
                                count++;
                            }
                        }
                    }
                }

                System.out.println(count);



//                {
//                    Message msg = new Message("rocketmqTopic",// topic
//                            "TagA",// tag
//                            "OrderID"+i,// key
//                            ("Hello MetaQA").getBytes());// body
//                    SendResult sendResult = producer.send(msg);
//                    System.out.println(sendResult);
//                }

//                {
//                    Message msg = new Message("TopicTest2",// topic
//                            "TagB",// tag
//                            "OrderID0034",// key
//                            ("Hello MetaQB").getBytes());// body
//                    SendResult sendResult = producer.send(msg);
//                    System.out.println(sendResult);
//                }
//
//                {
//                    Message msg = new Message("TopicTest3",// topic
//                            "TagC",// tag
//                            "OrderID061",// key
//                            ("Hello MetaQC").getBytes());// body
//                    SendResult sendResult = producer.send(msg);
//                    System.out.println(sendResult);
//                }
            } catch (Exception e) {
                e.printStackTrace();
            }
//            TimeUnit.MILLISECONDS.sleep(1000);
//        }

        /**
         * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
         * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
         */
//    producer.shutdown();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                producer.shutdown();
            }
        }));
        System.exit(0);
    }
}
