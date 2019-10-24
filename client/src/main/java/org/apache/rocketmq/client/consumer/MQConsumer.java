/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer;

import java.util.Set;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Message queue consumer interface
 */
public interface MQConsumer extends MQAdmin {

    /**
     * If consuming failure,message will be send back to the brokers,and delay consuming some time
     */
    @Deprecated
    void sendMessageBack(final MessageExt msg, final int delayLevel)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * If consuming failure,message will be send back to the broker,and delay consuming some time
     * 消息发送失败，重新发送到 Broker 服务器
     */
    void sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * Fetch message queues from consumer cache according to the topic
     * 获取消费者对主题 Topic 分配了那些消息队列。
     *
     * @param topic message topic
     * @return queue set
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
