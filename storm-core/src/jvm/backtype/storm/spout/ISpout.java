/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.spout;

import backtype.storm.task.TopologyContext;
import java.util.Map;
import java.io.Serializable;

/**
 * ISpout是定义和实现spout的核心接口。Spout负责像topology中生产需要处理的数据，也就是topology的数据源头。
 * 对于每一个由Spout发送出来的消息，Storm会跟踪由Spout产生的元组的DAG信息（可能会很大）。当Storm检测到DAG
 * 中的所有的元组都被成功处理之后，Storm会发送确认消息给Spout。
 *
 * 如果元组在配置的超时时间内处理失败（see {@link backtype.storm.Config}），Storm会发送当前元组的失败
 * 消息给spout。
 *
 * 当Spout发送元组的时候，Storm会用消息ID来标识这个元组，消息ID可以是任何类型。当Storm确认或者确认失败一个
 * 消息的时候，Storm会将消息ID发送回Spout来确认是哪个元组在处理过程中失败了。如果Spout没有设置消息ID，或者
 * 设置消息ID为null，则Storm不会跟踪spout发出的元组，也就不会收到任何关于消息处理成功或者失败的确认。
 *
 * Storm执行ack、fail和nextTuple都是在同一个线程中执行的，这意味着ISpout的实现不需要考虑这些方法的并发问题。
 * 然而也以为着实现必须保证nextTuple是不被阻塞的，否则ack和fail方法会被阻塞。
 *
 * ISpout is the core interface for implementing spouts. A Spout is responsible
 * for feeding messages into the topology for processing. For every tuple emitted by
 * a spout, Storm will track the (potentially very large) DAG of tuples generated
 * based on a tuple emitted by the spout. When Storm detects that every tuple in
 * that DAG has been successfully processed, it will send an ack message to the Spout.
 *
 * <p>If a tuple fails to be fully processed within the configured timeout for the
 * topology (see {@link backtype.storm.Config}), Storm will send a fail message to the spout
 * for the message.</p>
 *
 * <p> When a Spout emits a tuple, it can tag the tuple with a message id. The message id
 * can be any type. When Storm acks or fails a message, it will pass back to the
 * spout the same message id to identify which tuple it's referring to. If the spout leaves out
 * the message id, or sets it to null, then Storm will not track the message and the spout
 * will not receive any ack or fail callbacks for the message.</p>
 *
 * <p>Storm executes ack, fail, and nextTuple all on the same thread. This means that an implementor
 * of an ISpout does not need to worry about concurrency issues between those methods. However, it 
 * also means that an implementor must ensure that nextTuple is non-blocking: otherwise 
 * the method could block acks and fails that are pending to be processed.</p>
 */
public interface ISpout extends Serializable {

    /**
     * 当处理该组件的任务在集群中对该组件进行初始化的时候会调用该方法。
     * 他向该组件提供了当前组件执行的环境信息。
     */
    /**
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the spout with the environment in which the spout executes.
     *
     * <p>This includes the:</p>
     *
     * @param conf      当前组件的Storm配置信息，包括拓扑的配置信息和当前机器在集群中的配置信息。
     * @param context   可以用来获取当前任务在拓扑中的位置，包括任务ID、组件ID、输入和输出信息等。
     * @param collector 用于元组的输出，可以在当前组件的任何生命周期内调用，collector是线程安全的，需要保存在实例变量中。
     *
     * @param conf The Storm configuration for this spout. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this spout. Tuples can be emitted at any time, including the open and close methods. The collector is thread-safe and should be saved as an instance variable of this spout object.
     */
    void open(Map conf, TopologyContext context, SpoutOutputCollector collector);

    /**
     * 当ISpout将被关闭的时候调用，这个方法不一定会被调用，因为supervisor是使用kill -9来杀死任务进程的。
     *
     *  只有在一中情况下能够保证这个方法会被调用，就是在本地模式下执行Storm的时候。
     * Called when an ISpout is going to be shutdown. There is no guarentee that close
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     *
     * <p>The one context where close is guaranteed to be called is a topology is
     * killed when running Storm in local mode.</p>
     */
    void close();
    
    /**
     * 当当前Spout被从非活动状态设置为活动状态的时候被调用。接下来nextTuple才会被调用。
     * 当拓扑被使用storm客户端操作的时候，spout会从非活动状态变为活动状态。
     *
     * Called when a spout has been activated out of a deactivated mode.
     * nextTuple will be called on this spout soon. A spout can become activated
     * after having been deactivated when the topology is manipulated using the 
     * `storm` client. 
     */
    void activate();
    
    /**
     * 当spout被设置为非活动状态的时候会被调用。nextTuple在非活动状态下不会被调用，spout可能会被设置为非活动状态。
     *
     * Called when a spout has been deactivated. nextTuple will not be called while
     * a spout is deactivated. The spout may or may not be reactivated in the future.
     */
    void deactivate();

    /**
     *
     * 当这个方法被调用的时候，也就是Storm请求Spout发送数据的时候。这个方法必须是非阻塞的，因此如果nextTuple没有数据
     * 输出了，这个方法需要立即返回。nextTuple、ack、和fail是在task中杯一个单线程循环调用的。当没有数据输出的时候，
     * 最好的处理方式是让nextTuple休眠一小段时间，从而不会浪费太多CPU资源。
     *
     * When this method is called, Storm is requesting that the Spout emit tuples to the 
     * output collector. This method should be non-blocking, so if the Spout has no tuples
     * to emit, this method should return. nextTuple, ack, and fail are all called in a tight
     * loop in a single thread in the spout task. When there are no tuples to emit, it is courteous
     * to have nextTuple sleep for a short amount of time (like a single millisecond)
     * so as not to waste too much CPU.
     */
    void nextTuple();

    /**
     *
     * Storm是用msgId来标识每一个被从spout输出的元组的，典型的实现是将消息ID从队列移除，防止消息被重复处理。
     *
     * Storm has determined that the tuple emitted by this spout with the msgId identifier
     * has been fully processed. Typically, an implementation of this method will take that
     * message off the queue and prevent it from being replayed.
     */
    void ack(Object msgId);

    /**
     * 由当前tuple输出的由msgId标识的元组。典型的实现是将该消息ID放回队列，以便之后重新处理。
     *
     * The tuple emitted by this spout with the msgId identifier has failed to be
     * fully processed. Typically, an implementation of this method will put that
     * message back on the queue to be replayed at a later time.
     */
    void fail(Object msgId);
}