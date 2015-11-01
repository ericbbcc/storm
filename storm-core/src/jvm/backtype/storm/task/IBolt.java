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
package backtype.storm.task;

import backtype.storm.tuple.Tuple;
import java.util.Map;
import java.io.Serializable;

/**
 * IBolt代表一个以元组为输入，并以处理完的元组为输入的组件，tuples代表元组的意思，是Storm计算组件之间数据传输的载体。
 * IBolt可以做很多事情，包括过滤、连接、聚合等等。IBolt不需要立即对元组进行处理。
 * An IBolt represents a component that takes tuples as input and produces tuples
 * as output. An IBolt can do everything from filtering to joining to functions
 * to aggregations. It does not have to process a tuple immediately and may
 * hold onto tuples to process later.
 *
 * IBolt的生命周期如下：
 * IBolt对象在本地客户机器被创建。然后被JAVA序列化机制序列化，存储在topology中，然后提交给集群中的master机器，
 * 也就是Nimbus这个角色。然后Nimbus调度任务来反序列化组件，调用组件的prepare方法，然后开始处理元组。
 *
 * <p>A bolt's lifecycle is as follows:</p>
 *
 * <p>IBolt object created on client machine. The IBolt is serialized into the topology
 * (using Java serialization) and submitted to the master machine of the cluster (Nimbus).
 * Nimbus then launches workers which deserialize the object, call prepare on it, and then
 * start processing tuples.</p>
 *
 * 如果想要使得IBolt参数化，我们需要通过构造方法来传入参数，将参数以实例变量的方式存储在IBolt中，这些参数之后会被序列化
 * 并且提交给集群。
 *
 * <p>If you want to parameterize an IBolt, you should set the parameters through its
 * constructor and save the parameterization state as instance variables (which will
 * then get serialized and shipped to every task executing this bolt across the cluster).</p>
 *
 * 当使用JAVA来定义Bolt的时候，我们需要实现IRichBolt接口来实现一些使用JAVA API TopologyBuilder需要的接口。
 *
 * <p>When defining bolts in Java, you should use the IRichBolt interface which adds
 * necessary methods for using the Java TopologyBuilder API.</p>
 */
public interface IBolt extends Serializable {

    /**
     * 当组件在集群中被worker初始化的时候会调用这个方法，并传入bolt执行环境的上下文。
     */
    /**
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the bolt with the environment in which the bolt executes.
     *
     * <p>This includes the:</p>
     *
     * @param stormConf 当前bolt在Storm中的配置，包括拓扑和集群的配置。
     * @param context   上下文，可用来获取当前任务在拓扑中的位置信息，包括任务ID、组件ID、输入输出信息。
     * @param collector 这个collector用来输出当前bolt的处理完的元组，元组可以在任何时间被输出、包括prepare方法、cleanup方法、collector是线程安全的，需要定义为实例变量。
     * 
     * @param stormConf The Storm configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including the prepare and cleanup methods. The collector is thread-safe and should be saved as an instance variable of this bolt object.
     */
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    /**
     * 处理输入的一个元组，元组对象包含了当前元组的元数据信息，包括当前元组来自哪个组件、哪个流、哪个任务。
     * 元组的值可以通过Tuple#getValue方法来访问，IBolt不需要立即处理元组，最好是保存元组稍后一起处理。
     * 比如：聚合、连接。
     * 元组不要使用由prepare传入的OutputCollector来处理，我们需要在元组处理成功或者失败的时候通过OutputCollector
     * 来传递确认和失败信息。否则Storm无法知道当元组从spouts出来之后什么时候被处理完，也就是从拓扑输出。
     *
     * 通常我们会在execute方法的最后对元组的处理进行确认，然而IBasicBolt会自动帮我们确认。
     */

    /**
     * Process a single tuple of input. The Tuple object contains metadata on it
     * about which component/stream/task it came from. The values of the Tuple can
     * be accessed using Tuple#getValue. The IBolt does not have to process the Tuple
     * immediately. It is perfectly fine to hang onto a tuple and process it later
     * (for instance, to do an aggregation or join).
     *
     * <p>Tuples should be emitted using the OutputCollector provided through the prepare method.
     * It is required that all input tuples are acked or failed at some point using the OutputCollector.
     * Otherwise, Storm will be unable to determine when tuples coming off the spouts
     * have been completed.</p>
     *
     * <p>For the common case of acking an input tuple at the end of the execute method,
     * see IBasicBolt which automates this.</p>
     * 
     * @param input The input tuple to be processed.
     */
    void execute(Tuple input);

    /**
     * 当IBolt被关闭的时候会调用这个方法，Storm不会保证这个方法一定会被调用。因为supervisor会通过 kill -9
     * 来结束worker的生命。
     *
     * 唯一能够保证这个方法一定会被调用的地方是当Storm运行在本地模式，Topology被杀死的时候。
     */

    /**
     * Called when an IBolt is going to be shutdown. There is no guarentee that cleanup
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     *
     * <p>The one context where cleanup is guaranteed to be called is when a topology
     * is killed when running Storm in local mode.</p>
     */
    void cleanup();
}
