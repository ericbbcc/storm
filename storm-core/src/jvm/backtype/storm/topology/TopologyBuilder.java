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
package backtype.storm.topology;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.grouping.PartialKeyGrouping;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONValue;

/**
 * TopologyBuilder暴露JAVA API接口，用来构造用于Strom执行的topology。Topologies最终构造的是一个Thrift结构，
 * 但是、鉴于Thrift API是非常冗长繁琐的，TopologyBuilder使得出创建拓扑变得很容易。创建和提交topology的模板如下面所示：
 *
 *
 * TopologyBuilder exposes the Java API for specifying a topology for Storm
 * to execute. Topologies are Thrift structures in the end, but since the Thrift API
 * is so verbose, TopologyBuilder greatly eases the process of creating topologies.
 * The template for creating and submitting a topology looks something like:
 *
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5);
 * builder.setSpout("2", new TestWordSpout(true), 3);
 * builder.setBolt("3", new TestWordCounter(), 3)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * 
 * StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
 * </pre>
 *
 * 下面是本地模式运行同样的拓扑的代码。
 *
 * Running the exact same topology in local mode (in process), and configuring it to log all tuples
 * emitted, looks like the following. Note that it lets the topology run for 10 seconds
 * before shutting down the local cluster.
 *
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5);
 * builder.setSpout("2", new TestWordSpout(true), 3);
 * builder.setBolt("3", new TestWordCounter(), 3)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * conf.put(Config.TOPOLOGY_DEBUG, true);
 *
 * LocalCluster cluster = new LocalCluster();
 * cluster.submitTopology("mytopology", conf, builder.createTopology());
 * Utils.sleep(10000);
 * cluster.shutdown();
 * </pre>
 *
 * <p>The pattern for TopologyBuilder is to map component ids to components using the setSpout
 * and setBolt methods. Those methods return objects that are then used to declare
 * the inputs for that component.</p>
 */
public class TopologyBuilder {
    /**
     * 存储Bolt标识->Bolt的映射
     */
    private Map<String, IRichBolt> _bolts = new HashMap<String, IRichBolt>();
    /**
     * 存储Spout标识->Spot的映射
     */
    private Map<String, IRichSpout> _spouts = new HashMap<String, IRichSpout>();
    /**
     * 存储组件ID->ComponentCommon的映射，ComponentCommon包含的信息包括组件ID、组件配置信息和输入信息。
     * 包括bolt和spout的信息。
     */
    private Map<String, ComponentCommon> _commons = new HashMap<String, ComponentCommon>();

//    private Map<String, Map<GlobalStreamId, Grouping>> _inputs = new HashMap<String, Map<GlobalStreamId, Grouping>>();

    private Map<String, StateSpoutSpec> _stateSpouts = new HashMap<String, StateSpoutSpec>();
    
    
    public StormTopology createTopology() {
        Map<String, Bolt> boltSpecs = new HashMap<String, Bolt>();
        Map<String, SpoutSpec> spoutSpecs = new HashMap<String, SpoutSpec>();
        for(String boltId: _bolts.keySet()) {
            IRichBolt bolt = _bolts.get(boltId);
            ComponentCommon common = getComponentCommon(boltId, bolt);
            /**
             * 将组件序列化，生成组件ID、序列化的组件、和组件信息（包括上游组件、组件配置信息、分组策略等信息）。
             */
            boltSpecs.put(boltId, new Bolt(ComponentObject.serialized_java(Utils.javaSerialize(bolt)), common));
        }
        for(String spoutId: _spouts.keySet()) {
            IRichSpout spout = _spouts.get(spoutId);
            ComponentCommon common = getComponentCommon(spoutId, spout);
            /**
             * 同上、序列化...
             */
            spoutSpecs.put(spoutId, new SpoutSpec(ComponentObject.serialized_java(Utils.javaSerialize(spout)), common));
            
        }
        /**
         * 由将序列化的组件生成拓扑实例。
         */
        return new StormTopology(spoutSpecs,
                                 boltSpecs,
                                 new HashMap<String, StateSpoutSpec>());
    }

    /**
     *
     * 在拓扑中定义一个新的blot组件、这个借口定义为单线程。
     *
     * @param id 当前组件的ID，这个ID被其他消费当前组件输出的组件用来引用当前组件。
     *
     * Define a new bolt in this topology with parallelism of just one thread.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology with the specified amount of parallelism.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) throws IllegalArgumentException {
        validateUnusedId(id);
        initCommon(id, bolt, parallelism_hint);
        _bolts.put(id, bolt);
        return new BoltGetter(id);
    }

    /**
     * 在拓扑中定义一个blot，这里定义的是basic bolt，和IRichBolt不同的是basic bolt更容易使用但是有更多的限制。
     * 通常约定越多、限制越多对吧？这里basic bolt为我们做了ack和fail的工作。basic bolt目的是用于非聚合进程，并且会
     * 自动锚定和确认进程来达到拓扑中的相对可靠。
     *
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around the cluster.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) throws IllegalArgumentException {
        return setBolt(id, new BasicBoltExecutor(bolt), parallelism_hint);
    }

    /**
     * Define a new spout in this topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout) throws IllegalArgumentException {
        return setSpout(id, spout, null);
    }

    /**
     * Define a new spout in this topology with the specified parallelism. If the spout declares
     * itself as non-distributed, the parallelism_hint will be ignored and only one task
     * will be allocated to this component.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param parallelism_hint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a process somwehere around the cluster.
     * @param spout the spout
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint) throws IllegalArgumentException {
        validateUnusedId(id);//校验Spout的标识是否已经被用过
        initCommon(id, spout, parallelism_hint);//IRichSpout是扩展IComponent的
        _spouts.put(id, spout);
        return new SpoutGetter(id);
    }

    public void setStateSpout(String id, IRichStateSpout stateSpout) throws IllegalArgumentException {
        setStateSpout(id, stateSpout, null);
    }

    public void setStateSpout(String id, IRichStateSpout stateSpout, Number parallelism_hint) throws IllegalArgumentException {
        validateUnusedId(id);
        // TODO: finish
    }


    private void validateUnusedId(String id) {
        if(_bolts.containsKey(id)) {
            throw new IllegalArgumentException("Bolt has already been declared for id " + id);
        }
        if(_spouts.containsKey(id)) {
            throw new IllegalArgumentException("Spout has already been declared for id " + id);
        }
        if(_stateSpouts.containsKey(id)) {
            throw new IllegalArgumentException("State spout has already been declared for id " + id);
        }
    }

    private ComponentCommon getComponentCommon(String id, IComponent component) {
        ComponentCommon ret = new ComponentCommon(_commons.get(id));

        /**
         * Getter模式的用法可以用来被传入、然后被设置、最后可以被取出。
         */
        OutputFieldsGetter getter = new OutputFieldsGetter();//返回流ID -> 流信息映射
        component.declareOutputFields(getter);//这里调用组件的declareOutputFields方法来拿到用户定义的输出Field
        ret.set_streams(getter.getFieldsDeclaration());//设置流映射信息
        return ret;        
    }
    
    private void initCommon(String id, IComponent component, Number parallelism) throws IllegalArgumentException {
        ComponentCommon common = new ComponentCommon();
        common.set_inputs(new HashMap<GlobalStreamId, Grouping>());
        if(parallelism!=null) {
            int dop = parallelism.intValue();
            if(dop < 1) {
                throw new IllegalArgumentException("Parallelism must be positive.");
            }
            common.set_parallelism_hint(dop);//并行粒度
        }
        Map conf = component.getComponentConfiguration();//组件配置，我们测例子中改方法返回空，即我们没有配置。
        if(conf!=null) common.set_json_conf(JSONValue.toJSONString(conf));
        _commons.put(id, common);//组件ID -> 组件信息
    }

    /**
     * 这里我们可以看出、Getter模式通常和Declarer模式一起使用，Getter使用Declarer来声明Getter可以获取的数据。
     * OutputFieldsGetter使用的是继承的方式，而ConfigGetter使用的是泛型。
     * @param <T>
     */
    protected class ConfigGetter<T extends ComponentConfigurationDeclarer> extends BaseConfigurationDeclarer<T> {
        String _id;//组件ID
        
        public ConfigGetter(String id) {
            _id = id;
        }
        
        @Override
        public T addConfigurations(Map<String, Object> conf) {
            if(conf!=null && conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
                throw new IllegalArgumentException("Cannot set serializations for a component using fluent API");
            }
            String currConf = _commons.get(_id).get_json_conf();//将组件的配置、和当前方法传入的配置合并。
            _commons.get(_id).set_json_conf(mergeIntoJson(parseJson(currConf), conf));
            return (T) this;//这里可以看出，返回的应该是继承了ConfigGetter类和实现了ComponentConfigurationDeclarer接口的类的实例。
        }
    }

    /**
     * SpoutGetter继承了ConfigGetter
     */
    protected class SpoutGetter extends ConfigGetter<SpoutDeclarer> implements SpoutDeclarer {
        public SpoutGetter(String id) {
            super(id);
        }        
    }

    /**
     * bolt组件和spout组件不同的是，bolt组件要配置bolt接收数据的分组策略。
     * 这里顺便说一下Storm的几种分组策略：
     * 0.fields     :   根据字段来分组，例如，字段“words”被发送到A任务，则“words”字段一直发送给A任务。
     * 1.shuffle    :   表示随机分组，即上游数据会被随机分配到当前组件的任务当中，如当前组件被三个task线程运行，
     * 那么上游组件会随机挑选task配将数据输出到当前组件。
     * 2.all        :   上游组件的数据会被发送到下游组件的所有任务当中去。
     * 3.none       :   tuple会被发送到指定的task中去，这个task由torm来选择，这使得Storm可以S通过将任务绑定到单一的进程来达到优化的目的。
     * 4.direct     :   表示当前组件希望上游组件直接将tuples发送到当前组件。
     * 5.local_or_shuffle   :   发送到同一个worker进程的task当中，否则随机。
     * 6.global     :如果field字段为空，则为global分组。
     * 7.负载均衡分组策略
     * 8.用户自定义分组策略
     */
    protected class BoltGetter extends ConfigGetter<BoltDeclarer> implements BoltDeclarer {
        private String _boltId;//组件ID

        public BoltGetter(String boltId) {
            super(boltId);
            _boltId = boltId;
        }


        /**
         * 根据fields指定的字段来分组
         * @param componentId
         * @param fields
         * @return
         */
        public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
            return fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
        }

        /**
         * 指定流，根据fields指定的字段来分组。
         * @param componentId
         * @param streamId
         * @param fields
         * @return
         */
        public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
            return grouping(componentId, streamId, Grouping.fields(fields.toList()));
        }

        public BoltDeclarer globalGrouping(String componentId) {
            return globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer globalGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.fields(new ArrayList<String>()));
        }

        public BoltDeclarer shuffleGrouping(String componentId) {
            return shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.shuffle(new NullStruct()));
        }

        public BoltDeclarer localOrShuffleGrouping(String componentId) {
            return localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.local_or_shuffle(new NullStruct()));
        }
        
        public BoltDeclarer noneGrouping(String componentId) {
            return noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer noneGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.none(new NullStruct()));
        }

        public BoltDeclarer allGrouping(String componentId) {
            return allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer allGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.all(new NullStruct()));
        }

        public BoltDeclarer directGrouping(String componentId) {
            return directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer directGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.direct(new NullStruct()));
        }

        private BoltDeclarer grouping(String componentId, String streamId, Grouping grouping) {
            /**
             * GlobalStreamId:上游组件ID、流ID、默认为default。标识当前bolt组件的上游组件为componentId。
             * 最后得到的信息为:来自上游的哪个组件、哪个流、分组策略是啥。
             */
            _commons.get(_boltId).put_to_inputs(new GlobalStreamId(componentId, streamId), grouping);
            return this;
        }

        /**
         * 详细见对InputDeclarer的注释。
         * @param componentId
         * @param fields
         * @return
         */
        @Override
        public BoltDeclarer partialKeyGrouping(String componentId, Fields fields) {
            return customGrouping(componentId, new PartialKeyGrouping(fields));
        }

        @Override
        public BoltDeclarer partialKeyGrouping(String componentId, String streamId, Fields fields) {
            return customGrouping(componentId, streamId, new PartialKeyGrouping(fields));
        }

        @Override
        public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
            return customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping);
        }

        @Override
        public BoltDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping) {
            return grouping(componentId, streamId, Grouping.custom_serialized(Utils.javaSerialize(grouping)));
        }

        @Override
        public BoltDeclarer grouping(GlobalStreamId id, Grouping grouping) {
            return grouping(id.get_componentId(), id.get_streamId(), grouping);
        }        
    }
    
    private static Map parseJson(String json) {
        if(json==null) return new HashMap();
        else return (Map) JSONValue.parse(json);
    }
    
    private static String mergeIntoJson(Map into, Map newMap) {
        Map res = new HashMap(into);
        if(newMap!=null) res.putAll(newMap);
        return JSONValue.toJSONString(res);
    }
}
