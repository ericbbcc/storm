;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns backtype.storm.LocalCluster
  (:use [backtype.storm testing config])
  (:import [java.util Map])
  (:gen-class   ;;生成class
    :init init
    :implements [backtype.storm.ILocalCluster] ;;实现ILocalCluster接口
    :constructors {[] [] [java.util.Map] [] [String Long] []} ;;两个构造函数的参数列表定义 [当前构造函数参数] [父类构造函数参数]
    :state state)) ;;state表示本类状态

(defn -init ;;实现构造函数，其中每个方法的返回值为 [父类构造函数参数列表] [当前类的state]
  ([] ;;无参构造函数的实现
   (let [ret (mk-local-storm-cluster  ;;调用mk-local-storm-cluster方法,传入:daemon关键字对应的map结构参数，其实就是本地集群的配置
               :daemon-conf           ;;并将结果赋值给ret，mk-local-storm-cluster方法是testting定义的
               {TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true})]
     [[] ret])) ;;将上面方法返回值ret作为本类的state存储
  ([^String zk-host ^Long zk-port];;参数列表为类型为String,Long的构造方法，这里指定了本地集群的IP地址和端口号
   (let [ret (mk-local-storm-cluster :daemon-conf {TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true
                                                     STORM-ZOOKEEPER-SERVERS (list zk-host)
                                                     STORM-ZOOKEEPER-PORT zk-port})]
     [[] ret]))
  ([^Map stateMap]  ;;参数为Map类型的构造方法，支持将配置直接赋值
   [[] stateMap]))

;;实现ILocalCluster接口定义的submitTopology方法
;;调用submit-local-topology
;;传入{:nimbus state} name conf topology四个参数，其中第一个是map结构,代表着这个本地集群的状态
(defn -submitTopology
  [this name conf topology]
  (submit-local-topology
    (:nimbus (. this state)) name conf topology))


(defn -submitTopologyWithOpts
  [this name conf topology submit-opts]
  (submit-local-topology-with-opts
    (:nimbus (. this state)) name conf topology submit-opts))

(defn -uploadNewCredentials
  [this name creds]
  (.uploadNewCredentials (:nimbus (. this state)) name creds))

(defn -shutdown
  [this]
  (kill-local-storm-cluster (. this state)))

(defn -killTopology
  [this name]
  (.killTopology (:nimbus (. this state)) name))

(defn -getTopologyConf
  [this id]
  (.getTopologyConf (:nimbus (. this state)) id))

(defn -getTopology
  [this id]
  (.getTopology (:nimbus (. this state)) id))

(defn -getClusterInfo
  [this]
  (.getClusterInfo (:nimbus (. this state))))

(defn -getTopologyInfo
  [this id]
  (.getTopologyInfo (:nimbus (. this state)) id))

(defn -killTopologyWithOpts
  [this name opts]
  (.killTopologyWithOpts (:nimbus (. this state)) name opts))

(defn -activate
  [this name]
  (.activate (:nimbus (. this state)) name))

(defn -deactivate
  [this name]
  (.deactivate (:nimbus (. this state)) name))

(defn -rebalance
  [this name opts]
  (.rebalance (:nimbus (. this state)) name opts))

(defn -getState
  [this]
  (.state this))
