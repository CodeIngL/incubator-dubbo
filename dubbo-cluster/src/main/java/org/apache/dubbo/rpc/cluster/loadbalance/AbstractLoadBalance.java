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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

/**
 * AbstractLoadBalance
 */
public abstract class AbstractLoadBalance implements LoadBalance {
    /**
     * Calculate the weight according to the uptime proportion of warmup time
     * the new weight will be within 1(inclusive) to weight(inclusive)
     * <p>
     *     根据正常时间比例计算重量，新重量将在1（含）到重量（含）内
     *
     * @param uptime the uptime in milliseconds 服务启动时间
     * @param warmup the warmup time in milliseconds 预热时间
     * @param weight the weight of an invoker invoker的权重
     * @return weight which takes warmup into account 权重
     */
    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
        // 计算权重，下面代码逻辑上形似于 (uptime/warmup) * weight。
        // 随着服务运行时间 uptime 增大，权重计算值 ww 会慢慢接近配置值 weight
        int ww = (int) ((float) uptime / ((float) warmup / (float) weight));
        return ww < 1 ? 1 : (ww > weight ? weight : ww);
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        //空的就直接返回
        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }
        // 如果 invokers 列表中仅有一个 Invoker，直接返回即可，无需进行负载均衡
        if (invokers.size() == 1) {
            return invokers.get(0);
        }
        // 调用 doSelect 方法进行负载均衡，该方法为抽象方法，由子类实现
        return doSelect(invokers, url, invocation);
    }

    /**
     * 由子类实现特定的负载均衡算法
     * @param invokers
     * @param url
     * @param invocation
     * @param <T>
     * @return
     */
    protected abstract <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation);


    /**
     * Get the weight of the invoker's invocation which takes warmup time into account
     * if the uptime is within the warmup time, the weight will be reduce proportionally
     * <p>
     *     获得调用者调用的权重，如果正常运行时间在预热时间内，则需要将预热时间考虑在内，权重将按比例减少
     *
     * @param invoker    the invoker
     * @param invocation the invocation of this invoker
     * @return weight
     */
    protected int getWeight(Invoker<?> invoker, Invocation invocation) {
        // 从 url 中获取权重 weight 配置值，默认100
        int weight = invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.WEIGHT_KEY, Constants.DEFAULT_WEIGHT);
        if (weight > 0) {
            // 获取服务提供者启动时间戳，默认是0
            long timestamp = invoker.getUrl().getParameter(Constants.REMOTE_TIMESTAMP_KEY, 0L);
            if (timestamp > 0L) {
                // 计算服务提供者运行时长
                int uptime = (int) (System.currentTimeMillis() - timestamp);
                // 获取服务预热时间，默认为10分钟
                int warmup = invoker.getUrl().getParameter(Constants.WARMUP_KEY, Constants.DEFAULT_WARMUP);
                // 如果服务运行时间小于预热时间，则重新计算服务权重，即降权
                if (uptime > 0 && uptime < warmup) {
                    // 重新计算服务权重
                    weight = calculateWarmupWeight(uptime, warmup, weight);
                }
            }
        }
        return weight >= 0 ? weight : 0;
    }

}
