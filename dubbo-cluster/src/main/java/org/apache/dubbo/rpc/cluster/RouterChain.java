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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Router chain
 * 路由链
 */
public class RouterChain<T> {

    // full list of addresses from registry, classified by method name.
    // 注册表中的完整地址列表，按方法名称分类。
    private List<Invoker<T>> invokers = Collections.emptyList();

    // containing all routers, reconstruct every time 'route://' urls change.
    // 包含所有路由器，每次'route://'url更改时重建。
    private volatile List<Router> routers = Collections.emptyList();

    // Fixed router instances: ConfigConditionRouter, TagRouter, e.g., the rule for each instance may change but the
    // instance will never delete or recreate.
    // 固定路由器实例：ConfigConditionRouter，TagRouter，例如，每个实例的规则可能会更改但实例永远不会删除或重新创建
    private List<Router> builtinRouters = Collections.emptyList();

    /**
     * 为url构建路由链
     * @param url
     * @param <T>
     * @return
     */
    public static <T> RouterChain<T> buildChain(URL url) {
        return new RouterChain<>(url);
    }

    /**
     * 路由链,构造时使用一个从Extension中构造的作为路由链的一个固定的路由
     * @param url
     */
    private RouterChain(URL url) {
        //获得url对应的RouterFactory列表
        List<RouterFactory> extensionFactories = ExtensionLoader.getExtensionLoader(RouterFactory.class)
                .getActivateExtension(url, (String[]) null);

        // 利用Factory从url中提取相应的routers作为固定routers
        List<Router> routers = extensionFactories.stream()
                .map(factory -> factory.getRouter(url))
                .collect(Collectors.toList());

        // 初始化
        initWithRouters(routers);
    }

    /**
     * the resident routers must being initialized before address notification.
     * 构建固定路由器，在动态的调整之前，进行我们的构建
     * FIXME: this method should not be public
     */
    public void initWithRouters(List<Router> builtinRouters) {
        this.builtinRouters = builtinRouters;
        this.routers = new CopyOnWriteArrayList<>(builtinRouters);
        this.sort();
    }

    /**
     * If we use route:// protocol in version before 2.7.0, each URL will generate a Router instance, so we should
     * keep the routers up to date, that is, each time router URLs changes, we should update the routers list, only
     * keep the builtinRouters which are available all the time and the latest notified routers which are generated
     * from URLs.
     * <p>
     *     如果我们在2.7.0之前的版本中使用route://协议，每个URL都会生成一个Router实例，所以我们应该让路由器保持最新状态，
     *     也就是说，每次路由器URL更改时，我们都应该更新路由器列表，
     *     保留始终可用的builtinRouters以及从URL生成的最新通知路由器。
     * </p>
     *
     * @param routers routers from 'router://' rules in 2.6.x or before.
     */
    public void addRouters(List<Router> routers) {
        List<Router> newRouters = new CopyOnWriteArrayList<>();
        newRouters.addAll(builtinRouters);
        newRouters.addAll(routers);
        CollectionUtils.sort(routers);
        this.routers = newRouters;
    }

    private void sort() {
        Collections.sort(routers);
    }

    /**
     *
     * @param url
     * @param invocation
     * @return
     */
    public List<Invoker<T>> route(URL url, Invocation invocation) {
        List<Invoker<T>> finalInvokers = invokers;
        for (Router router : routers) {
            finalInvokers = router.route(finalInvokers, url, invocation);
        }
        return finalInvokers;
    }

    /**
     * Notify router chain of the initial addresses from registry at the first time.
     * Notify whenever addresses in registry change.
     */
    public void setInvokers(List<Invoker<T>> invokers) {
        this.invokers = (invokers == null ? Collections.emptyList() : invokers);
        routers.forEach(router -> router.notify(this.invokers));
    }
}
