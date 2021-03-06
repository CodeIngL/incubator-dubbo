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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

import static org.apache.dubbo.common.Constants.DEFAULT_KEY;
import static org.apache.dubbo.common.Constants.REGISTRY_KEY;

/**
 *
 */
public class RegistryAwareClusterInvoker<T> extends AbstractClusterInvoker<T> {


    public RegistryAwareClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        // First, pick the invoker (XXXClusterInvoker) that comes from the local registry, distinguish by a 'default' key.
        // 首先，选择来自本地注册表的调用者（XXXClusterInvoker），用“default”键区分。
        for (Invoker<T> invoker : invokers) {
            if (invoker.isAvailable() && invoker.getUrl().getParameter(REGISTRY_KEY + "." + DEFAULT_KEY, false)) {
                return invoker.invoke(invocation);
            }
        }
        // If none of the invokers has a local signal, pick the first one available.
        // 如果没有任何调用者有本地信号，请选择第一个可用的信号
        for (Invoker<T> invoker : invokers) {
            if (invoker.isAvailable()) {
                return invoker.invoke(invocation);
            }
        }
        throw new RpcException("No provider available in " + invokers);
    }
}
