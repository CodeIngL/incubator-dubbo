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

package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.filter.tps.DefaultTPSLimiter;
import org.apache.dubbo.rpc.filter.tps.TPSLimiter;

/**
 * TpsLimitFilter limit the TPS (transaction per second) for all method of a service or a particular method.
 * Service or method url can define <b>tps</b> or <b>tps.interval</b> to control this control.It use {@link DefaultTPSLimiter}
 * as it limit checker. If a provider service method is configured with <b>tps</b>(optionally with <b>tps.interval</b>),then
 * if invocation count exceed the configured <b>tps</b> value (default is -1 which means unlimited) then invocation will get
 * RpcException.
 * <p>
 *     TpsLimitFilter限制服务或特定方法的所有方法的TPS（每秒事务数）。 服务或方法url可以定义tps或tps.interval来控制此控件。它使用DefaultTPSLimiter作为限制检查器。
 *     如果使用tps（可选地使用tps.interval）配置了提供者服务方法，那么如果调用计数超过配置的tps值（默认值为-1，这意味着无限制），则调用将获得RpcException
 * </p>
 * */
@Activate(group = Constants.PROVIDER, value = Constants.TPS_LIMIT_RATE_KEY)
public class TpsLimitFilter implements Filter {

    private final TPSLimiter tpsLimiter = new DefaultTPSLimiter();

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        if (!tpsLimiter.isAllowable(invoker.getUrl(), invocation)) {
            throw new RpcException(
                    "Failed to invoke service " +
                            invoker.getInterface().getName() +
                            "." +
                            invocation.getMethodName() +
                            " because exceed max service tps.");
        }

        return invoker.invoke(invocation);
    }

}
