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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * <b>NOTICE!!</b>
 *
 * <p>
 * You should never rely on this class directly when using or extending Dubbo, the implementation of {@link AsyncRpcResult}
 * is only a workaround for compatibility purpose. It may be changed or even get removed from the next major version.
 * Please only use {@link Result} or {@link RpcResult}.
 * <p>
 * Extending the {@link Filter} is one typical use case:
 * <pre>
 * {@code
 * public class YourFilter implements Filter {
 *     @Override
 *     public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
 *         System.out.println("Filter get the return value: " + result.getValue());
 *         // Don't do this
 *         // AsyncRpcResult asyncRpcResult = ((AsyncRpcResult)result;
 *         // System.out.println("Filter get the return value: " + asyncRpcResult.getValue());
 *         return result;
 *     }
 *
 *     @Override
 *     public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
 *         return invoker.invoke(invocation);
 *     }
 * }
 * }
 * </pre>
 * </p>
 * TODO RpcResult can be an instance of {@link java.util.concurrent.CompletionStage} instead of composing CompletionStage inside.
 */
public class AsyncRpcResult extends AbstractResult {
    private static final Logger logger = LoggerFactory.getLogger(AsyncRpcResult.class);

    /**
     * RpcContext can be changed, because thread may have been used by other thread. It should be cloned before store.
     * So we use Invocation instead, Invocation will create for every invoke, but invocation only support attachments of string type.
     */
    private RpcContext storedContext;
    private RpcContext storedServerContext;

    protected CompletableFuture<Object> valueFuture;

    protected CompletableFuture<Result> resultFuture;

    public AsyncRpcResult(CompletableFuture<Object> future) {
        this(future, true);
    }

    public AsyncRpcResult(CompletableFuture<Object> future, boolean registerCallback) {
        this(future, new CompletableFuture<>(), registerCallback);
    }

    /**
     * @param future
     * @param rFuture
     * @param registerCallback
     */
    public AsyncRpcResult(CompletableFuture<Object> future, final CompletableFuture<Result> rFuture, boolean registerCallback) {
        if (rFuture == null) {
            throw new IllegalArgumentException();
        }
        resultFuture = rFuture;
        if (registerCallback) {
            /**
             * We do not know whether future already completed or not, it's a future exposed or even created by end user.
             * 1. future complete before whenComplete. whenComplete fn (resultFuture.complete) will be executed in thread subscribing, in our case, it's Dubbo thread.
             * 2. future complete after whenComplete. whenComplete fn (resultFuture.complete) will be executed in thread calling complete, normally its User thread.
             */
            /**
             * 我们不知道future是否已经完成，这是future暴露甚至最终用户创造的。
             * 1.future完成在whenComplete之前完成。 whenComplete fn（resultFuture.complete）将在线程订阅中执行，在我们的例子中，它是Dubbo线程。
             * 2.future完成在whenComplete之后完成。 whenComplete fn（resultFuture.complete）将在线程调用完成时执行，通常是其用户User线程。
             */
            future.whenComplete((v, t) -> {
                RpcResult rpcResult;
                if (t != null) {
                    if (t instanceof CompletionException) {
                        rpcResult = new RpcResult(t.getCause());
                    } else {
                        rpcResult = new RpcResult(t);
                    }
                } else {
                    rpcResult = new RpcResult(v);
                }
                // instead of resultFuture we must use rFuture here, resultFuture may being changed before complete when building filter chain, but rFuture was guaranteed never changed by closure.
                // 而不是resultFuture我们必须在这里使用rFuture，结果在构建过滤器链之前，Future可能会在完成之前被更改，但rFuture保证永远不会被闭包改变。
                rFuture.complete(rpcResult);
            });
        }
        this.valueFuture = future;
        // employ copy of context avoid the other call may modify the context content
        this.storedContext = RpcContext.getContext().copyOf();
        this.storedServerContext = RpcContext.getServerContext().copyOf();
    }

    @Override
    public Object getValue() {
        return getRpcResult().getValue();
    }

    @Override
    public Throwable getException() {
        return getRpcResult().getException();
    }

    @Override
    public boolean hasException() {
        return getRpcResult().hasException();
    }

    @Override
    public Object getResult() {
        return getRpcResult().getResult();
    }

    public CompletableFuture getValueFuture() {
        return valueFuture;
    }

    public CompletableFuture<Result> getResultFuture() {
        return resultFuture;
    }

    public void setResultFuture(CompletableFuture<Result> resultFuture) {
        this.resultFuture = resultFuture;
    }

    public Result getRpcResult() {
        try {
            if (resultFuture.isDone()) {
                return resultFuture.get();
            }
        } catch (Exception e) {
            // This should never happen;
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.", e);
        }
        return new RpcResult();
    }

    @Override
    public Object recreate() throws Throwable {
        return valueFuture;
    }

    public void thenApplyWithContext(Function<Result, Result> fn) {
        this.resultFuture = resultFuture.thenApply(fn.compose(beforeContext).andThen(afterContext));
    }

    @Override
    public Map<String, String> getAttachments() {
        return getRpcResult().getAttachments();
    }

    @Override
    public void setAttachments(Map<String, String> map) {
        getRpcResult().setAttachments(map);
    }

    @Override
    public void addAttachments(Map<String, String> map) {
        getRpcResult().addAttachments(map);
    }

    @Override
    public String getAttachment(String key) {
        return getRpcResult().getAttachment(key);
    }

    @Override
    public String getAttachment(String key, String defaultValue) {
        return getRpcResult().getAttachment(key, defaultValue);
    }

    @Override
    public void setAttachment(String key, String value) {
        getRpcResult().setAttachment(key, value);
    }

    /**
     * tmp context to use when the thread switch to Dubbo thread.
     */
    private RpcContext tmpContext;
    private RpcContext tmpServerContext;

    private Function<Result, Result> beforeContext = (result) -> {
        tmpContext = RpcContext.getContext();
        tmpServerContext = RpcContext.getServerContext();
        RpcContext.restoreContext(storedContext);
        RpcContext.restoreServerContext(storedServerContext);
        return result;
    };

    private Function<Result, Result> afterContext = (result) -> {
        RpcContext.restoreContext(tmpContext);
        RpcContext.restoreServerContext(tmpServerContext);
        return result;
    };
}

