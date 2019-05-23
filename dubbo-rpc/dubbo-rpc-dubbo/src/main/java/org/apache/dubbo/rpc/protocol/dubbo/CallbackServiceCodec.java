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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcInvocation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * callback service helper
 */
class CallbackServiceCodec {
    private static final Logger logger = LoggerFactory.getLogger(CallbackServiceCodec.class);

    private static final ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    private static final DubboProtocol protocol = DubboProtocol.getDubboProtocol();
    private static final byte CALLBACK_NONE = 0x0;
    private static final byte CALLBACK_CREATE = 0x1;
    private static final byte CALLBACK_DESTROY = 0x2;
    private static final String INV_ATT_CALLBACK_KEY = "sys_callback_arg-";

    /**
     * 是否是callBack
     * @param url
     * @param methodName 方法名
     * @param argIndex 参数位置
     * @return
     */
    private static byte isCallBack(URL url, String methodName, int argIndex) {
        // 参数回调规则:methodName.parameterIndex(从0开始).callback,指明对应的参数是一个回调
        byte isCallback = CALLBACK_NONE;
        if (url == null){
            return isCallback;
        }
        //关键的callback信息
        String callback = url.getParameter(methodName + "." + argIndex + ".callback");
        if (callback == null){
            return isCallback;
        }
        if (callback.equalsIgnoreCase("true")) {//"true"指明构建一个回调参数
            isCallback = CALLBACK_CREATE;
        } else if (callback.equalsIgnoreCase("false")) {//"false"指明销毁回调
            isCallback = CALLBACK_DESTROY;
        }
        //使用值
        return isCallback;
    }

    /**
     * export or unexport callback service on client side
     * <p>
     *     客户端的export或unexport回调服务
     *
     * @param channel 网络channel
     * @param url 调用的url
     * @param clazz 回调的类
     * @param inst 回调的参数
     * @param export 导出or销毁
     * @throws IOException
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static String exportOrUnexportCallbackService(Channel channel, URL url, Class clazz, Object inst, Boolean export) throws IOException {
        //实例id
        int instid = System.identityHashCode(inst);

        Map<String, String> params = new HashMap<>(3);
        // 再也不需要新客户了
        params.put(Constants.IS_SERVER_KEY, Boolean.FALSE.toString());
        // 标记它是一个回调，用于故障排除
        params.put(Constants.IS_CALLBACK_SERVICE, Boolean.TRUE.toString());
        //group
        String group = (url == null ? null : url.getParameter(Constants.GROUP_KEY));
        if (group != null && group.length() > 0) {
            params.put(Constants.GROUP_KEY, group);
        }
        // add方法，用于验证方法，自动回退（参见dubbo协议）
        params.put(Constants.METHODS_KEY, StringUtils.join(Wrapper.getWrapper(clazz).getDeclaredMethodNames(), ","));

        //参数
        Map<String, String> tmpMap = new HashMap<>(url.getParameters());
        tmpMap.putAll(params);
        tmpMap.remove(Constants.VERSION_KEY);// doesn't need to distinguish version for callback 不需要区分版本的回调
        tmpMap.put(Constants.INTERFACE_KEY, clazz.getName()); //客户端的回调服务接口名
        //客户端需要暴露的url,path是接口名加实例的id
        URL exportUrl = new URL(DubboProtocol.NAME, channel.getLocalAddress().getAddress().getHostAddress(), channel.getLocalAddress().getPort(), clazz.getName() + "." + instid, tmpMap);

        // no need to generate multiple exporters for different channel in the same JVM, cache key cannot collide.
        // 不需要为callback区分版本需要在同一个JVM中为不同的通道生成多个导出器，缓存键不能冲突
        String cacheKey = getClientSideCallbackServiceCacheKey(instid);
        //该接口下的同类型的回调的数量数量
        String countKey = getClientSideCountKey(clazz.getName());
        if (export) {
            // one channel can have multiple callback instances, no need to re-export for different instance.
            // 一个channel可以有多个回调实例，不需要为不同的实例重新导出
            if (!channel.hasAttribute(cacheKey)) { //通道上没有这个实例
                if (!isInstancesOverLimit(channel, url, clazz.getName(), instid, false)) {
                    Invoker<?> invoker = proxyFactory.getInvoker(inst, clazz, exportUrl);
                    // should destroy resource?
                    Exporter<?> exporter = protocol.export(invoker);
                    // this is used for tracing if instid has published service or not.
                    // 如果instid已发布服务，则用于跟踪。
                    channel.setAttribute(cacheKey, exporter);
                    logger.info("Export a callback service :" + exportUrl + ", on " + channel + ", url is: " + url);
                    increaseInstanceCount(channel, countKey);
                }
            }
        } else {
            if (channel.hasAttribute(cacheKey)) { //存在cacheKey
                Exporter<?> exporter = (Exporter<?>) channel.getAttribute(cacheKey);
                exporter.unexport();
                channel.removeAttribute(cacheKey);
                decreaseInstanceCount(channel, countKey);
            }
        }
        return String.valueOf(instid);
    }


    /**
     * refer or destroy callback service on server side
     * <p>
     *     在服务器端引用或销毁回调服务
     * @param channel 网络channel
     * @param url
     * @param clazz 回调类型
     * @param inv
     * @param instid 回调实例id
     * @param isRefer 是否引用
     * @return
     */
    @SuppressWarnings("unchecked")
    private static Object referOrDestroyCallbackService(Channel channel, URL url, Class<?> clazz, Invocation inv, int instid, boolean isRefer) {
        Object proxy = null;
        //获得invokerkey的标识 = proxy的标识+".invoke"
        String invokerCacheKey = getServerSideCallbackInvokerCacheKey(channel, clazz.getName(), instid);
        //获得proxy的标识
        String proxyCacheKey = getServerSideCallbackServiceCacheKey(channel, clazz.getName(), instid);
        //获得proxy
        proxy = channel.getAttribute(proxyCacheKey);
        //该类型上的回调的次数
        String countkey = getServerSideCountKey(channel, clazz.getName());
        if (isRefer) {
            if (proxy == null) {
                URL referurl = URL.valueOf("callback://" + url.getAddress() + "/" + clazz.getName() + "?" + Constants.INTERFACE_KEY + "=" + clazz.getName());
                referurl = referurl.addParametersIfAbsent(url.getParameters()).removeParameter(Constants.METHODS_KEY);
                if (!isInstancesOverLimit(channel, referurl, clazz.getName(), instid, true)) {
                    @SuppressWarnings("rawtypes")
                    Invoker<?> invoker = new ChannelWrappedInvoker(clazz, channel, referurl, String.valueOf(instid));//构建服务端能够回到远程客户的端的网络invoker
                    proxy = proxyFactory.getProxy(invoker); //将这个一个invoker转换成可以理解的proxy
                    channel.setAttribute(proxyCacheKey, proxy);//设置到channel上
                    channel.setAttribute(invokerCacheKey, invoker);
                    increaseInstanceCount(channel, countkey); //统计

                    //convert error fail fast .
                    //ignore concurrent problem.
                    Set<Invoker<?>> callbackInvokers = (Set<Invoker<?>>) channel.getAttribute(Constants.CHANNEL_CALLBACK_KEY);
                    if (callbackInvokers == null) {
                        callbackInvokers = new ConcurrentHashSet<Invoker<?>>(1);
                        callbackInvokers.add(invoker);
                        channel.setAttribute(Constants.CHANNEL_CALLBACK_KEY, callbackInvokers);
                    }
                    logger.info("method " + inv.getMethodName() + " include a callback service :" + invoker.getUrl() + ", a proxy :" + invoker + " has been created.");
                }
            }
        } else {
            if (proxy != null) {//删除掉
                Invoker<?> invoker = (Invoker<?>) channel.getAttribute(invokerCacheKey);
                try {
                    Set<Invoker<?>> callbackInvokers = (Set<Invoker<?>>) channel.getAttribute(Constants.CHANNEL_CALLBACK_KEY);
                    if (callbackInvokers != null) {
                        callbackInvokers.remove(invoker);
                    }
                    invoker.destroy();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                // cancel refer, directly remove from the map
                channel.removeAttribute(proxyCacheKey);
                channel.removeAttribute(invokerCacheKey);
                decreaseInstanceCount(channel, countkey);
            }
        }
        return proxy;
    }

    /**
     * callback.service.instid
     * @param instid
     * @return
     */
    private static String getClientSideCallbackServiceCacheKey(int instid) {
        return Constants.CALLBACK_SERVICE_KEY + "." + instid;
    }

    private static String getServerSideCallbackServiceCacheKey(Channel channel, String interfaceClass, int instid) {
        return Constants.CALLBACK_SERVICE_PROXY_KEY + "." + System.identityHashCode(channel) + "." + interfaceClass + "." + instid;
    }

    private static String getServerSideCallbackInvokerCacheKey(Channel channel, String interfaceClass, int instid) {
        return getServerSideCallbackServiceCacheKey(channel, interfaceClass, instid) + "." + "invoker";
    }

    private static String getClientSideCountKey(String interfaceClass) {
        return Constants.CALLBACK_SERVICE_KEY + "." + interfaceClass + ".COUNT";
    }

    private static String getServerSideCountKey(Channel channel, String interfaceClass) {
        return Constants.CALLBACK_SERVICE_PROXY_KEY + "." + System.identityHashCode(channel) + "." + interfaceClass + ".COUNT";
    }

    /**
     * 实例超过限制
     * @param channel
     * @param url
     * @param interfaceClass
     * @param instid
     * @param isServer 是否是服务
     * @return
     */
    private static boolean isInstancesOverLimit(Channel channel, URL url, String interfaceClass, int instid, boolean isServer) {
        //获得该接口在客户端或者服务端的回调的数量
        Integer count = (Integer) channel.getAttribute(isServer ? getServerSideCountKey(channel, interfaceClass) : getClientSideCountKey(interfaceClass));
        //默认限制是1
        int limit = url.getParameter(Constants.CALLBACK_INSTANCES_LIMIT_KEY, Constants.DEFAULT_CALLBACK_INSTANCES);
        if (count != null && count >= limit) {
            //client side error
            throw new IllegalStateException("interface " + interfaceClass + " `s callback instances num exceed providers limit :" + limit
                    + " ,current num: " + (count + 1) + ". The new callback service will not work !!! you can cancle the callback service which exported before. channel :" + channel);
        } else {
            return false;
        }
    }

    private static void increaseInstanceCount(Channel channel, String countkey) {
        try {
            //ignore concurrent problem?
            Integer count = (Integer) channel.getAttribute(countkey);
            if (count == null) {
                count = 1;
            } else {
                count++;
            }
            channel.setAttribute(countkey, count);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void decreaseInstanceCount(Channel channel, String countkey) {
        try {
            Integer count = (Integer) channel.getAttribute(countkey);
            if (count == null || count <= 0) {
                return;
            } else {
                count--;
            }
            channel.setAttribute(countkey, count);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 编码要进行rpc调用的参数，处理相关callback，
     * callback是创建还是关闭
     * @param channel
     * @param inv
     * @param paraIndex 参数坐标
     * @return
     * @throws IOException
     */
    public static Object encodeInvocationArgument(Channel channel, RpcInvocation inv, int paraIndex) throws IOException {
        // 直接获得url
        URL url = inv.getInvoker() == null ? null : inv.getInvoker().getUrl(); //获得目的url
        byte callbackStatus = isCallBack(url, inv.getMethodName(), paraIndex); //校验第x个元素是否是一个callback
        Object[] args = inv.getArguments(); //参数
        Class<?>[] pts = inv.getParameterTypes(); //参数类型
        switch (callbackStatus) { //是否是callback
            case CallbackServiceCodec.CALLBACK_CREATE:
            case CallbackServiceCodec.CALLBACK_DESTROY:
                //sys_callback_arg-paraIndex，实例id
                inv.setAttachment(INV_ATT_CALLBACK_KEY + paraIndex, exportOrUnexportCallbackService(channel, url, pts[paraIndex], args[paraIndex], callbackStatus == CALLBACK_CREATE));
                return null;
            case CALLBACK_NONE:
            default:
                return args[paraIndex];
        }
    }

    /**
     * 解码rpc调用的参数
     * @param channel
     * @param inv
     * @param pts
     * @param paraIndex
     * @param inObject
     * @return
     * @throws IOException
     */
    public static Object decodeInvocationArgument(Channel channel, RpcInvocation inv, Class<?>[] pts, int paraIndex, Object inObject) throws IOException {
        // if it's a callback, create proxy on client side, callback interface on client side can be invoked through channel
        // need get URL from channel and env when decode
        // 如果它是一个回调，在客户端创建代理，客户端的回调接口可以通过通道需要获取来自通道的URL和inv在解码时调用
        URL url = null;
        try {
            url = DubboProtocol.getDubboProtocol().getInvoker(channel, inv).getUrl();
        } catch (RemotingException e) {
            if (logger.isInfoEnabled()) {
                logger.info(e.getMessage(), e);
            }
            return inObject;
        }
        //是否是callback
        byte callbackStatus = isCallBack(url, inv.getMethodName(), paraIndex);
        switch (callbackStatus) {
            case CALLBACK_CREATE:
            case CALLBACK_DESTROY:
                try {
                    //sys_callback_arg-paraIndex,该参数需要引用远程的实例
                    return referOrDestroyCallbackService(channel, url, pts[paraIndex], inv, Integer.parseInt(inv.getAttachment(INV_ATT_CALLBACK_KEY + paraIndex)), callbackStatus == CALLBACK_CREATE);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    throw new IOException(StringUtils.toString(e));
                }
            case CALLBACK_NONE:
            default:
                return inObject;
        }
    }
}
