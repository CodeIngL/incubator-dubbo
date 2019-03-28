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
package org.apache.dubbo.rpc.protocol.hessian;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.http.HttpBinder;
import org.apache.dubbo.remoting.http.HttpHandler;
import org.apache.dubbo.remoting.http.HttpServer;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.AbstractProxyProtocol;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import com.caucho.hessian.HessianException;
import com.caucho.hessian.client.HessianConnectionException;
import com.caucho.hessian.client.HessianConnectionFactory;
import com.caucho.hessian.client.HessianProxyFactory;
import com.caucho.hessian.io.HessianMethodSerializationException;
import com.caucho.hessian.server.HessianSkeleton;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.dubbo.common.Constants.DEFAULT_EXCHANGER;
import static org.apache.dubbo.common.Constants.DEFAULT_HTTP_CLIENT;

/**
 * http rpc support.
 *
 * http rpc 支持
 * hessian
 */
public class HessianProtocol extends AbstractProxyProtocol {

    private final Map<String, HttpServer> serverMap = new ConcurrentHashMap<String, HttpServer>();

    private final Map<String, HessianSkeleton> skeletonMap = new ConcurrentHashMap<String, HessianSkeleton>();

    private HttpBinder httpBinder;

    public HessianProtocol() {
        super(HessianException.class);
    }

    public void setHttpBinder(HttpBinder httpBinder) {
        this.httpBinder = httpBinder;
    }

    @Override
    public int getDefaultPort() {
        return 80;
    }

    /**
     * 暴露
     * @param impl 实现的代理实例
     * @param type 类型
     * @param url 地址
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    protected <T> Runnable doExport(T impl, Class<T> type, URL url) throws RpcException {
        String addr = getAddr(url); //构建地址
        HttpServer server = serverMap.get(addr); //获得服务
        if (server == null) {
            server = httpBinder.bind(url, new HessianHandler()); //暴露一个httpServer
            serverMap.put(addr, server);
        }
        final String path = url.getAbsolutePath(); //全路径
        final HessianSkeleton skeleton = new HessianSkeleton(impl, type); //构建一个
        skeletonMap.put(path, skeleton);

        final String genericPath = path + "/" + Constants.GENERIC_KEY; //泛化调用路径
        skeletonMap.put(genericPath, new HessianSkeleton(impl, GenericService.class)); //构建一个

        return new Runnable() {
            @Override
            public void run() {
                skeletonMap.remove(path);
                skeletonMap.remove(genericPath);
            }
        };
    }

    /**
     * 将服务引用转换为hessian实现
     * @param serviceType
     * @param url
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    @SuppressWarnings("unchecked")
    protected <T> T doRefer(Class<T> serviceType, URL url) throws RpcException {
        String generic = url.getParameter(Constants.GENERIC_KEY);
        boolean isGeneric = ProtocolUtils.isGeneric(generic) || serviceType.equals(GenericService.class);
        if (isGeneric) {
            RpcContext.getContext().setAttachment(Constants.GENERIC_KEY, generic);
            url = url.setPath(url.getPath() + "/" + Constants.GENERIC_KEY);
        }

        //构建hessian代理
        HessianProxyFactory hessianProxyFactory = new HessianProxyFactory();
        //是否是hessian2序列化的请求
        boolean isHessian2Request = url.getParameter(Constants.HESSIAN2_REQUEST_KEY, Constants.DEFAULT_HESSIAN2_REQUEST);
        hessianProxyFactory.setHessian2Request(isHessian2Request);
        //是否是覆盖
        boolean isOverloadEnabled = url.getParameter(Constants.HESSIAN_OVERLOAD_METHOD_KEY, Constants.DEFAULT_HESSIAN_OVERLOAD_METHOD);
        hessianProxyFactory.setOverloadEnabled(isOverloadEnabled);
        //默认的
        String client = url.getParameter(Constants.CLIENT_KEY, DEFAULT_HTTP_CLIENT);
        if ("httpclient".equals(client)) { //jdk
            HessianConnectionFactory factory = new HttpClientConnectionFactory();
            factory.setHessianProxyFactory(hessianProxyFactory);
            hessianProxyFactory.setConnectionFactory(factory);
        } else if (StringUtils.isNotEmpty(client) && !DEFAULT_HTTP_CLIENT.equals(client)) {
            //之支持httpclient或者jdk
            throw new IllegalStateException("Unsupported http protocol client=\"" + client + "\"!");
        } else {
            HessianConnectionFactory factory = new DubboHessianURLConnectionFactory();
            factory.setHessianProxyFactory(hessianProxyFactory);
            hessianProxyFactory.setConnectionFactory(factory);
        }
        int timeout = url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
        hessianProxyFactory.setConnectTimeout(timeout);
        hessianProxyFactory.setReadTimeout(timeout);
        //暴露代理
        return (T) hessianProxyFactory.create(serviceType, url.setProtocol("http").toJavaURL(), Thread.currentThread().getContextClassLoader());
    }

    @Override
    protected int getErrorCode(Throwable e) {
        if (e instanceof HessianConnectionException) {
            if (e.getCause() != null) {
                Class<?> cls = e.getCause().getClass();
                if (SocketTimeoutException.class.equals(cls)) {
                    return RpcException.TIMEOUT_EXCEPTION;
                }
            }
            return RpcException.NETWORK_EXCEPTION;
        } else if (e instanceof HessianMethodSerializationException) {
            return RpcException.SERIALIZATION_EXCEPTION;
        }
        return super.getErrorCode(e);
    }

    @Override
    public void destroy() {
        super.destroy();
        for (String key : new ArrayList<>(serverMap.keySet())) {
            HttpServer server = serverMap.remove(key);
            if (server != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Close hessian server " + server.getUrl());
                    }
                    server.close();
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }
    }

    private class HessianHandler implements HttpHandler {

        @Override
        public void handle(HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {
            String uri = request.getRequestURI(); //请求uri
            HessianSkeleton skeleton = skeletonMap.get(uri); //对应的hessionSkeleton
            if (!request.getMethod().equalsIgnoreCase("POST")) { //只允许post
                response.setStatus(500);
            } else {
                RpcContext.getContext().setRemoteAddress(request.getRemoteAddr(), request.getRemotePort());

                Enumeration<String> enumeration = request.getHeaderNames();
                while (enumeration.hasMoreElements()) {
                    String key = enumeration.nextElement();
                    if (key.startsWith(DEFAULT_EXCHANGER)) {
                        RpcContext.getContext().setAttachment(key.substring(DEFAULT_EXCHANGER.length()),
                                request.getHeader(key));
                    }
                }

                try {
                    skeleton.invoke(request.getInputStream(), response.getOutputStream()); //skeleton使用
                } catch (Throwable e) {
                    throw new ServletException(e);
                }
            }
        }

    }

}
