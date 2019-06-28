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
package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.RegistryService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractRegistryFactory. (SPI, Singleton, ThreadSafe)
 *
 * @see org.apache.dubbo.registry.RegistryFactory
 */
public abstract class AbstractRegistryFactory implements RegistryFactory {

    // Log output
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRegistryFactory.class);

    // The lock for the acquisition process of the registry
    private static final ReentrantLock LOCK = new ReentrantLock();

    // Registry Collection Map<RegistryAddress, Registry>
    /**
     * 注册中心映射 Map<String, Registry>
     * key为相应的注册中心url的部分:protocol://username:password@ip:port/group/com.alibaba.dubbo.registry:version,value为对应的注册中心
     */
    private static final Map<String, Registry> REGISTRIES = new HashMap<>();

    /**
     * Get all registries
     *
     * @return all registries
     */
    public static Collection<Registry> getRegistries() {
        return Collections.unmodifiableCollection(REGISTRIES.values());
    }

    /**
     * Close all created registries
     */
    // TODO: 2017/8/30 to move somewhere else better
    public static void destroyAll() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Close all registries " + getRegistries());
        }
        // Lock up the registry shutdown process
        LOCK.lock();
        try {
            for (Registry registry : getRegistries()) {
                try {
                    registry.destroy();
                } catch (Throwable e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            REGISTRIES.clear();
        } finally {
            // Release the lock
            LOCK.unlock();
        }
    }

    /**
     * 获得实际的注册中心实例
     * <p>相关逻辑
     * <ul>对url进行设置，path为com.alibaba.dubbo.registry.Registry,移除其他非注册中心的属性，比如export，refer</ul><br/>
     * <ul>从url中获得关键key:protocol://username:password@ip:port/group/com.alibaba.dubbo.registry.{@link RegistryService}:version，一个注册中心对应的url对应一个注册中心</ul><br/>
     * <ul>缓存操作，构建或获取注册中心实际实例</ul><br/>
     * </p>
     * tip: 在一般情况下，对应某个特定的protocol，其userName和password都对应了，对本机来说，ip和port也就定了，因此只有group是可以操作的部分，也就是说相关的group组成了一个注册中心，而且這個group是
     * 有注册中心url自己配置的而不是从普通的url上转移过来的，也就是说一般的开发者不会配置该项group，对于zookeeper来说，一般开发者也不会配置username，password，和version，所以最终的结果是
     * zookeeper://ip:port/com.alibaba.dubbo.registry.{@link RegistryService}
     *
     * @param url 注册中心地址，不允许为空
     * @return 注册中心实例
     */
    @Override
    public Registry getRegistry(URL url) {
        // 构建属于注册中心url信息的utl，写入path，增加键值对，移除会变化的部分。
        // 主要是export键和refer键（只剩下注册中心的信息了）
        url = URLBuilder.from(url)
                .setPath(RegistryService.class.getName())
                .addParameter(Constants.INTERFACE_KEY, RegistryService.class.getName())
                .removeParameters(Constants.EXPORT_KEY, Constants.REFER_KEY)
                .build();
        //获得注册中心和该注册中心url对应的key值:protocol://username:password@ip:port/group/com.alibaba.dubbo.registry:version
        //构建注册中心相关身份key，不需要url的相关参数，能区分不同的注册中心就可以了，因为要存储在本地
        String key = url.toServiceStringWithoutResolving();
        // Lock the registry access process to ensure a single instance of the registry
        //锁定注册中心获取过程，保证注册中心单一实例
        //尝试从缓存中取，没有则新建
        LOCK.lock();
        try {
            Registry registry = REGISTRIES.get(key);
            if (registry != null) {
                return registry;
            }
            //create registry by spi/ioc
            //创建注册中心实例，
            registry = createRegistry(url);
            if (registry == null) {
                throw new IllegalStateException("Can not create registry " + url);
            }
            REGISTRIES.put(key, registry);
            return registry;
        } finally {
            // Release the lock
            LOCK.unlock();
        }
    }

    /**
     * 使用注册中心对应url（key）来构建一个对应的注册中心
     * @param url
     * @return
     */
    protected abstract Registry createRegistry(URL url);

}
