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
package org.apache.dubbo.remoting.transport.netty4;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.util.List;

/**
 * NettyCodecAdapter.
 * 编解码器适配器，封装了所有的编解码
 */
final public class NettyCodecAdapter {

    private final ChannelHandler encoder = new InternalEncoder();

    private final ChannelHandler decoder = new InternalDecoder();

    /**
     * dubbo编码解码器
     */
    private final Codec2 codec;

    private final URL url;

    private final org.apache.dubbo.remoting.ChannelHandler handler;

    public NettyCodecAdapter(Codec2 codec, URL url, org.apache.dubbo.remoting.ChannelHandler handler) {
        this.codec = codec;
        this.url = url;
        this.handler = handler;
    }

    public ChannelHandler getEncoder() {
        return encoder;
    }

    public ChannelHandler getDecoder() {
        return decoder;
    }

    /**
     * 编码器
     */
    private class InternalEncoder extends MessageToByteEncoder {

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            ChannelBuffer buffer = new NettyBackedChannelBuffer(out);
            Channel ch = ctx.channel();
            NettyChannel channel = NettyChannel.getOrAddChannel(ch, url, handler);
            try {
                codec.encode(channel, buffer, msg);
            } finally {
                NettyChannel.removeChannelIfDisconnected(ch);
            }
        }
    }

    /**
     * 解码器
     */
    private class InternalDecoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf input, List<Object> out) throws Exception {

            //buf包装为ChannelBuffer
            ChannelBuffer message = new NettyBackedChannelBuffer(input);

            //获得一个channel
            NettyChannel channel = NettyChannel.getOrAddChannel(ctx.channel(), url, handler);

            try {
                // decode object.
                do {
                    //读流量
                    int saveReaderIndex = message.readerIndex();
                    //解析
                    Object msg = codec.decode(channel, message);
                    if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
                        message.readerIndex(saveReaderIndex);//还要读更多
                        break;//退掉
                    } else {
                        //is it possible to go here ?
                        if (saveReaderIndex == message.readerIndex()) { //没用数据
                            throw new IOException("Decode without read data.");
                        }
                        if (msg != null) {//添加到输出的对象
                            out.add(msg);
                        }
                    }
                } while (message.readable()); //不断的读
            } finally {
                //异常，推出连接后，删除对应的关系
                NettyChannel.removeChannelIfDisconnected(ctx.channel());
            }
        }
    }
}
