/*
 * Copyright 2019-2022 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.postgresql;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.polypheny.db.StatusService;

public class ServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead (ChannelHandlerContext ctx, Object msg) {
        //ECHO-protocol... write buffers it, the flushes
        //alt: ctx. writeAndFlush(msg);
        StatusService.printInfo(String.format("channel read reached..."));

        String in = (String) msg;

        ctx.write("AuthenticationOk");
        ctx.flush();

        /*

        ChannelFuture channelFuture = ctx.write("AuthenticationOk");
        //channelFuture = ctx.write("ReadyForQuery");
        channelFuture.addListener(ChannelFutureListener.CLOSE);

        //ctx.write(msg);
        //ctx.flush();

        ByteBuf in = (ByteBuf) msg;

         */

    }

    @Override
    public void exceptionCaught (ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}

