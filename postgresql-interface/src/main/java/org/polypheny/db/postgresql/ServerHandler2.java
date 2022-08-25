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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.polypheny.db.StatusService;

import java.nio.charset.StandardCharsets;

public class ServerHandler2 extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead (ChannelHandlerContext ctx, Object msg) {
        StatusService.printInfo(String.format("channel read reached...2"));

        String param = "client_encoding";
        String paramVal = "UTF8";
        ByteBuf buffer3 = ctx.alloc().buffer(4+param.length() + 1 + paramVal.length() + 2);
        buffer3.writeByte('S');
        buffer3.writeInt(4+param.length() + 1 + paramVal.length() + 1); // size excluding char
        buffer3.writeBytes(param.getBytes(StandardCharsets.UTF_8));
        buffer3.writeBytes(paramVal.getBytes(StandardCharsets.UTF_8));
        //StatusService.printInfo(String.format("channel read reached..."));
        ctx.writeAndFlush(buffer3);
    }

}
