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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.polypheny.db.StatusService;

import java.lang.management.ManagementFactory;
import java.net.DatagramPacket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public class PGInterfaceServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead (ChannelHandlerContext ctx, Object msg) {
        //ECHO-protocol... write buffers it, the flushes
        //alt: ctx. writeAndFlush(msg);
        StatusService.printInfo(String.format("channel read reached..."));

        String in = (String) msg;

        //----------------------------------------- alti version --------------------------------------------
        //char header = 'R';
        String header ="R";
        ByteBuf headerBuf = Unpooled.copiedBuffer(header.getBytes(StandardCharsets.UTF_8),0, 1);
        //headerBuf.setByte('R', )

        String message = "0";
        //int message = 0;
        ByteBuf msgBuf = Unpooled.copiedBuffer(message.getBytes(StandardCharsets.UTF_8));

        int size = 8; //+ msgBuf.readableBytes();
        //capacity: Returns the number of bytes (octets) this buffer can contain.
        ByteBuf sizeBuf = Unpooled.copyInt(size);

        //---------------------------------------- diräkt of de buffer schriibe----------------------------------

        //authenticationOk
        ByteBuf buffer = ctx.alloc().buffer(9);
        buffer.writeByte('R');
        buffer.writeInt(8); // size excluding char
        buffer.writeInt(0);
        //StatusService.printInfo(String.format("channel read reached..."));
        ctx.writeAndFlush(buffer);

        //int x = DatagramPacket.sender;



        /*
        String paramu = "client_encoding";
        String paramValu = "UTF8";
        ByteBuf buffer3u = ctx.alloc().buffer(4+paramu.length() + 1 + paramValu.length() + 2);
        buffer3u.writeByte('S');
        buffer3u.writeInt(4+paramu.length() + 1 + paramValu.length() + 1); // size excluding char
        buffer3u.writeBytes(paramu.getBytes(StandardCharsets.UTF_8));
        buffer3u.writeBytes(paramValu.getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(buffer3u);

         */


        /*
        ByteBuf buffer5 = ctx.alloc().buffer(50);
        buffer5.writeByte('K');
        buffer5.writeInt(12);
        String pidName = ManagementFactory.getRuntimeMXBean().getName();
        int pid = Integer.parseInt(pidName.split("@")[0]);
        int secretKey = ThreadLocalRandom.current().nextInt();
        buffer5.writeInt(pid);
        buffer5.writeInt(secretKey);
        ctx.writeAndFlush(buffer5);

         */


        String param = "server_version";    //.14.4 (Debian 14.4-1.pgdg110+1)
        //String paramVal = "13.8"; //PG_SERVER_VERSION
        String paramVal = "14"; //PG_SERVER_VERSION
        //byte[] paramb = param.getBytes(StandardCharsets.UTF_8);
        //byte[] paramValb = paramVal.getBytes(StandardCharsets.UTF_8);
        //ByteBuf buffer3 = ctx.alloc().buffer(4+param.length() + 1 + paramVal.length() + 2);
        ByteBuf buffer3 = ctx.alloc().buffer(100);
        buffer3.writeByte('S');
        //int x = 4+paramb.length + 1 + paramValb.length + 2;
        //int y = 4+param.length() + 1 + paramVal.length() + 2;
        buffer3.writeInt(4+param.length() + 1 + paramVal.length() + 1); // size excluding char
        //buffer3.writeInt(4+param.length() + 1 + paramVal.length() + 2); // size excluding char
        buffer3.writeBytes(param.getBytes(StandardCharsets.US_ASCII));
        buffer3.writeByte(0);
        buffer3.writeBytes(paramVal.getBytes(StandardCharsets.US_ASCII));
        buffer3.writeByte(0);
        //buffer3.writeBytes(param.getBytes(StandardCharsets.UTF_8));
        //buffer3.writeByte(0);
        //buffer3.writeBytes(paramVal.getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(buffer3);





        /*
        //gibt protookollfehler wenn an dieser stelle gesendet... - no data indicator
        ByteBuf buffer4 = ctx.alloc().buffer(5);
        buffer4.writeByte('n');
        buffer4.writeInt(4);
        ctx.writeAndFlush(buffer4);

         */


        ByteBuf buffer2 = ctx.alloc().buffer(6);
        buffer2.writeByte('Z');
        buffer2.writeInt(5); // size excluding char
        buffer2.writeByte('I');
        //StatusService.printInfo(String.format("channel read reached..."));
        ctx.writeAndFlush(buffer2);



        //ctx.flush();



        /*
        ctx.write(headerBuf);
        ctx.write(sizeBuf);
        ctx.write(msgBuf);
        ctx.flush();

         */

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

