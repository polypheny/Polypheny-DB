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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.polypheny.db.StatusService;
import org.polypheny.db.transaction.TransactionManager;

/**
 * Forwards the message from the "netty flow" to the internal structure
 */
public class PGInterfaceServerHandler extends ChannelInboundHandlerAdapter {
    TransactionManager transactionManager;

    public PGInterfaceServerHandler(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    public void channelRead (ChannelHandlerContext ctx, Object msg) {
        //StatusService.printInfo(String.format("channel read reached..."));

        PGInterfaceInboundCommunicationHandler interfaceInboundCommunicationHandler = new PGInterfaceInboundCommunicationHandler("", ctx, transactionManager);
        interfaceInboundCommunicationHandler.decideCycle(msg);

        /*
        //authenticationOk
        PGInterfaceMessage mmsg = new PGInterfaceMessage(PGInterfaceHeaders.R, "0", 8, false);
        PGInterfaceServerWriter writer = new PGInterfaceServerWriter("i", mmsg, ctx);
        ctx.writeAndFlush(writer.writeOnByteBuf());


        //server_version (Parameter Status message)
        PGInterfaceMessage svmsg = new PGInterfaceMessage(PGInterfaceHeaders.S, "server_version§14", 4, true);
        PGInterfaceServerWriter svwriter = new PGInterfaceServerWriter("ss", svmsg, ctx);
        ctx.writeAndFlush(svwriter.writeOnByteBuf());
           r   user flufi database flufi client_encoding UTF8 DateStyle ISO TimeZone Europe/Berlin extra_float_digits 2  

        P   " SET extra_float_digits = 3   B           E   	    S   

        P   7 SET application_name = 'PostgreSQL JDBC Driver'   B           E   	    S   

        P   ) INSERT INTO lol(LolId) VALUES (3)    B               D      P   E    	     S     

        P    SELECT LolId FROM lol    B               D     P   E    	      S     
        P    SELECT LolId FROM lol   B           D   P E   	     S   


        //ReadyForQuery
        PGInterfaceMessage rqmsg = new PGInterfaceMessage(PGInterfaceHeaders.Z, "I", 5, false);
        PGInterfaceServerWriter rqwriter = new PGInterfaceServerWriter("c", rqmsg, ctx);
        ctx.writeAndFlush(rqwriter.writeOnByteBuf());

         */



        /*
        //ParameterStatus - client_encoding (ParameterStatus message)
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
        //BackendKeyData
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


    }

    @Override
    public void exceptionCaught (ChannelHandlerContext ctx, Throwable cause) {
        //cause.printStackTrace();
        //Todo: (evtl): error: Eine vorhandene Verbindung wurde vom Remotehost geschlossen --> if client closes connection "not properly"
        ctx.close();
    }
}

