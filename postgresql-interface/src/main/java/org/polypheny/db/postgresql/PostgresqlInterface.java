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


import com.google.common.collect.ImmutableList;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.StatusService;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Util;

import java.nio.ByteOrder;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;



@Slf4j
public class PostgresqlInterface extends QueryInterface {

    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_NAME = "Postgresql Interface";
    @SuppressWarnings("WeakerAccess")
    // TODO: Update description text
    public static final String INTERFACE_DESCRIPTION = "PostgreSQL-based query interface - in development";
    @SuppressWarnings("WeakerAccess")
    public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new QueryInterfaceSettingInteger( "port", false, true, false, 5432 )
            // new QueryInterfaceSettingInteger( "maxUploadSizeMb", false, true, true, 10000 ),
            // new QueryInterfaceSettingList( "serialization", false, true, false, ImmutableList.of( "PROTOBUF", "JSON" ) )
            // Possible to add more myself
    );


    private final int port;
    private final String uniqueName;

    // Counters
    private final Map<QueryLanguage, AtomicLong> statementCounters = new HashMap<>();

    private final MonitoringPage monitoringPage;

    // Server things
    private final EventLoopGroup bossGroup = new NioEventLoopGroup();
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();



    public PostgresqlInterface(TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
        super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, true );
        this.uniqueName = uniqueName;
        this.port = Integer.parseInt( settings.get( "port" ) );
        if ( !Util.checkIfPortIsAvailable( port ) ) {
            // Port is already in use
            throw new RuntimeException( "Unable to start " + INTERFACE_NAME + " on port " + port + "! The port is already in use." );
        }
        // Add information page
        monitoringPage = new MonitoringPage();
    }


    @Override
    public void run() {
        //ToDo: Instantiate Server (open port...)

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel socketChannel) throws Exception {
                            ChannelPipeline channelPipeline = socketChannel.pipeline();

                            //Inbound
                            //channelPipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(ByteOrder.LITTLE_ENDIAN, Integer.MAX_VALUE, 1, 4, -4, 0, true));
                            //channelPipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0));
                            //channelPipeline.addLast("DecoderText", new Decoder(Integer.MAX_VALUE, 1, 4, -4, 0));
                            //channelPipeline.addLast("headerdecoder", new StringDecoder());
                            channelPipeline.addLast("decoder", new StringDecoder());


                            //Outbound
                            channelPipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                            channelPipeline.addLast("encoder", new StringEncoder());

                            //Handler
                            channelPipeline.addLast("handler", new ServerHandler());

                        }
                    }).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);

            // Start accepting incoming connections
            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();

            // Waits until server socket is closed --> introduces bugs --> polypheny not starting (without reset) and not displaying interface correctly
            //channelFuture.channel().closeFuture().sync();


        } catch (Exception e) {
            log.error("Exception while starting" + INTERFACE_NAME, e);

        }


        StatusService.printInfo(String.format("%s started and is listening on port %d.", INTERFACE_NAME, port ));
    }



    @Override
    public List<QueryInterfaceSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }


    @Override
    public void shutdown() {
        //todo: end things from run()
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        monitoringPage.remove();
    }


    @Override
    public String getInterfaceType() {
        return INTERFACE_NAME;
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        //Todo: if settings are mutable, change it here (can make them mutable)
        //nothing in avatica/http interface
    }


    private class MonitoringPage {
        //TodO: vergliiche met anderne interfaces (zeigt infos em ui aah)

        private final InformationPage informationPage;
        private final InformationGroup informationGroupRequests;
        private final InformationTable statementsTable;


        public MonitoringPage() {
            InformationManager im = InformationManager.getInstance();

            informationPage = new InformationPage( uniqueName, INTERFACE_NAME ).fullWidth().setLabel( "Interfaces" );
            informationGroupRequests = new InformationGroup( informationPage, "Requests" );

            im.addPage( informationPage );
            im.addGroup( informationGroupRequests );

            statementsTable = new InformationTable(
                    informationGroupRequests,
                    Arrays.asList( "Language", "Percent", "Absolute" )
            );
            statementsTable.setOrder( 2 );
            im.registerInformation( statementsTable );

            informationGroupRequests.setRefreshFunction( this::update );
        }


        //reload button
        public void update() {
            double total = 0;
            for ( AtomicLong counter : statementCounters.values() ) {
                total += counter.get();
            }

            DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
            symbols.setDecimalSeparator( '.' );
            DecimalFormat df = new DecimalFormat( "0.0", symbols );
            statementsTable.reset();
            for ( Map.Entry<QueryLanguage, AtomicLong> entry : statementCounters.entrySet() ) {
                statementsTable.addRow( entry.getKey().name(), df.format( total == 0 ? 0 : (entry.getValue().longValue() / total) * 100 ) + " %", entry.getValue().longValue() );
            }
        }


        public void remove() {
            InformationManager im = InformationManager.getInstance();
            im.removeInformation( statementsTable );
            im.removeGroup( informationGroupRequests );
            im.removePage( informationPage );
        }

    }

}
