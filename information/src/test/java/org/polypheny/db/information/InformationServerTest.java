/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.information;


import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.webui.InformationService;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;


@Slf4j
public class InformationServerTest {


    public static void main( String[] args ) {
        InformationService s = new InformationService( null );
        demoData();
    }


    /**
     * Generate test data
     */
    private static void demoData() {
        InformationManager im = InformationManager.getInstance();

        //SYSTEM GROUP

        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        OperatingSystem os = si.getOperatingSystem();

        InformationPage systemPage = new InformationPage( "p1", "System", "Here you can find some information about this computer, as well as randomly generated data." );
        im.addPage( systemPage );
        systemPage.setRefreshFunction( () -> {
            log.debug( "refreshing page" );
        } );

        InformationGroup systemGroup = new InformationGroup( systemPage, "System" ).setOrder( 1 );
        im.addGroup( systemGroup );

        String family = os.getFamily();
        String manufacturer = os.getManufacturer();
        String version = os.getVersionInfo().getVersion();
        Information i1 = new InformationHtml( "os", systemGroup.getId(), "<ul>"
                + "<li>family: " + family + "</li>"
                + "<li>manufacturer: " + manufacturer + "</li>"
                + "<li>version: " + version + "</li>"
                + "</ul>" ).setOrder( 1 );
        im.registerInformation( i1 );

        Information iAction = new InformationAction( systemGroup, "executeAction", ( params ) -> {
            String out = "Executed action with params: ";
            if ( params != null ) {
                for ( Map.Entry<String, String> entry : params.entrySet() ) {
                    out += String.format( "%s: %s; ", entry.getKey(), entry.getValue() );
                }
            }
            return out;
        } ).withParameters( "p1", "p2", "p3" ).setOrder( 2 );
        im.registerInformation( iAction );

        //CPU GROUP

        /*InformationGroup cpuGroup = new InformationGroup( systemPage, "cpu" ).setOrder( 2 );
        im.addGroup( cpuGroup );

        int cpuLoad = (int) Math.round( hal.getProcessor().getSystemCpuLoad() * 100 );
        InformationProgress i2 = new InformationProgress( "mem", cpuGroup.getId(), "cpu load", cpuLoad );
        im.registerInformation( i2 );

        Timer t1 = new Timer();
        t1.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                int cpuLoad = (int) Math.round( hal.getProcessor().getSystemCpuLoad() * 100 );
                i2.updateProgress( cpuLoad );
            }
        }, 5000, 5000 );*/

        //PROCESSES GROUP

        InformationGroup processesGroup = new InformationGroup( systemPage.getId(), "processes" );
        im.addGroup( processesGroup );

        List<OSProcess> procs = os.getProcesses();

        List<String> procNames = new ArrayList<>();
        List<Double> procPerc = new ArrayList<>();
        for ( int i = 0; i < procs.size() && i < 5; i++ ) {
            OSProcess proc = procs.get( i );
            double cpuPerc = 100d * (proc.getKernelTime() + proc.getUserTime()) / proc.getUpTime();
            String name = proc.getName();
            //if( cpuPerc > 1){
            procNames.add( name );
            procPerc.add( cpuPerc );
            //}else{
            //    break;
            //}
        }

        GraphData<Double> data = new GraphData<Double>( "processes", procPerc.toArray( new Double[0] ) );
        InformationGraph graph = new InformationGraph( "proc-graph", processesGroup.getId(), GraphType.POLARAREA, procNames.toArray( new String[0] ), data );
        im.registerInformation( graph );

        Timer t2 = new Timer();
        t2.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                List<OSProcess> procs = os.getProcesses();

                ArrayList<String> procNames = new ArrayList<>();
                ArrayList<Double> procPerc = new ArrayList<>();
                for ( int i = 0; i < procs.size() && i < 5; i++ ) {
                    OSProcess proc = procs.get( i );
                    double cpuPerc = 100d * (proc.getKernelTime() + proc.getUserTime()) / proc.getUpTime();
                    String name = proc.getName();
                    //if( cpuPerc > 1){
                    procNames.add( name );
                    procPerc.add( Math.round( cpuPerc * 10.0 ) / 10.0 );
                    //}else{
                    //    break;
                    //}
                }

                GraphData<Double> data2 = new GraphData<>( "processes", procPerc.toArray( new Double[0] ) );
                graph.updateGraph( procNames.toArray( new String[0] ), data2 );
            }
        }, 5000, 5000 );

        //RANDOM DATA GROUP

        Integer[] randomData1 = new Integer[10];
        Integer[] randomData2 = new Integer[10];
        Integer[] randomData3 = new Integer[10];
        Random r = new Random();
        for ( int i = 0; i < 10; i++ ) {
            randomData1[i] = r.nextInt( 100 );
            randomData2[i] = r.nextInt( 100 );
            randomData3[i] = r.nextInt( 100 );
        }
        InformationGroup randomGroup = new InformationGroup( systemPage, "random data" ).setOrder( 4 );
        im.addGroup( randomGroup );
        String[] randomLabels = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" };
        GraphData randomGraphData1 = new GraphData<Integer>( "x", randomData1 );
        GraphData randomGraphData2 = new GraphData<Integer>( "y", randomData2 );
        //GraphData randomGraphData3 = new GraphData<Integer>( "z", randomData3);
        InformationGraph randomGraph = new InformationGraph( "random.graph", randomGroup.getId(), GraphType.BAR, randomLabels, randomGraphData1, randomGraphData2 );
        im.registerInformation( randomGraph );

        //COLLECTING DATA GROUP

        InformationGroup collectingGroup = new InformationGroup( systemPage, "collecting data" ).setOrder( 5 );
        collectingGroup.setRefreshFunction( () -> {
            im.getInformation( "collectingGraph" ).unwrap( InformationGraph.class ).addData( "dynamic", (int) (Math.random() * 10) );
        } );
        im.addGroup( collectingGroup );
        GraphData<Integer> dynamicData = new GraphData<>( "dynamic", new Integer[]{ 1, 2, 3 }, 10 );
        GraphData<Integer> staticData = new GraphData<>( "static", new Integer[]{ 2, 4, 6, 8, 10, 9, 7, 5, 3, 1 } );
        InformationGraph collectingGraph = new InformationGraph( "collectingGraph", collectingGroup.getId(), GraphType.BAR, null, new GraphData[]{ dynamicData, staticData } ).maxY( 10 );
        im.registerInformation( collectingGraph );
        Timer collectingTimer = new Timer();
        collectingTimer.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                collectingGraph.addData( "dynamic", (int) (Math.random() * 10) );
            }
        }, 5000, 5000 );

        Timer t3 = new Timer();
        t3.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                Integer[] randomData1 = new Integer[10];
                Integer[] randomData2 = new Integer[10];
                //Integer[] randomData3 = new Integer[10];
                Random r = new Random();
                for ( int i = 0; i < 10; i++ ) {
                    randomData1[i] = r.nextInt( 100 );
                    randomData2[i] = r.nextInt( 100 );
                    //randomData3[i] = r.nextInt( 100 );
                }
                GraphData randomGraphData1 = new GraphData<Integer>( "x", randomData1 );
                GraphData randomGraphData2 = new GraphData<Integer>( "y", randomData2 );
                //GraphData randomGraphData3 = new GraphData<Integer>( "z", randomData3);
                randomGraph.updateGraph( randomLabels, randomGraphData1, randomGraphData2 );
            }
        }, 10000, 10000 );

        //DURATION GROUP

        InformationGroup durationGroup = new InformationGroup( systemPage, "durations" );
        im.addGroup( durationGroup );
        InformationDuration duration1 = new InformationDuration( durationGroup );
        duration1.start( "Task1" ).setLimit( 50 );
        sleep( 100 );
        duration1.stop( "Task1" );
        duration1.addNanoDuration( "TaskInBetween", 30_000_000L );
        duration1.addMilliDuration( "TaskInBetween", 30L );
        duration1.start( "Task2" ).setLimit( 1000 ).noProgressBar();
        duration1.get( "Task2" ).start( "sub1" );
        sleep( 150 );
        duration1.get( "Task2" ).stop( "sub1" );
        duration1.get( "Task2" ).start( "sub2" );
        sleep( 50 );
        duration1.get( "Task2" ).stop( "sub2" );
        duration1.stop( "Task2" );
        duration1.start( "Task3" );
        sleep( 50 );
        duration1.stop( "Task3" );
        duration1.setOrder( 1 );
        im.registerInformation( duration1 );

        InformationDuration duration2 = new InformationDuration( durationGroup );
        duration2.start( "group1" );
        duration2.get( "group1" ).start( "sub1" );
        sleep( 100 );
        duration2.get( "group1" ).get( "sub1" ).start( "subSub" ).setLimit( 50 );
        sleep( 100 );
        duration2.get( "group1" ).get( "sub1" ).stop( "subSub" );
        duration2.get( "group1" ).get( "sub1" ).get( "subSub" ).start( "s4" );
        duration2.get( "group1" ).get( "sub1" ).start( "subSub2" );
        sleep( 100 );
        duration2.get( "group1" ).get( "sub1" ).stop( "subSub2" );
        duration2.get( "group1" ).stop( "sub1" );
        duration2.stop( "group1" );
        duration2.setOrder( 2 );
        im.registerInformation( duration2 );

        InformationDuration duration3 = new InformationDuration( durationGroup );
        duration3.start( "test" );
        duration3.stop( "test" );
        duration3.setOrder( 3 );
        im.registerInformation( duration3 );

        //InformationKeyValue
        InformationGroup kvGroup = new InformationGroup( systemPage, "InformationKeyValue" );
        im.addGroup( kvGroup );
        InformationKeyValue iKV = new InformationKeyValue( "kv", kvGroup );
        iKV.putPair( "k1", "value1" ).putPair( "k2", "value2" ).putPair( "k3", "value3" );
        assertEquals( iKV.getValue( "k1" ), "value1" );
        im.registerInformation( iKV );
        Timer t4 = new Timer();
        final boolean[] alternate = { true };
        t4.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                if ( alternate[0] ) {
                    iKV.removePair( "k2" );
                } else {
                    iKV.putPair( "k2", "value2" );
                }
                alternate[0] = !alternate[0];
            }
        }, 5000, 5000 );

        InformationGroup textGroup = new InformationGroup( systemPage, "InformationText" );
        im.addGroup( textGroup );
        InformationText text1 = new InformationText( "text1", textGroup, "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas molestie vestibulum est vitae consequat. Vivamus mi dolor, faucibus nec urna sit amet, tempor varius est." );
        im.registerInformation( text1 );
        InformationText text2 = new InformationText( "text2", textGroup );
        im.registerInformation( text2 );
        text2.setText( "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas molestie vestibulum est vitae consequat. Vivamus mi dolor, faucibus nec urna sit amet, tempor varius est. " );
    }


    /**
     * Sleep method for testing
     */
    private static void sleep( long sleep ) {
        try {
            Thread.sleep( sleep );
        } catch ( InterruptedException e ) {
            e.printStackTrace();
        }
    }


}
