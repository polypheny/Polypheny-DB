/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.information;


import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph.GraphData;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph.GraphType;
import ch.unibas.dmi.dbis.polyphenydb.webui.InformationServer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import oshi.software.os.OperatingSystem.ProcessSort;


public class InformationServerTest {


    public static void main( String[] args ) {
        InformationServer s = new InformationServer( 8082 );
        demoData();
    }


    /**
     * Generate test data
     */
    private static void demoData() {
        InformationManager im = InformationManager.getInstance();

        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        OperatingSystem os = si.getOperatingSystem();

        InformationPage p = new InformationPage( "p1", "System", "Here you can find some information about this computer, as well as randomly generated data." );
        im.addPage( p );

        InformationGroup g1 = new InformationGroup( "System", "p1" ).setOrder( 1 );
        im.addGroup( g1 );

        String family = os.getFamily();
        String manufacturer = os.getManufacturer();
        String version = os.getVersion().toString();
        Information i1 = new InformationHtml( "os", "System", "<ul>"
                + "<li>family: " + family + "</li>"
                + "<li>manufacturer: " + manufacturer + "</li>"
                + "<li>version: " + version + "</li>"
                + "</ul>" );
        im.registerInformation( i1 );

        InformationGroup g2 = new InformationGroup( "cpu", "p1" ).setOrder( 2 );
        im.addGroup( g2 );

        int cpuLoad = (int) Math.round( hal.getProcessor().getSystemCpuLoad() * 100 );
        InformationProgress i2 = new InformationProgress( "mem", "cpu", "cpu load", cpuLoad );
        im.registerInformation( i2 );

        Timer t1 = new Timer();
        t1.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                int cpuLoad = (int) Math.round( hal.getProcessor().getSystemCpuLoad() * 100 );
                i2.updateProgress( cpuLoad );
            }
        }, 5000, 5000 );

        InformationGroup g3 = new InformationGroup( "processes", "p1" );
        im.addGroup( g3 );

        List<OSProcess> procs = Arrays.asList( os.getProcesses( 5, ProcessSort.CPU ) );

        ArrayList<String> procNames = new ArrayList<>();
        ArrayList<Double> procPerc = new ArrayList<>();
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
        InformationGraph graph = new InformationGraph( "proc-graph", "processes", GraphType.POLARAREA, procNames.toArray( new String[0] ), data );
        im.registerInformation( graph );

        Timer t2 = new Timer();
        t2.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                List<OSProcess> procs = Arrays.asList( os.getProcesses( 5, ProcessSort.CPU ) );

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

                GraphData<Double> data2 = new GraphData<Double>( "processes", procPerc.toArray( new Double[0] ) );
                graph.updateGraph( procNames.toArray( new String[0] ), data2 );
            }
        }, 5000, 5000 );

        //random data
        Integer[] randomData1 = new Integer[10];
        Integer[] randomData2 = new Integer[10];
        Integer[] randomData3 = new Integer[10];
        Random r = new Random();
        for ( int i = 0; i < 10; i++ ) {
            randomData1[i] = r.nextInt( 100 );
            randomData2[i] = r.nextInt( 100 );
            randomData3[i] = r.nextInt( 100 );
        }
        InformationGroup randomGroup = new InformationGroup( "random data", "p1" ).setOrder( 4 );
        im.addGroup( randomGroup );
        String[] randomLabels = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" };
        GraphData randomGraphData1 = new GraphData<Integer>( "x", randomData1 );
        GraphData randomGraphData2 = new GraphData<Integer>( "y", randomData2 );
        //GraphData randomGraphData3 = new GraphData<Integer>( "z", randomData3);
        InformationGraph randomGraph = new InformationGraph( "random.graph", "random data", GraphType.BAR, randomLabels, randomGraphData1, randomGraphData2 );
        im.registerInformation( randomGraph );

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

    }


}
