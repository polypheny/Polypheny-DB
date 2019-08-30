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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import oshi.software.os.OperatingSystem.ProcessSort;


public class HostInformation {


    /**
     * Generate test data
     */
    public HostInformation() {
        InformationManager im = InformationManager.getInstance();

        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        OperatingSystem os = si.getOperatingSystem();

        InformationPage page = new InformationPage( "host", "Host" );
        im.addPage( page );

        //
        // Operating System
        InformationGroup osGroup = new InformationGroup( "Operating System", page.getId() ).setOrder( 1 );
        im.addGroup( osGroup );

        InformationTable osInformation = new InformationTable( "os", osGroup.getId(), Arrays.asList( "Attribute", "Value" ) );
        osInformation.addRow( "Family", os.getFamily() );
        osInformation.addRow( "Manufacturer", os.getManufacturer() );
        osInformation.addRow( "Version", os.getVersion().toString() );
        im.registerInformation( osInformation );

        //
        // Hardware
        InformationGroup hardwareGroup = new InformationGroup( "Hardware", page.getId() ).setOrder( 2 );
        im.addGroup( hardwareGroup );

        InformationTable hardwareInformation = new InformationTable( "hw", hardwareGroup.getId(), Arrays.asList( "Attribute", "Value" ) );
        hardwareInformation.addRow( "CPU Count", "" + hal.getProcessor().getPhysicalPackageCount() );
        if ( hal.getProcessor().getPhysicalPackageCount() > 1 ) {
            for ( int i = 0; i < hal.getProcessor().getPhysicalPackageCount(); i++ ) {
                hardwareInformation.addRow( "CPU " + i, hal.getProcessor().getName() );
            }
        } else {
            hardwareInformation.addRow( "CPU", hal.getProcessor().getName() );
        }
        hardwareInformation.addRow( "Mainboard", hal.getComputerSystem().getBaseboard().toString() );
        hardwareInformation.addRow( "Firmware Version", hal.getComputerSystem().getFirmware().getVersion() );
        hardwareInformation.addRow( "Manufacturer", hal.getComputerSystem().getManufacturer() );
        hardwareInformation.addRow( "Model", hal.getComputerSystem().getModel() );
        hardwareInformation.addRow( "Serial Number", hal.getComputerSystem().getSerialNumber() );
        im.registerInformation( hardwareInformation );

        //
        // Disks
        InformationGroup storageGroup = new InformationGroup( "Storage", page.getId() );
        im.addGroup( storageGroup );

        InformationTable storageInformation = new InformationTable( "disks", storageGroup.getId(), Arrays.asList( "Name", "Model", "Size" ) );
        for ( int i = 0; i < hal.getDiskStores().length; i++ ) {
            storageInformation.addRow( hal.getDiskStores()[i].getName(), hal.getDiskStores()[i].getModel(), humanReadableByteCount( hal.getDiskStores()[i].getSize(), false ) );
        }
        im.registerInformation( storageInformation );

        //
        // Network
        InformationGroup networkGroup = new InformationGroup( "Network Interfaces", page.getId() );
        im.addGroup( networkGroup );

        InformationTable networkInformation = new InformationTable( "nics", networkGroup.getId(), Arrays.asList( "Name", "IPv4" ) );
        for ( int i = 0; i < hal.getNetworkIFs().length; i++ ) {
            networkInformation.addRow( hal.getNetworkIFs()[i].getDisplayName(), String.join( ".", hal.getNetworkIFs()[i].getIPv4addr() ) );
        }
        im.registerInformation( networkInformation );


/*
        //
        // Load
        InformationGroup loadGroup = new InformationGroup( "Load", page.getId() ).setOrder( 3 );
        im.addGroup( loadGroup );

        int cpuLoad = (int) Math.round( hal.getProcessor().getSystemCpuLoad() * 100 );
        InformationProgress i2 = new InformationProgress( "mem", loadGroup.getId(), "sysload", cpuLoad );
        im.registerInformation( i2 );

        Timer t1 = new Timer();
        t1.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                int cpuLoad = (int) Math.round( hal.getProcessor().getSystemCpuLoad() * 100 );
                i2.updateProgress( cpuLoad );
            }
        }, 5000, 5000 );
*/

        // TODO: Memory

        //
        // Processes
        InformationGroup processesGroup = new InformationGroup( "Processes", page.getId() );
        im.addGroup( processesGroup );

        InformationGraph graph = new InformationGraph( "proc-graph", processesGroup.getId(), GraphType.POLARAREA, new String[]{} );
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
        }, 0, 5000 );


    }


    // taken from https://stackoverflow.com/a/3758880
    private static String humanReadableByteCount( long bytes, boolean si ) {
        int unit = si ? 1000 : 1024;
        if ( bytes < unit ) {
            return bytes + " B";
        }
        int exp = (int) (Math.log( bytes ) / Math.log( unit ));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt( exp - 1 ) + (si ? "" : "i");
        return String.format( "%.1f %sB", bytes / Math.pow( unit, exp ), pre );
    }


}
