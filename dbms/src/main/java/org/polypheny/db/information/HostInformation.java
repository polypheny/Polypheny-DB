/*
 * Copyright 2019-2020 The Polypheny Project
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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.SystemUtils;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
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

        InformationPage page = new InformationPage( "Host" );
        im.addPage( page );

        //
        // Operating System
        InformationGroup osGroup = new InformationGroup( page, "Operating System" ).setOrder( 1 );
        im.addGroup( osGroup );

        InformationTable osInformation = new InformationTable( osGroup, Arrays.asList( "Attribute", "Value" ) );
        osInformation.addRow( "Family", os.getFamily() );
        osInformation.addRow( "Manufacturer", os.getManufacturer() );
        osInformation.addRow( "Version", os.getVersionInfo().toString() );
        im.registerInformation( osInformation );

        //
        // Hardware
        InformationGroup hardwareGroup = new InformationGroup( page, "Hardware" ).setOrder( 2 );
        im.addGroup( hardwareGroup );

        InformationTable hardwareInformation = new InformationTable( hardwareGroup, Arrays.asList( "Attribute", "Value" ) );
        hardwareInformation.addRow( "CPU / Core Count", hal.getProcessor().getPhysicalPackageCount() + " / " + hal.getProcessor().getPhysicalProcessorCount() );
        if ( hal.getProcessor().getPhysicalPackageCount() > 1 ) {
            for ( int i = 0; i < hal.getProcessor().getPhysicalPackageCount(); i++ ) {
                hardwareInformation.addRow( "CPU " + i, hal.getProcessor().getProcessorIdentifier().getName() );
            }
        } else {
            hardwareInformation.addRow( "CPU", hal.getProcessor().getProcessorIdentifier().getName() );
        }
        hardwareInformation.addRow( "Memory", humanReadableByteCount( hal.getMemory().getTotal(), false ) );
        hardwareInformation.addRow( "Mainboard", hal.getComputerSystem().getBaseboard().getModel() );
        hardwareInformation.addRow( "Firmware Version", hal.getComputerSystem().getFirmware().getVersion() );
        hardwareInformation.addRow( "Manufacturer", hal.getComputerSystem().getManufacturer() );
        hardwareInformation.addRow( "Model", hal.getComputerSystem().getModel() );
        hardwareInformation.addRow( "Serial Number", hal.getComputerSystem().getSerialNumber() );
        im.registerInformation( hardwareInformation );

        //
        // Disks
        InformationGroup storageGroup = new InformationGroup( page, "Storage" ).setOrder( 4 );
        im.addGroup( storageGroup );

        InformationTable storageInformation = new InformationTable( storageGroup, Arrays.asList( "Name", "Model", "Size" ) );
        for ( HWDiskStore diskStore : hal.getDiskStores() ) {
            storageInformation.addRow( diskStore.getName(), diskStore.getModel(), humanReadableByteCount( diskStore.getSize(), false ) );
        }
        im.registerInformation( storageInformation );

        //
        // Network
        InformationGroup networkGroup = new InformationGroup( page, "Network Interfaces" ).setOrder( 6 );
        im.addGroup( networkGroup );

        InformationTable networkInformation = new InformationTable( networkGroup, Arrays.asList( "Name", "IPv4" ) );
        for ( NetworkIF networkIF : hal.getNetworkIFs() ) {
            networkInformation.addRow( networkIF.getDisplayName(), String.join( ".", networkIF.getIPv4addr() ) );
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

        //Memory Information
        final int base;
        if ( SystemUtils.IS_OS_MAC ) {
            base = 1000;
        } else {
            base = 1024;
        }
        final double div = Math.pow( base, 3 );
        InformationGroup memoryGroup = new InformationGroup( page, "Memory" ).setOrder( 5 );
        im.addGroup( memoryGroup );
        GlobalMemory mem = hal.getMemory();
        final String[] labels = new String[]{ "used", "free" };
        InformationGraph memoryGraph = new InformationGraph( memoryGroup, GraphType.PIE, labels );
        memoryGroup.setRefreshFunction( () -> {
            memoryGraph.updateGraph( labels, new GraphData<>( "memory", new Double[]{ (mem.getTotal() - mem.getAvailable()) / div, mem.getAvailable() / div } ) );
        } );
        im.registerInformation( memoryGraph );

        //
        // Processes
        InformationGroup processesGroup = new InformationGroup( page.getId(), "Processes" ).setOrder( 3 );
        im.addGroup( processesGroup );

        InformationGraph graph = new InformationGraph( processesGroup, GraphType.POLARAREA, new String[]{} );
        im.registerInformation( graph );

        processesGroup.setRefreshFunction( () -> {
            List<OSProcess> procs = os.getProcesses( 5, ProcessSort.CPU );

            ArrayList<String> procNames = new ArrayList<>();
            ArrayList<Double> procPerc = new ArrayList<>();
            for ( int i = 0; i < procs.size() && i < 5; i++ ) {
                OSProcess proc = procs.get( i );
                double cpuPerc = 100d * (proc.getKernelTime() + proc.getUserTime()) / proc.getUpTime();
                String name = proc.getName();
                procNames.add( name );
                procPerc.add( Math.round( cpuPerc * 10.0 ) / 10.0 );
            }

            GraphData<Double> data2 = new GraphData<>( "processes", procPerc.toArray( new Double[0] ) );
            graph.updateGraph( procNames.toArray( new String[0] ), data2 );
        } );


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
