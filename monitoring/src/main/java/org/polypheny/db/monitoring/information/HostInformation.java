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

package org.polypheny.db.monitoring.information;


import java.net.Inet6Address;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.util.Pair;
import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import oshi.software.os.OperatingSystem.ProcessSorting;


@Getter
public class HostInformation {


    private final SystemInfo systemInfo;
    private final HardwareAbstractionLayer hardwareAbstractionLayer;
    private final Supplier<Pair<Long, Long>> usedFreeProvider;

    @Getter(lazy = true)
    private final static HostInformation INSTANCE = new HostInformation();


    /**
     * Generate test data
     */
    private HostInformation() {
        InformationManager im = InformationManager.getInstance();

        this.systemInfo = new SystemInfo();
        this.hardwareAbstractionLayer = systemInfo.getHardware();
        OperatingSystem os = systemInfo.getOperatingSystem();

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
        hardwareInformation.addRow( "CPU / Core Count", hardwareAbstractionLayer.getProcessor().getPhysicalPackageCount() + " / " + hardwareAbstractionLayer.getProcessor().getPhysicalProcessorCount() );
        if ( hardwareAbstractionLayer.getProcessor().getPhysicalPackageCount() > 1 ) {
            for ( int i = 0; i < hardwareAbstractionLayer.getProcessor().getPhysicalPackageCount(); i++ ) {
                hardwareInformation.addRow( "CPU " + i, hardwareAbstractionLayer.getProcessor().getProcessorIdentifier().getName() );
            }
        } else {
            hardwareInformation.addRow( "CPU", hardwareAbstractionLayer.getProcessor().getProcessorIdentifier().getName() );
        }
        hardwareInformation.addRow( "Memory", humanReadableByteCount( hardwareAbstractionLayer.getMemory().getTotal(), false ) );
        hardwareInformation.addRow( "Mainboard", hardwareAbstractionLayer.getComputerSystem().getBaseboard().getModel() );
        hardwareInformation.addRow( "Firmware Version", hardwareAbstractionLayer.getComputerSystem().getFirmware().getVersion() );
        hardwareInformation.addRow( "Manufacturer", hardwareAbstractionLayer.getComputerSystem().getManufacturer() );
        hardwareInformation.addRow( "Model", hardwareAbstractionLayer.getComputerSystem().getModel() );
        hardwareInformation.addRow( "Serial Number", hardwareAbstractionLayer.getComputerSystem().getSerialNumber() );
        im.registerInformation( hardwareInformation );

        //
        // Disks
        InformationGroup storageGroup = new InformationGroup( page, "Storage" ).setOrder( 4 );
        im.addGroup( storageGroup );

        InformationTable storageInformation = new InformationTable( storageGroup, Arrays.asList( "Name", "Model", "Size" ) );
        try {
            for ( HWDiskStore diskStore : hardwareAbstractionLayer.getDiskStores() ) {
                storageInformation.addRow( diskStore.getName(), diskStore.getModel(), humanReadableByteCount( diskStore.getSize(), false ) );
            }
            im.registerInformation( storageInformation );
        } catch ( UnsatisfiedLinkError ignore ) {
            // This happens on Linux systems without udev library
        }

        //
        // Network
        InformationGroup networkGroup = new InformationGroup( page, "Network Interfaces" ).setOrder( 6 );
        im.addGroup( networkGroup );

        InformationTable networkInformation2 = new InformationTable( networkGroup, Arrays.asList( "Name", "IPv4" ) );

        try {
            NetworkInterface.networkInterfaces()
                    .forEach( networkInterface ->
                            networkInformation2.addRow(
                                    networkInterface.getDisplayName(),
                                    networkInterface.getInterfaceAddresses().stream()
                                            .filter( i -> !i.getAddress().isLinkLocalAddress() && !(i.getAddress() instanceof Inet6Address) )
                                            .map( i -> i.getAddress().getHostAddress() )
                                            .collect( Collectors.joining( ", " ) )
                            )
                    );
        } catch ( SocketException ignore ) {
        }
        im.registerInformation( networkInformation2 );

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
        final double div = Math.pow( 1024, 3 );
        InformationGroup memoryGroup = new InformationGroup( page, "Memory in GiB" ).setOrder( 5 );
        im.addGroup( memoryGroup );
        GlobalMemory mem = hardwareAbstractionLayer.getMemory();
        final String[] labels = new String[]{ "used", "free" };
        InformationGraph memoryGraph = new InformationGraph( memoryGroup, GraphType.PIE, labels );
        this.usedFreeProvider = () -> Pair.of( mem.getTotal() - mem.getAvailable(), mem.getAvailable() );
        memoryGroup.setRefreshFunction( () ->
                memoryGraph.updateGraph( labels, new GraphData<>( "memory", new Double[]{ (mem.getTotal() - mem.getAvailable()) / div, mem.getAvailable() / div } ) )
        );
        im.registerInformation( memoryGraph );

        //
        // Processes
        InformationGroup processesGroup = new InformationGroup( page.getId(), "Processes" ).setOrder( 3 );
        im.addGroup( processesGroup );

        InformationGraph graph = new InformationGraph( processesGroup, GraphType.POLARAREA, new String[]{} );
        im.registerInformation( graph );

        processesGroup.setRefreshFunction( () -> {
            List<OSProcess> procs = os.getProcesses( null, ProcessSorting.CPU_DESC, 5 );

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
