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


import java.util.Arrays;
import java.util.List;
import org.polypheny.db.information.GraphColor;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;


public class JavaInformation {


    public JavaInformation() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage( "Java Runtime" );
        im.addPage( page );

        // JVM Info
        InformationGroup groupJvm = new InformationGroup( page, "JVM Detail" ).setOrder( 1 );
        im.addGroup( groupJvm );

        InformationTable javaInfoJvm = new InformationTable(
                groupJvm,
                Arrays.asList( "Attribute", "Value" ) );
        im.registerInformation( javaInfoJvm );
        javaInfoJvm.addRow( "JVM Name", System.getProperty( "java.vm.name" ) );
        javaInfoJvm.addRow( "Runtime Version", System.getProperty( "java.runtime.version" ) );

        // Heap info
        InformationGroup groupHeap = new InformationGroup( page, "Java Heap Size" ).setOrder( 2 );
        im.addGroup( groupHeap );

        InformationGraph heapInfoGraph = new InformationGraph(
                groupHeap,
                GraphType.DOUGHNUT,
                new String[]{ "Current", "Maximum", "Free" }
        );
        heapInfoGraph.colors( List.of( GraphColor.PASTEL_RED, GraphColor.BATTERY_CHARGED_BLUE, GraphColor.LIME ) );
        im.registerInformation( heapInfoGraph );

        InformationTable heapInfoTable = new InformationTable(
                groupHeap,
                Arrays.asList( "Attribute", "Value" ) );
        im.registerInformation( heapInfoTable );

        groupHeap.setRefreshFunction( () -> {
            // from: https://stackoverflow.com/questions/2015463/how-to-view-the-current-heap-size-that-an-application-is-using
            // Get current size of heap in bytes
            long current = Runtime.getRuntime().totalMemory();
            // Get maximum size of heap in bytes. The heap cannot grow beyond this size. Any attempt will result in an OutOfMemoryException.
            long max = Runtime.getRuntime().maxMemory();
            // Get amount of free memory within the heap in bytes. This size will increase after garbage collection and decrease as new objects are created.
            long free = Runtime.getRuntime().freeMemory();

            long available = max - current;
            heapInfoGraph.updateGraph(
                    new String[]{ "Current", "Free", "Available" },
                    new GraphData<>( "heap-data", new Long[]{ current - free, free, available } )
            );

            heapInfoTable.reset();
            heapInfoTable.addRow( "Current", humanReadableByteCount( current - free, false ) );
            heapInfoTable.addRow( "Free", humanReadableByteCount( free, false ) );
            heapInfoTable.addRow( "Max", humanReadableByteCount( max, false ) );
        } );

        // Heap over time info
        InformationGroup groupHeapOverTime = new InformationGroup( page, "Heap over time" ).setOrder( 3 );
        im.addGroup( groupHeapOverTime );

        InformationGraph heapOverTimeGraph = new InformationGraph(
                groupHeapOverTime,
                GraphType.LINE,
                null,
                new GraphData<>( "Total in MB", new Long[]{ Runtime.getRuntime().totalMemory() / 1000000 }, 20 )
        );
        heapOverTimeGraph.minY( 1 );
        im.registerInformation( heapOverTimeGraph );

        BackgroundTaskManager.INSTANCE.registerTask(
                () -> {
                    long current = Runtime.getRuntime().totalMemory() / 1000000;
                    heapOverTimeGraph.addData( "Total in MB", current );
                },
                "Update Java runtime information",
                TaskPriority.LOW,
                TaskSchedulingType.EVERY_MINUTE_FIXED
        );
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
