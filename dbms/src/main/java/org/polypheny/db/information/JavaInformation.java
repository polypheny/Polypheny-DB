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


import java.util.Arrays;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.util.background.BackgroundTask;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;


public class JavaInformation {


    public JavaInformation() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage( "javaInfo", "Java Runtime" );
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
        im.registerInformation( heapInfoGraph );

        InformationTable heapInfoTable = new InformationTable(
                groupHeap,
                Arrays.asList( "Attribute", "Value" ) );
        im.registerInformation( heapInfoTable );

        JavaHeapInfo heapInfoRunnable = new JavaHeapInfo( heapInfoGraph, heapInfoTable );
        BackgroundTaskManager.INSTANCE.registerTask( heapInfoRunnable, "Update Java runtime information", TaskPriority.LOW, TaskSchedulingType.EVERY_MINUTE );
    }


    static class JavaHeapInfo implements BackgroundTask {

        private final InformationGraph graph;
        private final InformationTable table;


        JavaHeapInfo( InformationGraph graph, InformationTable table ) {
            this.graph = graph;
            this.table = table;
        }


        @Override
        public void backgroundTask() {
            // from: https://stackoverflow.com/questions/2015463/how-to-view-the-current-heap-size-that-an-application-is-using
            // Get current size of heap in bytes
            long current = Runtime.getRuntime().totalMemory();
            // Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
            long max = Runtime.getRuntime().maxMemory();
            // Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
            long free = Runtime.getRuntime().freeMemory();

            graph.updateGraph(
                    new String[]{ "Current", "Maximum", "Free" },
                    new GraphData<>( "heap-data", new Long[]{ current, max, free } )
            );

            table.reset();
            table.addRow( "Current", humanReadableByteCount( current, false ) );
            table.addRow( "Free", humanReadableByteCount( free, false ) );
            table.addRow( "Max", humanReadableByteCount( max, false ) );
        }
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
