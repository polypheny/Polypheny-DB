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
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class JavaInformation {


    public JavaInformation() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage( "javaInfo", "Java Runtime" );
        im.addPage( page );

        // JVM Info
        InformationGroup groupJvm = new InformationGroup( "JVM Detail", page.getId() ).setOrder( 1 );
        im.addGroup( groupJvm );

        InformationTable javaInfoJvm = new InformationTable(
                "javaJvmInfo",
                groupJvm.getId(),
                Arrays.asList( "Attribute", "Value" ) );
        im.registerInformation( javaInfoJvm );
        javaInfoJvm.addRow( "JVM Name", System.getProperty( "java.vm.name" ) );
        javaInfoJvm.addRow( "Runtime Version", System.getProperty( "java.runtime.version" ) );

        // Heap info
        InformationGroup groupHeap = new InformationGroup( "Java Heap Size", page.getId() ).setOrder( 2 );
        im.addGroup( groupHeap );

        InformationGraph heapInfoGraph = new InformationGraph(
                "javaHeapInfoGraph",
                groupHeap.getId(),
                GraphType.DOUGHNUT,
                new String[]{ "Current", "Maximum", "Free" }
        );
        im.registerInformation( heapInfoGraph );

        InformationTable heapInfoTable = new InformationTable(
                "javaHeapInfoTable",
                groupHeap.getId(),
                Arrays.asList( "Attribute", "Value" ) );
        im.registerInformation( heapInfoTable );

        JavaHeapInfo heapInfoRunnable = new JavaHeapInfo( heapInfoGraph, heapInfoTable );
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate( heapInfoRunnable, 0, 1, TimeUnit.MINUTES );

    }


    static class JavaHeapInfo implements Runnable {

        private final InformationGraph graph;
        private final InformationTable table;


        JavaHeapInfo( InformationGraph graph, InformationTable table ) {
            this.graph = graph;
            this.table = table;
        }


        @Override
        public void run() {
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
