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

package org.polypheny.db.protointerface;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class PIPerformanceLogger {
    private BufferedWriter writer;
    private static final String PATH = System.getProperty( "user.home" ) + "/uni_basel/master_project/benchmarking/";

    // Method to open a file. If the file exists, it will be overwritten.
    public void openFile(String fileName) {
        try {
            // FileWriter is set to false to overwrite the file if it exists
            writer = new BufferedWriter(new FileWriter(PATH + fileName, true));
        } catch ( IOException e) {
            e.printStackTrace();
        }
    }

    // Method to write a line to the file
    public void write(String line) {
        try {
            if (writer != null) {
                writer.write(line + ",");
            } else {
                System.out.println("Error: File is not opened yet.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeNewLine() {
        try {
            if (writer != null) {
                writer.newLine();
            } else {
                System.out.println("Error: File is not opened yet.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to close the file
    public void closeFile() {
        try {
            if (writer != null) {
                writer.close();
            } else {
                System.out.println("Error: File is not opened or already closed.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
