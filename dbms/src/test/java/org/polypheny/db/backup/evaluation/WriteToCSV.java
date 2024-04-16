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

package org.polypheny.db.backup.evaluation;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import lombok.SneakyThrows;

public class WriteToCSV {

    /**
     * writes Data to a CSV
     * @param data measured execution time
     * @param typeOfData title for csv: Connection (PSQL, PolyPGI, PolyJdbc); Query Number (Q1); Nbr of Executions (E10000)
     * @throws IOException when something goes wrong with creating a filewriter
     */
    @SneakyThrows
    public void writeToCSV ( ArrayList<Long> data, String typeOfData) {
        System.out.println("started writing to csv file");

        //typeOfData: Connection (PSQL, PolyPGI, PolyJdbc); Query Number (Q1); Nbr of Executions (E10000)
        String filename = typeOfData + ".csv";
        String path = "C:\\Users\\esigu\\SynologyDrive\\01_Uni\\UniBasel\\23HS\\Bachelorarbeit\\Evaluation-Data\\" + filename;
        File file= new File(path);
        FileWriter filewriter = new FileWriter(file);
        //filewriter.append("execution_time_in_nanosecs");
        //filewriter.append(',');

        for (int i = 0; i < data.size(); i++) {
            String value = String.valueOf(data.get(i)*0.000001);    //millisecs
            filewriter.append(value);
            filewriter.append(' ');
        }

        filewriter.flush();
        filewriter.close();

        System.out.println("finished writing to file" + filename);

    }

}
