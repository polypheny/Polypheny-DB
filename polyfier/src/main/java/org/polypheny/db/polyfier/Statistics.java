/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.polyfier;

import org.polypheny.db.polyfier.core.PolyfierQueryExecutor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.OptionalDouble;

public abstract class Statistics {


    public static List<List<Object>> valueCounts(Statement statement, String table, String column ) {
        String sql = String.format("SELECT %s, COUNT(*) AS num FROM %s GROUP BY %s", column, table, column );
        return PolyfierQueryExecutor.auxiliaryExecute( statement, sql );
    }

    public static long getRowCount( Statement statement, String table ) {
        String sql = String.format( "SELECT COUNT(*) FROM %s", table );
        return (long) PolyfierQueryExecutor.auxiliaryExecute( statement, sql ).get( 0 ).get( 0 );
    }

    public static double valueCountAverage( List<List<Object>> valueCount ) {
        OptionalDouble optionalDouble = valueCount.stream().map(xs -> xs.get( 1 ) ).mapToLong(val -> (long) val ).average();
        if ( optionalDouble.isEmpty() ) {
            throw new RuntimeException("Could not evaluate Value Count.");
        }
        return optionalDouble.getAsDouble();
    }

    public static double labelAverage(PolyType polyType, List<List<Object>> valueCount ) {
        assert PolyType.NUMERIC_TYPES.contains( polyType );
        OptionalDouble optionalDouble = OptionalDouble.empty();
        switch ( polyType ) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                optionalDouble = valueCount.stream().map( xs -> xs.get( 0 ) ).mapToInt( val -> (int) val ).average();
                break;
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                optionalDouble = valueCount.stream().map( xs -> xs.get( 0 ) ).mapToDouble( val -> (double) val ).average();
                break;
        }
        if ( optionalDouble.isEmpty() ) {
            throw new RuntimeException("Could not evaluate Value Count.");
        }
        return optionalDouble.getAsDouble();
    }


    private static void createCsvFile(String csvFilePath, String[] header) {
        try {
            File file = new File(csvFilePath);

            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            FileWriter writer = new FileWriter(file);
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < header.length; i++) {
                sb.append(header[i]);

                if (i != header.length - 1) {
                    sb.append(",");
                }
            }

            writer.write(sb.toString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void appendToCsv(String csvFilePath, String[] data) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(csvFilePath, true));
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < data.length; i++) {
                sb.append(data[i]);

                if (i != data.length - 1) {
                    sb.append(",");
                }
            }

            writer.newLine();
            writer.write(sb.toString());

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String[] convertListToStringArray( List<Object> list ) {
        String[] stringArray = new String[list.size()];

        for (int i = 0; i < list.size(); i++) {
            stringArray[i] = ( isPrimitive( list.get( i ) ) ) ? String.valueOf( list.get( i )) : list.get(i).toString();
        }

        return stringArray;
    }

    public static void createCsvFile( String name, List<Object> headers ) {
        createCsvFile( System.getProperty("user.home") + File.separator + name + ".csv", convertListToStringArray( headers ) );
    }

    public static void appendCsvFile( String name, List<Object> data ) {
        appendToCsv( System.getProperty("user.home") + File.separator + name + ".csv", convertListToStringArray( data ) );
    }

    private static boolean isPrimitive(Object value) {
        return (value instanceof Boolean ||
                value instanceof Character ||
                value instanceof Byte ||
                value instanceof Short ||
                value instanceof Integer ||
                value instanceof Long ||
                value instanceof Float ||
                value instanceof Double);
    }


}
