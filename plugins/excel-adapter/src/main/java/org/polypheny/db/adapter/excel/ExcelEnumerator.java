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

package org.polypheny.db.adapter.excel;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.Source;

class ExcelEnumerator implements Enumerator<PolyValue[]> {

    Iterator<Row> reader;
    private final AtomicBoolean cancelFlag;
    private final RowConverter rowConverter;
    private PolyValue[] current;

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    /**
     * Name of the column that is implicitly created in an Excel stream table to hold the data arrival time.
     */
    private static final String ROWTIME_COLUMN_NAME = "ROWTIME";
    private static String sheet;


    static {
        final TimeZone gmt = TimeZone.getTimeZone( "GMT" );
        TIME_FORMAT_DATE = FastDateFormat.getInstance( "yyyy-MM-dd", gmt );
        TIME_FORMAT_TIME = FastDateFormat.getInstance( "HH:mm:ss", gmt );
        TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance( "yyyy-MM-dd HH:mm:ss", gmt );
    }


    ExcelEnumerator( Source source, AtomicBoolean cancelFlag, List<ExcelFieldType> fieldTypes, String sheet ) {
        this( source, cancelFlag, fieldTypes, identityList( fieldTypes.size() ), sheet );
    }


    ExcelEnumerator( Source source, AtomicBoolean cancelFlag, List<ExcelFieldType> fieldTypes, int[] fields, String sheet ) {
        this( source, cancelFlag, false, null, converter( fieldTypes, fields ), sheet );
    }


    ExcelEnumerator( Source source, AtomicBoolean cancelFlag, boolean stream, String[] filterValues, RowConverter rowConverter, String sheet ) {
        this.cancelFlag = cancelFlag;
        this.rowConverter = rowConverter;
        try {
            if ( stream ) {
                //this.reader = new ExcelStreamReader( source );
            } else {
                this.reader = openExcel( source, sheet );
            }
            this.reader.next(); // skip header row
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private static RowConverter converter( List<ExcelFieldType> fieldTypes, int[] fields ) {
        return new ArrayRowConverter( fieldTypes, fields );
    }


    /**
     * Deduces the names and types of a table's columns by reading the first line of an Excel file.
     */
    static AlgDataType deduceRowType( JavaTypeFactory typeFactory, Source source, List<ExcelFieldType> fieldTypes ) {
        return deduceRowType( typeFactory, source, fieldTypes, false );
    }


    /**
     * Deduces the names and types of a table's columns by reading the first line of an Excel file.
     */
    static AlgDataType deduceRowType( JavaTypeFactory typeFactory, Source source, String sheetName, List<ExcelFieldType> fieldTypes ) {
        return deduceRowType( typeFactory, source, sheetName, fieldTypes, false );
    }


    /**
     * Deduces the names and types of a table's columns by reading the first line of an Excel file.
     */
    static AlgDataType deduceRowType( JavaTypeFactory typeFactory, Source source, List<ExcelFieldType> fieldTypes, Boolean stream ) {
        final List<AlgDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        final List<Long> ids = new ArrayList<>();

        if ( stream ) {
            names.add( ROWTIME_COLUMN_NAME );
            types.add( typeFactory.createPolyType( PolyType.TIMESTAMP ) );
            ids.add( null );
        }
        try {
            Iterator<Row> rows = openExcel( source, "" );
            while ( rows.hasNext() ) {
                Row row = rows.next();
                Iterator<Cell> cellIterator = row.cellIterator();
                while ( cellIterator.hasNext() ) {
                    Cell cell = cellIterator.next();
                    names.add( cell.getStringCellValue() );
                    ids.add( null );
                }
                break;
            }

            while ( rows.hasNext() ) {
                Row row = rows.next();
                Iterator<Cell> cellIterator = row.cellIterator();
                while ( cellIterator.hasNext() ) {
                    Cell cell = cellIterator.next();
                    ExcelFieldType fieldType = ExcelFieldType.of( cell );
                    AlgDataType type;
                    if ( fieldType == null ) {
                        type = typeFactory.createJavaType( String.class );
                    } else {
                        type = fieldType.toType( typeFactory );
                    }
                    types.add( type );
                    if ( fieldTypes != null ) {
                        fieldTypes.add( fieldType );
                    }
                }

            }
        } catch ( IOException e ) {
            // ignore
        }
        if ( names.isEmpty() ) {
            names.add( "line" );
            types.add( typeFactory.createPolyType( PolyType.VARCHAR ) );
            ids.add( null );
        }
        return typeFactory.createStructType( ids, types, names );
    }


    /**
     * Deduces the names and types of a table's columns by reading the first line of a Excel file.
     */
    static AlgDataType deduceRowType( JavaTypeFactory typeFactory, Source source, String sheetname, List<ExcelFieldType> fieldTypes, Boolean stream ) {
        final List<AlgDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        final List<Long> ids = new ArrayList<>();

        if ( stream ) {
            names.add( ROWTIME_COLUMN_NAME );
            types.add( typeFactory.createPolyType( PolyType.TIMESTAMP ) );
            ids.add( null );
        }
        try {
            Iterator<Row> rows = openExcel( source, sheetname );
            while ( rows.hasNext() ) {
                Row row = rows.next();
                Iterator<Cell> cellIterator = row.cellIterator();
                while ( cellIterator.hasNext() ) {
                    Cell cell = cellIterator.next();
                    names.add( cell.getStringCellValue() );
                    ids.add( null );
                }
                break;
            }

            while ( rows.hasNext() ) {
                Row row = rows.next();
                Iterator<Cell> cellIterator = row.cellIterator();
                while ( cellIterator.hasNext() ) {
                    Cell cell = cellIterator.next();
                    ExcelFieldType fieldType = ExcelFieldType.of( cell );
                    AlgDataType type;
                    if ( fieldType == null ) {
                        type = typeFactory.createJavaType( String.class );
                    } else {
                        type = fieldType.toType( typeFactory );
                    }
                    types.add( type );
                    if ( fieldTypes != null ) {
                        fieldTypes.add( fieldType );
                    }
                }

            }
        } catch ( IOException e ) {
            // ignore
        }
        if ( names.isEmpty() ) {
            names.add( "line" );
            types.add( typeFactory.createPolyType( PolyType.VARCHAR ) );
        }
        return typeFactory.createStructType( ids, types, names );
    }


    public static Iterator<Row> openExcel( Source source, String sheetname ) throws IOException {
        Sheet sheet;
        Iterator<Row> rowIterator = null;
        FileInputStream fileIn = new FileInputStream( source.file() );

        Workbook workbook = WorkbookFactory.create( fileIn );
        workbook.getNumberOfSheets();
        if ( sheetname.isEmpty() ) {
            sheet = workbook.getSheetAt( 0 );
        } else {
            sheet = workbook.getSheet( sheetname );
        }

        rowIterator = sheet.iterator();
        return rowIterator;
    }


    public static void setSheet( String sheetName ) {
        sheet = sheetName;
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            outer:
            for ( ; ; ) {
                if ( cancelFlag.get() ) {
                    return false;
                }

                Row columnValues = null;
                try {
                    if ( reader.hasNext() ) {
                        columnValues = reader.next();
                    }
                } catch ( Exception e ) {
                    columnValues = null;
                }

                if ( columnValues == null ) {
                    if ( reader instanceof ExcelStreamReader ) {
                        try {
                            Thread.sleep( ExcelStreamReader.DEFAULT_MONITOR_DELAY );
                        } catch ( InterruptedException e ) {
                            throw new GenericRuntimeException( e );
                        }
                        System.out.println( "Stream" );
                        continue;
                    }
                    current = null;
                    return false;
                }
                current = rowConverter.convertRow( columnValues );
                return true;
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }

    }


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
//        try {
//            reader.close();
//        } catch ( IOException e ) {
//            throw new RuntimeException( "Error closing CSV reader", e );
//        }
    }


    /**
     * Returns an array of integers {0, ..., n - 1}.
     */
    static int[] identityList( int n ) {
        int[] integers = new int[n];
        for ( int i = 0; i < n; i++ ) {
            integers[i] = i;
        }
        return integers;
    }


    /**
     * Row converter.
     */
    abstract static class RowConverter {

        abstract PolyValue[] convertRow( Row rows );


        protected PolyValue convert( ExcelFieldType fieldType, Cell cell ) {
            if ( fieldType == null ) {
                throw new NotImplementedException();
                //return cell;
            }
            if ( cell == null ) {
                return PolyNull.NULL;
            }
            try {

                switch ( fieldType ) {
                    case BOOLEAN:
                        return PolyBoolean.of( cell.getBooleanCellValue() );
                    case BYTE:
                        return PolyInteger.of( Byte.parseByte( cell.getStringCellValue() ) );
                    case SHORT:
                        return PolyInteger.of( Short.parseShort( cell.getStringCellValue() ) );
                    case INT:
                        return PolyInteger.of( cell.getNumericCellValue() );
                    case LONG:
                        if ( cell.getCellType() == CellType.STRING ) {
                            return PolyLong.of( Long.parseLong( cell.getStringCellValue() ) );
                        }
                        return PolyLong.of( cell.getNumericCellValue() );
                    case FLOAT:
                        if ( cell.getCellType() == CellType.STRING ) {
                            return PolyFloat.of( Float.parseFloat( cell.getStringCellValue() ) );
                        }
                        return PolyFloat.of( cell.getNumericCellValue() );
                    case DOUBLE:
                        return PolyDouble.of( cell.getNumericCellValue() );
                    case DATE:
                        try {
                            //convert date from string to date
                            if ( cell.getCellType() == CellType.STRING ) {
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern( "dd.MM.yyyy", Locale.ENGLISH );
                                LocalDate date2 = LocalDate.parse( cell.getStringCellValue(), formatter );
                                return PolyDate.of( (TimeUnit.DAYS.toMillis( date2.toEpochDay() ) / DateTimeUtils.MILLIS_PER_DAY) );
                            }
                            Date date = cell.getDateCellValue();
                            return PolyDate.of( (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY) );


                        } catch ( Exception e ) {
                            throw new GenericRuntimeException( "Could not read the date field from the document." );
                        }
                    case TIME:

                        try {
                            Date date = TIME_FORMAT_TIME.parse( cell
                                    .getStringCellValue() );
                            return PolyTime.of( date.getTime() );


                        } catch ( Exception e ) {
                            throw new GenericRuntimeException( "Could not read the time field from the document." );
                        }
                    case TIMESTAMP:
                        try {
                            Date date = TIME_FORMAT_TIMESTAMP.parse( cell
                                    .getStringCellValue() );
                            return PolyTimestamp.of( date.getTime() );
                        } catch ( Exception e ) {
                            throw new GenericRuntimeException( "Could not read the timestamp field from the document." );
                        }
                    case STRING:
                    default:
                        return PolyString.of( cell.getStringCellValue() );
                }
            } catch ( Exception e ) {
                throw new GenericRuntimeException( "Could not read %s from the document.", cell );
            }
        }

    }


    /**
     * Array row converter.
     */
    static class ArrayRowConverter extends RowConverter {

        private final ExcelFieldType[] fieldTypes;
        private final int[] fields;
        // whether the row to convert is from a stream
        private final boolean stream;


        ArrayRowConverter( List<ExcelFieldType> fieldTypes, int[] fields ) {
            this.fieldTypes = fieldTypes.toArray( new ExcelFieldType[0] );
            this.fields = fields;
            this.stream = false;
        }


        ArrayRowConverter( List<ExcelFieldType> fieldTypes, int[] fields, boolean stream ) {
            this.fieldTypes = fieldTypes.toArray( new ExcelFieldType[0] );
            this.fields = fields;
            this.stream = stream;
        }


        @Override
        public PolyValue[] convertRow( Row row ) {
            if ( stream ) {
                return convertStreamRow( row );
            } else {
                return convertNormalRow( row );
            }
        }


        public PolyValue[] convertNormalRow( Row row ) {
            Iterator<Cell> cells = row.cellIterator();
            final PolyValue[] objects = new PolyValue[fields.length];
            while ( cells.hasNext() ) {
                Cell cell = cells.next();
                int field = fields[cell.getColumnIndex()] - 1;
                objects[field] = convert( fieldTypes[field], cell );
            }
            return objects;
        }


        public PolyValue[] convertStreamRow( Row row ) {
            final PolyValue[] objects = new PolyValue[fields.length + 1];
            objects[0] = PolyLong.of( System.currentTimeMillis() );
            return objects;
        }

    }


    /**
     * Single column row converter.
     */
    private static class SingleColumnRowConverter extends RowConverter {

        private final ExcelFieldType fieldType;
        private final int fieldIndex;


        private SingleColumnRowConverter( ExcelFieldType fieldType, int fieldIndex ) {
            this.fieldType = fieldType;
            this.fieldIndex = fieldIndex;
        }


        @Override
        public PolyValue[] convertRow( Row row ) {
            return new PolyValue[]{ convert( fieldType, row.getCell( fieldIndex ) ) };
        }

    }

}
