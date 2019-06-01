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

package ch.unibas.dmi.dbis.polyphenydb.catalog;


import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfColumnsException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfDatabasesException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfSchemasException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfTablesException;
import java.io.Serializable;
import lombok.EqualsAndHashCode;


/**
 * This class represents a internal name used in the catalog and provides utils for working with them.
 */
@EqualsAndHashCode
public final class InternalName implements Serializable {

    private static final long serialVersionUID = 3281298693122455674L;

    //   aaa      bbb      cc       dd
    //  Column   Table   Schema   Database
    public static final int INTERNAL_NAME_DATABASE_PART_LENGTH = 2;
    public static final int INTERNAL_NAME_SCHEMA_PART_LENGTH = 2;
    public static final int INTERNAL_NAME_TABLE_PART_LENGTH = 3;
    public static final int INTERNAL_NAME_COLUMN_PART_LENGTH = 3;
    public static final int INTERNAL_NAME_LENGTH = INTERNAL_NAME_DATABASE_PART_LENGTH + INTERNAL_NAME_SCHEMA_PART_LENGTH + INTERNAL_NAME_TABLE_PART_LENGTH + INTERNAL_NAME_COLUMN_PART_LENGTH;

    // Internal names are lower case!
    public static final String DIGITS = "abcdefghijklmnopqrstuvwxyz";


    private final int databasePart;
    private final int schemaPart;
    private final int tablePart;
    private final int columnPart;


    public static InternalName fromIndexes( final int databaseIndex, final int schemaIndex, final int tableIndex, final int columnIndex ) throws ExceedsMaximumNumberOfDatabasesException, ExceedsMaximumNumberOfSchemasException, ExceedsMaximumNumberOfTablesException, ExceedsMaximumNumberOfColumnsException {
        return new InternalName( databaseIndex, schemaIndex, tableIndex, columnIndex );
    }


    public InternalName( final int databaseIndex, final int schemaIndex, final int tableIndex, final int columnIndex ) throws ExceedsMaximumNumberOfDatabasesException, ExceedsMaximumNumberOfSchemasException, ExceedsMaximumNumberOfTablesException, ExceedsMaximumNumberOfColumnsException {
        // Check if indexes are valid
        if ( databaseIndex < 0 || schemaIndex < 0 || tableIndex < 0 || columnIndex < 0 ) {
            throw new IllegalArgumentException( "InternalName must be a positive number or zero!" );
        }
        if ( databaseIndex == 0 || schemaIndex == 0 && (tableIndex > 0 || columnIndex > 0) || tableIndex == 0 && columnIndex > 0 ) {
            throw new IllegalArgumentException( "Invalid indexes for InternalName!" );
        }

        if ( databaseIndex >= Math.pow( DIGITS.length(), INTERNAL_NAME_DATABASE_PART_LENGTH ) ) {
            throw new ExceedsMaximumNumberOfDatabasesException();
        }
        if ( schemaIndex >= Math.pow( DIGITS.length(), INTERNAL_NAME_SCHEMA_PART_LENGTH ) ) {
            throw new ExceedsMaximumNumberOfSchemasException();
        }
        if ( tableIndex >= Math.pow( DIGITS.length(), INTERNAL_NAME_TABLE_PART_LENGTH ) ) {
            throw new ExceedsMaximumNumberOfTablesException();
        }
        if ( columnIndex >= Math.pow( DIGITS.length(), INTERNAL_NAME_COLUMN_PART_LENGTH ) ) {
            throw new ExceedsMaximumNumberOfColumnsException();
        }

        databasePart = databaseIndex;
        schemaPart = schemaIndex;
        tablePart = tableIndex;
        columnPart = columnIndex;
    }


    // Constructor for internal use only, does not perform any checks and therefore not throws any exceptions
    private InternalName( final int[] indexes ) {
        databasePart = indexes[0];
        schemaPart = indexes[1];
        tablePart = indexes[2];
        columnPart = indexes[3];
    }


    public static InternalName fromString( final String internalName ) {
        return new InternalName( internalName );
    }


    public InternalName( final String internalName ) {
        String[] splittedInternalName = splitInternalName( internalName );
        databasePart = getIndexFromStr( splittedInternalName[0] );
        schemaPart = getIndexFromStr( splittedInternalName[1] );
        tablePart = getIndexFromStr( splittedInternalName[2] );
        columnPart = getIndexFromStr( splittedInternalName[3] );
    }


    @Override
    public String toString() {
        int database = databasePart;
        int schema = schemaPart;
        int table = tablePart;
        int column = columnPart;

        // a should mean 1 so decrease each index if > 0
        if ( database > 0 ) {
            database--;
        }
        if ( schema > 0 ) {
            schema--;
        }
        if ( table > 0 ) {
            table--;
        }
        if ( column > 0 ) {
            column--;
        }

        long index = (long) (database + schema * Math.pow( DIGITS.length(), INTERNAL_NAME_DATABASE_PART_LENGTH ) + table * Math.pow( DIGITS.length(), INTERNAL_NAME_DATABASE_PART_LENGTH + INTERNAL_NAME_SCHEMA_PART_LENGTH ) + column * Math.pow( DIGITS.length(), INTERNAL_NAME_DATABASE_PART_LENGTH + INTERNAL_NAME_SCHEMA_PART_LENGTH + INTERNAL_NAME_TABLE_PART_LENGTH ));

        char[] builder = new char[INTERNAL_NAME_LENGTH];

        long current = index;
        int offset = builder.length - 1;
        while ( current > 0 ) {
            builder[offset--] = DIGITS.charAt( (int) (current % DIGITS.length()) );
            current /= DIGITS.length();
        }

        if ( offset >= 0 ) {
            while ( offset >= 0 ) {
                builder[offset--] = DIGITS.charAt( 0 );
            }
        }

        int cut = 0;
        if ( columnPart == 0 ) {
            cut += INTERNAL_NAME_COLUMN_PART_LENGTH;
        }
        if ( tablePart == 0 ) {
            cut += INTERNAL_NAME_TABLE_PART_LENGTH;
        }
        if ( schemaPart == 0 ) {
            cut += INTERNAL_NAME_SCHEMA_PART_LENGTH;
        }

        return (new String( builder )).substring( cut );
    }


    public int getDatabasePart() {
        return databasePart;
    }


    public int getSchemaPart() {
        return schemaPart;
    }


    public int getTablePart() {
        return tablePart;
    }


    public int getColumnPart() {
        return columnPart;
    }


    public InternalName getDatabase() {
        return new InternalName( new int[]{ databasePart, 0, 0, 0 } );
    }


    public InternalName getSchema() {
        return new InternalName( new int[]{ databasePart, schemaPart, 0, 0 } );
    }


    public InternalName getTable() {
        return new InternalName( new int[]{ databasePart, schemaPart, tablePart, 0 } );
    }


    int[] getIndexes() {
        return new int[]{
                databasePart, schemaPart, tablePart, columnPart
        };
    }


    // 0: database part
    // 1: schema part
    // 2: table part
    // 3: column part
    private static String[] splitInternalName( final String internalName ) {
        if ( internalName.length() > INTERNAL_NAME_LENGTH || internalName.length() < INTERNAL_NAME_DATABASE_PART_LENGTH ) {
            throw new IllegalArgumentException( "Invalid InternalName: " + internalName );
        }

        String[] splittedInternalName = new String[4];

        String str = internalName;
        splittedInternalName[0] = str.substring( str.length() - INTERNAL_NAME_DATABASE_PART_LENGTH );
        str = str.substring( 0, str.length() - INTERNAL_NAME_DATABASE_PART_LENGTH );

        if ( str.length() >= INTERNAL_NAME_SCHEMA_PART_LENGTH ) {
            splittedInternalName[1] = str.substring( str.length() - INTERNAL_NAME_SCHEMA_PART_LENGTH );
            str = str.substring( 0, str.length() - INTERNAL_NAME_SCHEMA_PART_LENGTH );

            if ( str.length() >= INTERNAL_NAME_TABLE_PART_LENGTH ) {
                splittedInternalName[2] = str.substring( str.length() - INTERNAL_NAME_TABLE_PART_LENGTH );
                str = str.substring( 0, str.length() - INTERNAL_NAME_TABLE_PART_LENGTH );

                if ( str.length() >= INTERNAL_NAME_COLUMN_PART_LENGTH ) {
                    splittedInternalName[3] = str.substring( str.length() - INTERNAL_NAME_COLUMN_PART_LENGTH );
                    str = str.substring( 0, str.length() - INTERNAL_NAME_COLUMN_PART_LENGTH );
                }
            }

        }

        if ( str.length() != 0 ) {
            throw new IllegalArgumentException( internalName );
        }

        return splittedInternalName;
    }


    private static int getIndexFromStr( final String internalNamePart ) {
        if ( internalNamePart == null ) {
            return 0;
        }

        // remove leading a's
        String str = internalNamePart;
        for ( int i = 0; i < str.length(); i++ ) {
            if ( str.charAt( 0 ) == DIGITS.charAt( 0 ) ) {
                str = str.substring( 1, str.length() );
            }
        }

        double index = 0;
        int j = 0;
        for ( int i = str.length() - 1; i >= 0; i-- ) {
            index += (DIGITS.indexOf( str.charAt( i ) )) * Math.pow( DIGITS.length(), j );
            j++;
        }
        return (int) index + 1;
    }
}
