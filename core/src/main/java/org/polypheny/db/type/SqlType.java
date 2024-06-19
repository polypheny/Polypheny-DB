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

package org.polypheny.db.type;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Extends the information in {@link java.sql.Types}.
 *
 * <p>The information in the following conversions tables
 * (from the JDBC 4.1 specification) is held in members of this class.
 *
 * <p>Table B-1: JDBC Types Mapped to Java Types
 *
 * <pre>
 * JDBC Type     Java Type
 * ============= =========================
 * CHAR          String
 * VARCHAR       String
 * LONGVARCHAR   String
 * NUMERIC       java.math.BigDecimal
 * DECIMAL       java.math.BigDecimal
 * BIT           boolean
 * BOOLEAN       boolean
 * TINYINT       byte
 * SMALLINT      short
 * INTEGER       int
 * BIGINT        long
 * REAL          float
 * FLOAT         double
 * DOUBLE        double
 * BINARY        byte[]
 * VARBINARY     byte[]
 * LONGVARBINARY byte[]
 * DATE          java.sql.Date
 * TIME          java.sql.Time
 * TIMESTAMP     java.sql.Timestamp
 * CLOB          java.sql.Clob
 * BLOB          java.sql.Blob
 * ARRAY         java.sql.Array
 * DISTINCT      mapping of underlying type
 * STRUCT        java.sql.Struct
 * REF           java.sql.Ref
 * DATALINK      java.net.URL
 * JAVA_OBJECT   underlying Java class
 * ROWID         java.sql.RowId
 * NCHAR         String
 * NVARCHAR      String
 * LONGNVARCHAR  String
 * NCLOB         java.sql.NClob
 * SQLXML        java.sql.SQLXML
 * </pre>
 *
 * <p>Table B-2: Standard Mapping from Java Types to JDBC Types
 *
 * <pre>
 * Java Type            JDBC Type
 * ==================== ==============================================
 * String               CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR or
 *                      LONGNVARCHAR
 * java.math.BigDecimal NUMERIC
 * boolean              BIT or BOOLEAN
 * byte                 TINYINT
 * short                SMALLINT
 * int                  INTEGER
 * long                 BIGINT
 * float                REAL
 * double               DOUBLE
 * byte[]               BINARY, VARBINARY, or LONGVARBINARY
 * java.sql.Date        DATE
 * java.sql.Time        TIME
 * java.sql.Timestamp   TIMESTAMP
 * java.sql.Clob        CLOB
 * java.sql.Blob        BLOB
 * java.sql.Array       ARRAY
 * java.sql.Struct      STRUCT
 * java.sql.Ref         REF
 * java.net.URL         DATALINK
 * Java class           JAVA_OBJECT
 * java.sql.RowId       ROWID
 * java.sql.NClob       NCLOB
 * java.sql.SQLXML      SQLXML
 * </pre>
 *
 * <p>TABLE B-3: Mapping from JDBC Types to Java Object Types
 *
 * <pre>
 * JDBC Type     Java Object Type
 * ============= ======================
 * CHAR          String
 * VARCHAR       String
 * LONGVARCHAR   String
 * NUMERIC       java.math.BigDecimal
 * DECIMAL       java.math.BigDecimal
 * BIT           Boolean
 * BOOLEAN       Boolean
 * TINYINT       Integer
 * SMALLINT      Integer
 * INTEGER       Integer
 * BIGINT        Long
 * REAL          Float
 * FLOAT         Double
 * DOUBLE        Double
 * BINARY        byte[]
 * VARBINARY     byte[]
 * LONGVARBINARY byte[]
 * DATE          java.sql.Date
 * TIME          java.sql.Time
 * TIMESTAMP     java.sql.Timestamp
 * DISTINCT      Object type of underlying type
 * CLOB          java.sql.Clob
 * BLOB          java.sql.Blob
 * ARRAY         java.sql.Array
 * STRUCT        java.sql.Struct or java.sql.SQLData
 * REF           java.sql.Ref
 * DATALINK      java.net.URL
 * JAVA_OBJECT   underlying Java class
 * ROWID         java.sql.RowId
 * NCHAR         String
 * NVARCHAR      String
 * LONGNVARCHAR  String
 * NCLOB         java.sql.NClob
 * SQLXML        java.sql.SQLXML
 * </pre>
 *
 * <p>TABLE B-4: Mapping from Java Object Types to JDBC Types
 *
 * <pre>
 * Java Object Type     JDBC Type
 * ==================== ===========================================
 * String               CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR
 *                      or LONGNVARCHAR
 * java.math.BigDecimal NUMERIC
 * Boolean              BIT or BOOLEAN
 * Byte                 TINYINT
 * Short                SMALLINT
 * Integer              INTEGER
 * Long                 BIGINT
 * Float                REAL
 * Double               DOUBLE
 * byte[]               BINARY, VARBINARY, or LONGVARBINARY
 * java.math.BigInteger BIGINT
 * java.sql.Date        DATE
 * java.sql.Time        TIME
 * java.sql.Timestamp   TIMESTAMP
 * java.sql.Clob        CLOB
 * java.sql.Blob        BLOB
 * java.sql.Array       ARRAY
 * java.sql.Struct      STRUCT
 * java.sql.Ref         REF
 * java.net.URL         DATALINK
 * Java class           JAVA_OBJECT
 * java.sql.RowId       ROWID
 * java.sql.NClob       NCLOB
 * java.sql.SQLXML      SQLXML
 * java.util.Calendar   TIMESTAMP
 * java.util.Date       TIMESTAMP
 * </pre>
 *
 * <p><a id="B5">TABLE B-5</a>: Conversions performed by {@code setObject} and
 * {@code setNull} between Java object types and target JDBC types
 * <!--
 * CHECKSTYLE: OFF
 * -->
 * <pre>
 *                      T S I B R F D D N B B C V L B V L D T T A B C S R D J R N N L N S
 *                      I M N I E L O E U I O H A O I A O A I I R L L T E A A O C V O C Q
 *                      N A T G A O U C M T O A R N N R N T M M R O O R F T V W H A N L L
 *                      Y L E I L A B I E   L R C G A B G E E E A B B U   A A I A R G O X
 *                      I L G N   T L M R   E   H V R I V E   S Y     C   L _ D R C N B M
 *                      N I E T     E A I   A   A A Y N A     T       T   I O     H V   L
 *                      T N R         L C   N   R R   A R     A           N B     A A
 *                        T                       C   R B     M           K J     R R
 *                                                H   Y I     P                     C
 * Java type
 * ==================== = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
 * String               x x x x x x x x x x x x x x x x x x x x . . . . . . . . x x x . .
 * java.math.BigDecimal x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Boolean              x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Byte                 x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Short                x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Integer              x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Long                 x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Float                x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * Double               x x x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * byte[]               . . . . . . . . . . . . . . x x x . . . . . . . . . . . . . . . .
 * java.math.BigInteger . . . x . . . . . . . x x x . . . . . . . . . . . . . . . . . . .
 * java.sql.Date        . . . . . . . . . . . x x x . . . x . x . . . . . . . . . . . . .
 * java.sql.Time        . . . . . . . . . . . x x x . . . . x x . . . . . . . . . . . . .
 * java.sql.Timestamp   . . . . . . . . . . . x x x . . . x x x . . . . . . . . . . . . .
 * java.sql.Array       . . . . . . . . . . . . . . . . . . . . x . . . . . . . . . . . .
 * java.sql.Blob        . . . . . . . . . . . . . . . . . . . . . x . . . . . . . . . . .
 * java.sql.Clob        . . . . . . . . . . . . . . . . . . . . . . x . . . . . . . . . .
 * java.sql.Struct      . . . . . . . . . . . . . . . . . . . . . . . x . . . . . . . . .
 * java.sql.Ref         . . . . . . . . . . . . . . . . . . . . . . . . x . . . . . . . .
 * java.net.URL         . . . . . . . . . . . . . . . . . . . . . . . . . x . . . . . . .
 * Java class           . . . . . . . . . . . . . . . . . . . . . . . . . . x . . . . . .
 * java.sql.Rowid       . . . . . . . . . . . . . . . . . . . . . . . . . . . x . . . . .
 * java.sql.NClob       . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . x .
 * java.sql.SQLXML      . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . x
 * java.util.Calendar   . . . . . . . . . . . x x x . . . x x x . . . . . . . . . . . . .
 * java.util.Date       . . . . . . . . . . . x x x . . . x x x . . . . . . . . . . . . .
 * </pre>
 * <!--
 * CHECKSTYLE: ON
 * -->
 *
 * <p><a id="B6">TABLE B-6</a>: Use of {@code ResultSet} getter methods to
 * retrieve JDBC data types
 * <!--
 * CHECKSTYLE: OFF
 * -->
 * <pre>
 *                      T S I B R F D D N B B C V L B V L D T T C B A R D S J R N N L N S
 *                      I M N I E L O E U I O H A O I A O A I I L L R E A T A O C V O C Q
 *                      N A T G A O U C M T O A R N N R N T M M O O R F T R V W H A N L L
 *                      Y L E I L A B I E   L R C G A B G E E E B B A   A U A I A R G O X
 *                      I L G N   T L M R   E   H V R I V E   S     Y   L C _ D R C N B M
 *                      N I E T     E A I   A   A A Y N A     T         I T O     H V   L
 *                      T N R         L C   N   R R   A R     A         N   B     A A
 *                        T                       C   R B     M         K   J     R R
 *                                                H   Y I     P                     C
 * Java type
 * ==================== = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
 * getByte              X x x x x x x x x x x x x . . . . . . . . . . . . . . x . . . . .
 * getShort             x X x x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * getInt               x x X x x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * getLong              x x x X x x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * getFloat             x x x x X x x x x x x x x . . . . . . . . . . . . . . . . . . . .
 * getDouble            x x x x x X X x x x x x x . . . . . . . . . . . . . . . . . . . .
 * getBigDecimal        x x x x x x x X X x x x x . . . . . . . . . . . . . . . . . . . .
 * getBoolean           x x x x x x x x x X x x x . . . . . . . . . . . . . . . . . . . .
 * getString            x x x x x x x x x x x X X x x x x x x x . . . . x . . . x x x . .
 * getNString           x x x x x x x x x x x x x x x x x x x x . . . . x . . . X X x . .
 * getBytes             . . . . . . . . . . . . . . X X x . . . . . . . . . . . . . . . .
 * getDate              . . . . . . . . . . . x x x . . . X . x . . . . . . . . . . . . .
 * getTime              . . . . . . . . . . . x x x . . . . X x . . . . . . . . . . . . .
 * getTimestamp         . . . . . . . . . . . x x x . . . x x X . . . . . . . . . . . x .
 * getAsciiStream       . . . . . . . . . . . x x X x x x . . . x . . . . . . . . . . . x
 * getBinaryStream      . . . . . . . . . . . . . . x x X . . . . x . . . . . . . . . x x
 * getCharacterStream   . . . . . . . . . . . x x X x x x . . . x . . . . . . . x x x x x
 * getNCharacterStream  . . . . . . . . . . . x x x x x x . . . x . . . . . . . x x X x x
 * getClob              . . . . . . . . . . . . . . . . . . . . X . . . . . . . . . . x .
 * getNClob             . . . . . . . . . . . . . . . . . . . . x . . . . . . . . . . X .
 * getBlob              . . . . . . . . . . . . . . . . . . . . . X . . . . . . . . . . .
 * getArray             . . . . . . . . . . . . . . . . . . . . . . X . . . . . . . . . .
 * getRef               . . . . . . . . . . . . . . . . . . . . . . . X . . . . . . . . .
 * getURL               . . . . . . . . . . . . . . . . . . . . . . . . X . . . . . . . .
 * getObject            x x x x x x x x x x x x x x x x x x x x x x x x x X X x x x x x x
 * getRowId             . . . . . . . . . . . . . . . . . . . . . . . . . . . X . . . . .
 * getSQLXML            . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . X
 * </pre>
 * <!--
 * CHECKSTYLE: ON
 * -->
 */
public enum SqlType {
    BIT( Types.BIT ),
    BOOLEAN( Types.BOOLEAN ),
    TINYINT( Types.TINYINT ),
    SMALLINT( Types.SMALLINT ),
    INTEGER( Types.INTEGER ),
    BIGINT( Types.BIGINT ),
    NUMERIC( Types.NUMERIC ),
    DECIMAL( Types.DECIMAL ),
    FLOAT( Types.FLOAT ),
    REAL( Types.REAL ),
    DOUBLE( Types.DOUBLE ),
    DATE( Types.DATE ),
    TIME( Types.TIME ),
    TIMESTAMP( Types.TIMESTAMP ),
    INTERVAL_YEAR_MONTH( Types.OTHER ),
    INTERVAL_DAY_TIME( Types.OTHER ),
    CHAR( Types.CHAR ),
    VARCHAR( Types.VARCHAR ),
    LONGVARCHAR( Types.LONGVARCHAR ),
    BINARY( Types.BINARY ),
    VARBINARY( Types.VARBINARY ),
    LONGVARBINARY( Types.LONGVARBINARY
    ),
    NULL( Types.NULL ),
    ANY( Types.JAVA_OBJECT ),
    SYMBOL( Types.OTHER ),
    MULTISET( Types.ARRAY ),
    ARRAY( Types.ARRAY ),
    BLOB( Types.BLOB ),
    CLOB( Types.CLOB ),
    SQLXML( Types.SQLXML ),
    MAP( Types.OTHER ),
    DISTINCT( Types.DISTINCT ),
    STRUCT( Types.STRUCT ),
    REF( Types.REF ),
    DATALINK( Types.DATALINK ),
    JAVA_OBJECT( Types.JAVA_OBJECT ),
    ROWID( Types.ROWID ),
    NCHAR( Types.NCHAR ),
    NVARCHAR( Types.NVARCHAR ),
    LONGNVARCHAR( Types.LONGNVARCHAR ),
    NCLOB( Types.NCLOB ),
    ROW( Types.STRUCT ),
    OTHER( Types.OTHER ),
    CURSOR( 2012 ),
    TIME_WITH_TIMEZONE( 2013 ),
    TIMESTAMP_WITH_TIMEZONE( 2014 ),
    COLUMN_LIST( Types.OTHER + 2 );

    /**
     * Type id as appears in {@link java.sql.Types},
     * e.g. {@link java.sql.Types#INTEGER}.
     */
    public final int id;

    private static final Map<Integer, SqlType> BY_ID = new HashMap<>();


    static {
        for ( SqlType sqlType : values() ) {
            BY_ID.put( sqlType.id, sqlType );
        }
    }


    SqlType( int id ) {
        this.id = id;
    }


    public static SqlType valueOf( int type ) {
        final SqlType sqlType = BY_ID.get( type );
        if ( sqlType == null ) {
            throw new IllegalArgumentException( "Unknown SQL type " + type );
        }
        return sqlType;
    }


}
