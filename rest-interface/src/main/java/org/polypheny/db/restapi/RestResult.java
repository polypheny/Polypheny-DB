/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.restapi;


import com.google.gson.Gson;
import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import io.javalin.http.Context;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.Pair;


@Slf4j
public class RestResult {

    private final Kind Kind;
    private final Iterator<Object> iterator;
    private final AlgDataType dataType;
    List<ColumnMetaData> columns;
    private List<Map<String, Object>> result;
    @Getter
    private long executionTime;

    boolean containsFiles = false;
    File zipFile;
    FileOutputStream fos;
    ZipOutputStream zipOut;


    public RestResult( Kind Kind, Iterator<Object> iterator, AlgDataType dataType, List<ColumnMetaData> columns ) {
        this.Kind = Kind;
        this.iterator = iterator;
        this.dataType = dataType;
        this.columns = columns;
    }


    public RestResult transform() {
        if ( Kind.belongsTo( Kind.DML ) ) {
            transformDML();
        } else {
            transformNonDML();
        }
        return this;
    }


    private void transformDML() {
        Object object;
        int rowsChanged = -1;
        while ( iterator.hasNext() ) {
            object = iterator.next();
            int num;
            if ( object != null && object.getClass().isArray() ) {
                Object[] o = (Object[]) object;
                num = ((Number) o[0]).intValue();
            } else if ( object != null ) {
                num = ((Number) object).intValue();
            } else {
                throw new RuntimeException( "Result is null" );
            }
            // Check if num is equal for all stores
            if ( rowsChanged != -1 && rowsChanged != num ) {
                throw new RuntimeException( "The number of changed rows is not equal for all stores!" );
            }
            rowsChanged = num;
        }
        List<Map<String, Object>> result = new ArrayList<>();
        HashMap<String, Object> map = new HashMap<>();
        map.put( columns.get( 0 ).columnName, rowsChanged );
        result.add( map );
        this.result = result;
    }


    private void transformNonDML() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<Map<String, Object>> result = new ArrayList<>();
        while ( iterator.hasNext() ) {
            Object next = iterator.next();
            Object[] row;
            if ( next.getClass().isArray() ) {
                row = (Object[]) next;
            } else {
                row = new Object[]{ next };
            }
            HashMap<String, Object> temp = new HashMap<>();
            int i = 0;
            for ( AlgDataTypeField type : dataType.getFieldList() ) {
                Object o = row[i];
                if ( type.getType().getPolyType().getFamily() == PolyTypeFamily.MULTIMEDIA ) {
                    if ( o instanceof File ) {
                        o = addZipEntry( o );
                    } else if ( o instanceof InputStream || o instanceof Blob ) {
                        o = addZipEntry( o );
                    } else if ( o instanceof byte[] ) {
                        o = addZipEntry( o );
                    }
                    temp.put( columns.get( i ).columnName, o );
                } else {
                    if ( type.getType().getPolyType().equals( PolyType.TIMESTAMP ) ) {
                        Long nanoSeconds = (Long) o;
                        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond( nanoSeconds / 1000L, (int) ((nanoSeconds % 1000) * 1000), ZoneOffset.UTC );
                        temp.put( columns.get( i ).columnName, localDateTime.toString() );
                    } else if ( type.getType().getPolyType().equals( PolyType.TIME ) ) {
                        temp.put( columns.get( i ).columnName, o.toString() );
                    } else {
                        temp.put( columns.get( i ).columnName, o );
                    }
                }
                i++;
            }
            result.add( temp );
        }
        stopWatch.stop();
        this.executionTime = stopWatch.getNanoTime();
        this.result = result;
    }


    private String addZipEntry( final Object data ) {
        //see https://www.baeldung.com/java-compress-and-uncompress
        containsFiles = true;
        String tempFileName = UUID.randomUUID().toString();
        try {
            if ( zipFile == null ) {
                zipFile = new File( System.getProperty( "user.home" ), ".polypheny/tmp/" + UUID.randomUUID().toString() + ".zip" );
                fos = new FileOutputStream( zipFile );
                zipOut = new ZipOutputStream( fos );
            }
            ZipEntry zipEntry = new ZipEntry( tempFileName + getContentType( data ) );
            zipOut.putNextEntry( zipEntry );
            if ( data instanceof File ) {
                File f = ((File) data);
                FileInputStream fis = new FileInputStream( f );
                byte[] bytes = new byte[1024];
                int len;
                while ( (len = fis.read( bytes )) > 0 ) {
                    zipOut.write( bytes, 0, len );
                }
                fis.close();
            } else if ( data instanceof InputStream ) {
                IOUtils.copyLarge( (InputStream) data, fos );
            } else if ( data instanceof byte[] ) {
                fos.write( (byte[]) data );
            }
            //zipOut.close();
            //fos.close();
        } catch ( IOException e ) {
            log.error( "Could not write to zip file", e );
        }
        return tempFileName;
    }


    private String getContentType( Object o ) {
        ContentInfoUtil util = new ContentInfoUtil();
        ContentInfo info;
        if ( o instanceof File ) {
            try {
                info = util.findMatch( (File) o );
            } catch ( IOException e ) {
                log.error( "Could not determine content type of file {}", ((File) o).getAbsolutePath() );
                return "";
            }
        } else if ( o instanceof byte[] ) {
            info = util.findMatch( (byte[]) o );
        } else if ( o instanceof InputStream ) {
            PushbackInputStream pbis = new PushbackInputStream( (InputStream) o, ContentInfoUtil.DEFAULT_READ_SIZE );
            byte[] buffer = new byte[ContentInfoUtil.DEFAULT_READ_SIZE];
            try {
                pbis.read( buffer );
                info = util.findMatch( buffer );
                pbis.unread( buffer );
            } catch ( IOException e ) {
                log.error( "Could not determine content type of InputStream" );
                return "";
            }
        } else {
            throw new RuntimeException( "Unexpected data for content type detection: " + o.getClass().getSimpleName() );
        }
        if ( info != null && info.getFileExtensions() != null && info.getFileExtensions().length > 0 ) {
            return "." + info.getFileExtensions()[0];
        } else {
            return "";
        }
    }


    public Pair<String, Integer> getResult( final Context ctx ) {
        Gson gson = new Gson();
        Map<String, Object> finalResult = new HashMap<>();
        finalResult.put( "result", result );
        finalResult.put( "size", result.size() );
        if ( !containsFiles ) {
            return new Pair( gson.toJson( finalResult ), finalResult.size() );
        } else {
            OutputStream os;
            ZipEntry zipEntry = new ZipEntry( "data.json" );
            try {
                zipOut.putNextEntry( zipEntry );
                zipOut.write( gson.toJson( finalResult ).getBytes( StandardCharsets.UTF_8 ) );
                zipOut.close();
                fos.close();
                ctx.contentType( "application/octet-stream" );
                //ctx.res.setContentType( "application/octet-stream" );
                ctx.res.setHeader( "Content-Disposition", "attachment; filename=result.zip" );
                os = ctx.res.getOutputStream();
                FileInputStream fis = new FileInputStream( zipFile );
                byte[] buf = new byte[2048];
                int len;
                while ( (len = fis.read( buf )) > 0 ) {
                    os.write( buf, 0, len );
                }
                if ( !zipFile.delete() ) {
                    log.warn( "Could not delete {}", zipFile.getAbsolutePath() );
                }
            } catch ( IOException e ) {
                zipFile.delete();
                ctx.status( 500 );
            }
            return new Pair( "", finalResult.size() );
        }
    }

}
