/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.processing.shuttles;


import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import com.j256.simplemagic.ContentType;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.FileInputHandle;


public class ParameterValueValidator extends AlgShuttleImpl {

    final AlgDataType rowType;
    final DataContext dataContext;


    public ParameterValueValidator( final AlgDataType rowType, DataContext dataContext ) {
        this.rowType = rowType;
        this.dataContext = dataContext;
    }


    /**
     * Visits a particular child of a parent, without copying a child
     */
    @Override
    protected <T extends AlgNode> T visitChild( T parent, int i, AlgNode child ) {
        stack.push( parent );
        try {
            child.accept( this );
            return parent;
        } finally {
            stack.pop();
        }
    }


    @Override
    public AlgNode visit( LogicalProject project ) {
        ParameterValueValidator2 validator2 = new ParameterValueValidator2();
        validator2.apply( project.getChildExps() );
        return super.visit( project );
    }


    class ParameterValueValidator2 extends RexShuttle {

        @Override
        public RexNode visitDynamicParam( RexDynamicParam dynamicParam ) {
            long index = dynamicParam.getIndex();
            if ( dataContext.getParameterType( index ) == null ) {
                //skip validation if parameterType is not set
                return super.visitDynamicParam( dynamicParam );
            }
            PolyType polyType = dataContext.getParameterType( index ).getPolyType();
            //PolyType polyType = dynamicParam.getType().getPolyType();//is not always correct
            Object o = null;
            boolean valid = true;
            for ( Map<Long, Object> map : dataContext.getParameterValues() ) {
                o = map.get( index );
                if ( o == null ) {
                    break;
                /*    if ( dynamicParam.getType().isNullable() ) {
                        break;
                    } else {
                        throw new InvalidParameterValueException( "Null in not nullable column" );
                    }
                 */
                }
                switch ( polyType.getFamily() ) {
                    //case ANY:
                    //break;
                    case CHARACTER:
                        valid = o instanceof String || o instanceof Character || o instanceof Character[];
                        break;
                    case NUMERIC:
                        valid = o instanceof Number;
                        break;
                    case DATE:
                        valid = o instanceof Date || o instanceof Integer;
                        break;
                    case TIME:
                        valid = o instanceof Time || o instanceof Integer;
                        break;
                    case TIMESTAMP:
                        valid = o instanceof Timestamp || o instanceof Long;
                        break;
                    case BOOLEAN:
                        valid = o instanceof Boolean;
                        break;
                    case MULTIMEDIA:
                        if ( polyType == PolyType.FILE || !RuntimeConfig.VALIDATE_MM_CONTENT_TYPE.getBoolean() ) {
                            if ( o instanceof byte[] || o instanceof InputStream || o instanceof File || o instanceof FileInputHandle ) {
                                return super.visitDynamicParam( dynamicParam );
                            } else {
                                throw new InvalidParameterValueException( String.format( "Parameter value '%s' of type %s does not match the PolyType %s", o.toString(), o.getClass().getSimpleName(), polyType ) );
                            }
                        }
                        ContentInfoUtil util = new ContentInfoUtil();
                        ContentInfo info;
                        if ( o instanceof byte[] ) {
                            info = util.findMatch( (byte[]) o );
                        } else if ( o instanceof InputStream ) {
                            PushbackInputStream pbis = new PushbackInputStream( (InputStream) o, ContentInfoUtil.DEFAULT_READ_SIZE );
                            byte[] buffer = new byte[ContentInfoUtil.DEFAULT_READ_SIZE];
                            try {
                                pbis.read( buffer );
                                info = util.findMatch( buffer );
                                pbis.unread( buffer );
                                map.put( index, pbis );
                            } catch ( IOException e ) {
                                throw new InvalidParameterValueException( "Exception while trying to determine file content type", e );
                            }
                        } else if ( o instanceof File ) {
                            try {
                                info = util.findMatch( (File) o );
                            } catch ( IOException e ) {
                                throw new InvalidParameterValueException( "Exception while trying to determine file content type", e );
                            }
                        } else if ( o instanceof FileInputHandle ) {
                            try {
                                info = ((FileInputHandle) o).getContentType( util );
                            } catch ( IOException e ) {
                                throw new InvalidParameterValueException( "Exception while trying to determine file content type", e );
                            }
                        } else {
                            throw new InvalidParameterValueException( "Multimedia object in unexpected form " + o.getClass().getSimpleName() );
                        }
                        if ( info == null ) {
                            throw new InvalidParameterValueException( String.format( "The content type of the %s file could not be determined and is thus invalid", polyType ) );
                        }
                        ContentType[] imageTypes = new ContentType[]{ ContentType.APPLE_QUICKTIME_IMAGE, ContentType.BMP, ContentType.GIF, ContentType.JPEG, ContentType.JPEG_2000, ContentType.PBM, ContentType.PGM, ContentType.PNG, ContentType.PPM, ContentType.SVG, ContentType.TIFF };
                        ContentType[] videoTypes = new ContentType[]{ ContentType.APPLE_QUICKTIME_MOVIE, ContentType.AVI, ContentType.MNG, ContentType.MP4A, ContentType.MP4V, ContentType.VIDEO_MPEG };
                        ContentType[] soundTypes = new ContentType[]{ ContentType.AIFF, ContentType.AUDIO_MPEG, ContentType.MIDI, ContentType.REAL_AUDIO, ContentType.WAV };
                        switch ( polyType ) {
                            case IMAGE:
                                valid = Arrays.asList( imageTypes ).contains( info.getContentType() );
                                break;
                            case VIDEO:
                                valid = Arrays.asList( videoTypes ).contains( info.getContentType() );
                                break;
                            case SOUND:
                                valid = Arrays.asList( soundTypes ).contains( info.getContentType() );
                                break;
                            //case File:
                            //break;
                        }
                        if ( !valid ) {
                            throw new InvalidParameterValueException( String.format( "The %s file has the content type '%s' which is not valid for the %s PolyType", polyType.toString().toLowerCase(), info.getName(), polyType ) );
                        }
                        break;
                }
                if ( !valid ) {
                    break;
                }
            }
            if ( !valid ) {
                throw new InvalidParameterValueException( String.format( "Parameter value '%s' of type %s does not match the PolyType %s", o.toString(), o.getClass().getSimpleName(), polyType ) );
            }
            return super.visitDynamicParam( dynamicParam );
        }

    }


    static class InvalidParameterValueException extends RuntimeException {

        public InvalidParameterValueException( String message ) {
            super( message );
        }


        public InvalidParameterValueException( String message, Throwable cause ) {
            super( message, cause );
        }

    }

}
