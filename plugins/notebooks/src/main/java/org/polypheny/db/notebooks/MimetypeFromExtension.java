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

package org.polypheny.db.notebooks;

import java.util.HashMap;
import java.util.Map;

public class MimetypeFromExtension {

    private static final MimetypeFromExtension INSTANCE = new MimetypeFromExtension();
    private final Map<String, String> extensions = new HashMap<>();


    private MimetypeFromExtension() {
        // From https://sqlite.org/althttpd/file?ci=tip&name=althttpd.c&ln=1665-1864
        extensions.put( "ai", "application/postscript" );
        extensions.put( "aif", "audio/x-aiff" );
        extensions.put( "aifc", "audio/x-aiff" );
        extensions.put( "aiff", "audio/x-aiff" );
        extensions.put( "arj", "application/x-arj-compressed" );
        extensions.put( "asc", "text/plain" );
        extensions.put( "asf", "video/x-ms-asf" );
        extensions.put( "asx", "video/x-ms-asx" );
        extensions.put( "au", "audio/ulaw" );
        extensions.put( "avi", "video/x-msvideo" );
        extensions.put( "bat", "application/x-msdos-program" );
        extensions.put( "bcpio", "application/x-bcpio" );
        extensions.put( "bin", "application/octet-stream" );
        extensions.put( "c", "text/plain" );
        extensions.put( "cc", "text/plain" );
        extensions.put( "ccad", "application/clariscad" );
        extensions.put( "cdf", "application/x-netcdf" );
        extensions.put( "class", "application/octet-stream" );
        extensions.put( "cod", "application/vnd.rim.cod" );
        extensions.put( "com", "application/x-msdos-program" );
        extensions.put( "cpio", "application/x-cpio" );
        extensions.put( "cpt", "application/mac-compactpro" );
        extensions.put( "csh", "application/x-csh" );
        extensions.put( "css", "text/css" );
        extensions.put( "dcr", "application/x-director" );
        extensions.put( "deb", "application/x-debian-package" );
        extensions.put( "dir", "application/x-director" );
        extensions.put( "dl", "video/dl" );
        extensions.put( "dms", "application/octet-stream" );
        extensions.put( "doc", "application/msword" );
        extensions.put( "drw", "application/drafting" );
        extensions.put( "dvi", "application/x-dvi" );
        extensions.put( "dwg", "application/acad" );
        extensions.put( "dxf", "application/dxf" );
        extensions.put( "dxr", "application/x-director" );
        extensions.put( "eps", "application/postscript" );
        extensions.put( "etx", "text/x-setext" );
        extensions.put( "exe", "application/octet-stream" );
        extensions.put( "ez", "application/andrew-inset" );
        extensions.put( "f", "text/plain" );
        extensions.put( "f90", "text/plain" );
        extensions.put( "fli", "video/fli" );
        extensions.put( "flv", "video/flv" );
        extensions.put( "gif", "image/gif" );
        extensions.put( "gl", "video/gl" );
        extensions.put( "gtar", "application/x-gtar" );
        extensions.put( "gz", "application/x-gzip" );
        extensions.put( "hdf", "application/x-hdf" );
        extensions.put( "hh", "text/plain" );
        extensions.put( "hqx", "application/mac-binhex40" );
        extensions.put( "h", "text/plain" );
        extensions.put( "htm", "text/html" );
        extensions.put( "html", "text/html" );
        extensions.put( "ice", "x-conference/x-cooltalk" );
        extensions.put( "ief", "image/ief" );
        extensions.put( "iges", "model/iges" );
        extensions.put( "igs", "model/iges" );
        extensions.put( "ips", "application/x-ipscript" );
        extensions.put( "ipx", "application/x-ipix" );
        extensions.put( "jad", "text/vnd.sun.j2me.app-descriptor" );
        extensions.put( "jar", "application/java-archive" );
        extensions.put( "jpeg", "image/jpeg" );
        extensions.put( "jpe", "image/jpeg" );
        extensions.put( "jpg", "image/jpeg" );
        extensions.put( "js", "text/x-javascript" );
        extensions.put( "json", "application/json" );
        extensions.put( "kar", "audio/midi" );
        extensions.put( "latex", "application/x-latex" );
        extensions.put( "lha", "application/octet-stream" );
        extensions.put( "lsp", "application/x-lisp" );
        extensions.put( "lzh", "application/octet-stream" );
        extensions.put( "m", "text/plain" );
        extensions.put( "m3u", "audio/x-mpegurl" );
        extensions.put( "man", "application/x-troff-man" );
        extensions.put( "me", "application/x-troff-me" );
        extensions.put( "mesh", "model/mesh" );
        extensions.put( "mid", "audio/midi" );
        extensions.put( "midi", "audio/midi" );
        extensions.put( "mif", "application/x-mif" );
        extensions.put( "mime", "www/mime" );
        extensions.put( "mjs", "text/javascript" /*ES6 module*/ );
        extensions.put( "movie", "video/x-sgi-movie" );
        extensions.put( "mov", "video/quicktime" );
        extensions.put( "mp2", "audio/mpeg" );
        extensions.put( "mp3", "audio/mpeg" );
        extensions.put( "mpeg", "video/mpeg" );
        extensions.put( "mpe", "video/mpeg" );
        extensions.put( "mpga", "audio/mpeg" );
        extensions.put( "mpg", "video/mpeg" );
        extensions.put( "ms", "application/x-troff-ms" );
        extensions.put( "msh", "model/mesh" );
        extensions.put( "nc", "application/x-netcdf" );
        extensions.put( "oda", "application/oda" );
        extensions.put( "ogg", "application/ogg" );
        extensions.put( "ogm", "application/ogg" );
        extensions.put( "pbm", "image/x-portable-bitmap" );
        extensions.put( "pdb", "chemical/x-pdb" );
        extensions.put( "pdf", "application/pdf" );
        extensions.put( "pgm", "image/x-portable-graymap" );
        extensions.put( "pgn", "application/x-chess-pgn" );
        extensions.put( "pgp", "application/pgp" );
        extensions.put( "pl", "application/x-perl" );
        extensions.put( "pm", "application/x-perl" );
        extensions.put( "png", "image/png" );
        extensions.put( "pnm", "image/x-portable-anymap" );
        extensions.put( "pot", "application/mspowerpoint" );
        extensions.put( "ppm", "image/x-portable-pixmap" );
        extensions.put( "pps", "application/mspowerpoint" );
        extensions.put( "ppt", "application/mspowerpoint" );
        extensions.put( "ppz", "application/mspowerpoint" );
        extensions.put( "pre", "application/x-freelance" );
        extensions.put( "prt", "application/pro_eng" );
        extensions.put( "ps", "application/postscript" );
        extensions.put( "qt", "video/quicktime" );
        extensions.put( "ra", "audio/x-realaudio" );
        extensions.put( "ram", "audio/x-pn-realaudio" );
        extensions.put( "rar", "application/x-rar-compressed" );
        extensions.put( "ras", "image/cmu-raster" );
        extensions.put( "rgb", "image/x-rgb" );
        extensions.put( "rm", "audio/x-pn-realaudio" );
        extensions.put( "roff", "application/x-troff" );
        extensions.put( "rpm", "audio/x-pn-realaudio-plugin" );
        extensions.put( "rtf", "text/rtf" );
        extensions.put( "rtx", "text/richtext" );
        extensions.put( "scm", "application/x-lotusscreencam" );
        extensions.put( "set", "application/set" );
        extensions.put( "sgml", "text/sgml" );
        extensions.put( "sgm", "text/sgml" );
        extensions.put( "sh", "application/x-sh" );
        extensions.put( "shar", "application/x-shar" );
        extensions.put( "silo", "model/mesh" );
        extensions.put( "sit", "application/x-stuffit" );
        extensions.put( "skd", "application/x-koan" );
        extensions.put( "skm", "application/x-koan" );
        extensions.put( "skp", "application/x-koan" );
        extensions.put( "skt", "application/x-koan" );
        extensions.put( "smi", "application/smil" );
        extensions.put( "smil", "application/smil" );
        extensions.put( "snd", "audio/basic" );
        extensions.put( "sol", "application/solids" );
        extensions.put( "spl", "application/x-futuresplash" );
        extensions.put( "src", "application/x-wais-source" );
        extensions.put( "step", "application/STEP" );
        extensions.put( "stl", "application/SLA" );
        extensions.put( "stp", "application/STEP" );
        extensions.put( "sv4cpio", "application/x-sv4cpio" );
        extensions.put( "sv4crc", "application/x-sv4crc" );
        extensions.put( "svg", "image/svg+xml" );
        extensions.put( "swf", "application/x-shockwave-flash" );
        extensions.put( "t", "application/x-troff" );
        extensions.put( "tar", "application/x-tar" );
        extensions.put( "tcl", "application/x-tcl" );
        extensions.put( "tex", "application/x-tex" );
        extensions.put( "texi", "application/x-texinfo" );
        extensions.put( "texinfo", "application/x-texinfo" );
        extensions.put( "tgz", "application/x-tar-gz" );
        extensions.put( "tiff", "image/tiff" );
        extensions.put( "tif", "image/tiff" );
        extensions.put( "tr", "application/x-troff" );
        extensions.put( "tsi", "audio/TSP-audio" );
        extensions.put( "tsp", "application/dsptype" );
        extensions.put( "tsv", "text/tab-separated-values" );
        extensions.put( "txt", "text/plain" );
        extensions.put( "unv", "application/i-deas" );
        extensions.put( "ustar", "application/x-ustar" );
        extensions.put( "vcd", "application/x-cdlink" );
        extensions.put( "vda", "application/vda" );
        extensions.put( "viv", "video/vnd.vivo" );
        extensions.put( "vivo", "video/vnd.vivo" );
        extensions.put( "vrml", "model/vrml" );
        extensions.put( "vsix", "application/vsix" );
        extensions.put( "wasm", "application/wasm" );
        extensions.put( "wav", "audio/x-wav" );
        extensions.put( "wax", "audio/x-ms-wax" );
        extensions.put( "wiki", "application/x-fossil-wiki" );
        extensions.put( "wma", "audio/x-ms-wma" );
        extensions.put( "wmv", "video/x-ms-wmv" );
        extensions.put( "wmx", "video/x-ms-wmx" );
        extensions.put( "wrl", "model/vrml" );
        extensions.put( "wvx", "video/x-ms-wvx" );
        extensions.put( "xbm", "image/x-xbitmap" );
        extensions.put( "xlc", "application/vnd.ms-excel" );
        extensions.put( "xll", "application/vnd.ms-excel" );
        extensions.put( "xlm", "application/vnd.ms-excel" );
        extensions.put( "xls", "application/vnd.ms-excel" );
        extensions.put( "xlw", "application/vnd.ms-excel" );
        extensions.put( "xml", "text/xml" );
        extensions.put( "xpm", "image/x-xpixmap" );
        extensions.put( "xwd", "image/x-xwindowdump" );
        extensions.put( "xyz", "chemical/x-pdb" );
        extensions.put( "zip", "application/zip" );
    }


    public static String guessMimetype( String filename ) {
        String[] parts = filename.split( "\\." );
        return INSTANCE.extensions.getOrDefault( parts[parts.length - 1], "application/octet-stream" );
    }

}
