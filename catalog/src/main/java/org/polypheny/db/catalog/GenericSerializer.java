package org.polypheny.db.catalog;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;


public class GenericSerializer<T extends Serializable> implements org.mapdb.Serializer<T>, Serializable {


    @Override
    public void serialize( @NotNull DataOutput2 dataOutput2, @NotNull T entry ) throws IOException {
        org.mapdb.Serializer.BYTE_ARRAY.serialize( dataOutput2, serialize( entry ) );

    }


    @Override
    public T deserialize( @NotNull DataInput2 dataInput2, int i ) throws IOException {
        return deserialize( org.mapdb.Serializer.BYTE_ARRAY.deserialize( dataInput2, i ) );
    }

    public static <T extends Serializable> byte[] serialize( T entry ) {
        // https://stackoverflow.com/questions/2836646/java-serializable-object-to-byte-array
        byte[] bytes = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream( bos );
            out.writeObject( entry );
            out.flush();
            bytes = bos.toByteArray();
            bos.close();
            out.close();
            return bytes;
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        return bytes;

    }


    // caller needs to handle casting for now
    public static <S extends Serializable> S deserialize( byte[] bytes ) {
        S object = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream( bytes );
            ObjectInput in = new ObjectInputStream( bis );
            object = (S) in.readObject();
            bis.close();
            in.close();
            return object;
        } catch ( IOException | ClassNotFoundException e ) {
            e.printStackTrace();
        }
        return object;

    }
}
