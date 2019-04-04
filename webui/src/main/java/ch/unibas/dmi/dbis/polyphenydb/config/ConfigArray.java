package ch.unibas.dmi.dbis.polyphenydb.config;


public class ConfigArray extends Config {

    private ConfigScalar[] array;

    public ConfigArray ( String key ) {
        super( key );
    }

    public ConfigArray ( String key, String description ) {
        super( key, description );
    }

    @Override
    public void setObject( Object value ) {
        this.array = ( ConfigScalar[] ) value;
    }

    @Override
    public Integer[] getIntegerArray() {
        Integer[] out = new Integer[array.length];
        for ( int i = 0; i < out.length; i++ ) {
            out[i] = array[i].getInt();
        }
        return out;
    }

    @Override
    public Double[] getDoubleArray() {
        Double[] out = new Double[array.length];
        for ( int i = 0; i < out.length; i++ ) {
            out[i] = array[i].getDouble();
        }
        return out;
    }

}
