package ch.unibas.dmi.dbis.polyphenydb.config;


public class ConfigArray extends Config {

    private ConfigScalar[] array;

    public ConfigArray ( String key, ConfigScalar[] array) {
        this.key = key;
        this.array = array;
    }

    public ConfigArray ( String key, String description, ConfigScalar[] array ) {
        this.key = key;
        this.description = description;
        this.array = array;
    }

    @Override
    public void setObject( Object value ) {
        this.array = ( ConfigScalar[] ) value;
        notifyConfigListeners( this );
    }

    @Override
    public int[] getIntArray() {
        int[] out = new int[array.length];
        for ( int i = 0; i < out.length; i++ ) {
            out[i] = array[i].getInt();
        }
        return out;
    }

    @Override
    public double[] getDoubleArray() {
        double[] out = new double[array.length];
        for ( int i = 0; i < out.length; i++ ) {
            out[i] = array[i].getDouble();
        }
        return out;
    }

}
