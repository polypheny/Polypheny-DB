package ch.unibas.dmi.dbis.polyphenydb.config;


public class ConfigTable extends Config {

    private ConfigScalar[][] table;

    public ConfigTable ( String key ) {
        super( key );
    }

    public ConfigTable ( String key, String description ) {
        super( key, description );
    }

    @Override
    public void setObject( Object value ) {
        this.table = ( ConfigScalar[][] ) value;
        notifyConfigListeners( this );
    }

    @Override
    public Integer[][] getIntegerTable () {
        Integer[][] out = new Integer[table.length][table[0].length];
        for ( int i = 0; i < out.length; i++ ) {
            for ( int j = 0; j < out[0].length; j++) {
                out[i][j] = this.table[i][j].getInt();
            }
        }
        return out;
    }

    @Override
    public Double[][] getDoubleTable () {
        Double[][] out = new Double[table.length][table[0].length];
        for ( int i = 0; i < out.length; i++ ) {
            for ( int j = 0; j < out[0].length; j++) {
                out[i][j] = this.table[i][j].getDouble();
            }
        }
        return out;
    }

}
