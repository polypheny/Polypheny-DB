package ch.unibas.dmi.dbis.polyphenydb.config;


public class ConfigTable extends Config {

    private ConfigScalar[][] table;

    public ConfigTable ( String key, ConfigScalar[][] table ) {
        this.key = key;
        this.table = table;
    }

    public ConfigTable ( String key, String description, ConfigScalar[][] table ) {
        this.key = key;
        this.description = description;
        this.table = table;
    }

    @Override
    public void setObject( Object value ) {
        this.table = ( ConfigScalar[][] ) value;
        notifyConfigListeners( this );
    }

    @Override
    public int[][] getIntTable() {
        int[][] out = new int[table.length][table[0].length];
        for ( int i = 0; i < out.length; i++ ) {
            for ( int j = 0; j < out[0].length; j++) {
                out[i][j] = this.table[i][j].getInt();
            }
        }
        return out;
    }

    @Override
    public double[][] getDoubleTable () {
        double[][] out = new double[table.length][table[0].length];
        for ( int i = 0; i < out.length; i++ ) {
            for ( int j = 0; j < out[0].length; j++) {
                out[i][j] = this.table[i][j].getDouble();
            }
        }
        return out;
    }

}
