package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;

public class DBPAXpage {

    public Object[][] fields;
    public DataType[] types;
    public int endOfMinipage;
    public boolean eof;

    public DBPAXpage(DataType[] types, int tuplePerPage) {
        this.fields = new Object[types.length][tuplePerPage];
        this.types = types;
        this.eof = false;
    }

    public DBPAXpage() {
        this.eof = true;
    }

    public void append(Object[] o) {
        for (int i=0; i < o.length; i++) {
            fields[i][endOfMinipage] = o[i];
        }
        endOfMinipage++;
    }

    public Object[] getRow(int rowNumber) {
        if (rowNumber > endOfMinipage) {
            throw new IllegalArgumentException("Invalid row number !");
        } else if (rowNumber == endOfMinipage) {
            return null;
        }

        Object[] row = new Object[fields.length];
        for (int i=0; i < fields.length; i++) {
            row[i] = fields[i][rowNumber];
        }

        return row;
    }
}
