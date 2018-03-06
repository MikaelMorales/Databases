package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;

public class DBPAXpage {

    public Object[][] page;
    public DataType[] types;
    public int endOfMinipage;
    public boolean eof;

    public DBPAXpage(Object[][] page, DataType[] types, int endOfMinipage) {
        this.page = page.clone();
        this.types = types;
        this.endOfMinipage = endOfMinipage;
        this.eof = false;
    }

    public DBPAXpage() {
        this.eof = true;
    }

    public Object[] getRow(int rowNumber) {
        if (rowNumber > endOfMinipage) {
            throw new IllegalArgumentException("Invalid row number !");
        } else if (rowNumber == endOfMinipage) {
            return null;
        }

        Object[] row = new Object[page.length];
        for (int i=0; i < page.length; i++) {
            row[i] = page[i][rowNumber];
        }

        return row;
    }
}
