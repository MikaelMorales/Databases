package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ColumnStore extends Store {
    private DBColumn[] database;
    private String filename;
    private String delimiter;
    public DataType[] schema;

    public ColumnStore(DataType[] schema, String filename, String delimiter) {
        database = new DBColumn[schema.length];
        this.schema = schema;
        this.filename = filename;
        this.delimiter = delimiter;
    }

    @Override
    public void load() {
        try {
            int numberLines = countLines(filename);
            Object[][] columns = new Object[schema.length][numberLines];
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line;
            int index = 0;
            while ((line = br.readLine()) != null) {
                String[] tuple = line.split(delimiter);
                checkNumberOfAttributes(tuple, schema);
                Object[] fields = parseDataWithType(tuple, schema);
                for (int i = 0; i < fields.length; i++) {
                    columns[i][index] = fields[i];
                }
                index++;
            }

            for (int i=0; i < columns.length; i++) {
                database[i] = new DBColumn(columns[i], schema[i]);
            }

            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public DBColumn[] getColumns(int[] columnsToGet) {
        DBColumn[] columns = new DBColumn[columnsToGet.length];
        int i = 0;
        for (Integer index : columnsToGet) {
            if (index < 0 || index >= database.length)
                throw new IllegalArgumentException("Invalid column number !");
            columns[i] = database[index];
            i++;
        }
        return columns;
    }
}
