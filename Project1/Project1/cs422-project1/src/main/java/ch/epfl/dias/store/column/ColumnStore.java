package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.IntStream;

public class ColumnStore extends Store {
    private DBColumn[] database;
    private String filename;
    private String delimiter;
    private DataType[] schema;

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
            String[] tuple;
            Object[] fields;
            int index = 0;
            while ((line = br.readLine()) != null) {
                tuple = line.split(delimiter);
                checkNumberOfAttributes(tuple, schema);
                fields = parseDataWithType(tuple, schema);
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
        if (columnsToGet == null) {
            columnsToGet = IntStream.range(0, schema.length).toArray();
        }

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
