package ch.epfl.dias.store.row;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RowStore extends Store {
    private final List<DBTuple> database = new ArrayList<DBTuple>();
    private DataType[] schema;
    private String filename;
    private String delimiter;

	public RowStore(DataType[] schema, String filename, String delimiter) {
		this.schema = schema.clone();
		this.filename = filename;
		this.delimiter = delimiter;
	}

	@Override
	public void load() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tuple = line.split(delimiter);
                addData(tuple);
            }
            database.add(new DBTuple()); // EOF
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

	@Override
	public DBTuple getRow(int rownumber) {
		if (rownumber < 0 || rownumber >= database.size())
		    throw new IllegalArgumentException("Invalid row number !");
		return database.get(rownumber);
	}

	private void addData(String[] tuple) throws IOException {
        checkNumberOfAttributes(tuple, schema);
        Object[] fields = parseDataWithType(tuple, schema);
        database.add(new DBTuple(fields, schema));
    }
}
