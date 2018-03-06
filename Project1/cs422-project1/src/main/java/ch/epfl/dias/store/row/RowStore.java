package ch.epfl.dias.store.row;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class RowStore extends Store {
	private DBTuple[] database;
	private DataType[] schema;
	private String filename;
	private String delimiter;

	public RowStore(DataType[] schema, String filename, String delimiter) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
	}

	@Override
	public void load() {
		try {
			int numberLines = countLines(filename);
			database = new DBTuple[numberLines+1];
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line;
			int index = 0;
			while ((line = br.readLine()) != null) {
				String[] tuple = line.split(delimiter);
				checkNumberOfAttributes(tuple, schema);
				Object[] fields = parseDataWithType(tuple, schema);
				database[index] = new DBTuple(fields, schema);
				index++;
			}
			database[index] = new DBTuple(); // EOF

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {
		if (rownumber < 0 || rownumber >= database.length)
			throw new IllegalArgumentException("Invalid row number !");
		return database[rownumber];
	}
}
