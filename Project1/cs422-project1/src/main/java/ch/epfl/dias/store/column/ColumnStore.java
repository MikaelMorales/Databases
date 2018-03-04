package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ColumnStore extends Store {
	private DBColumn[] database;
	private DataType[] schema;
	private String filename;
	private String delimiter;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		database = new DBColumn[schema.length+1];
		this.schema = schema.clone();
		this.filename = filename;
		this.delimiter = delimiter;
	}

	@Override
	public void load() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = br.readLine();
			String[] row = line.split(delimiter);
			checkNumberOfAttributes(row, schema);

			for (int i = 0; i < row.length; i++) {
				database[i] = new DBColumn(schema[i]);
			}

			addData(row); // First row

			while ((line = br.readLine()) != null) {
				row = line.split(delimiter);
				addData(row);
			}

			database[database.length-1] = new DBColumn(); //EOF

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
				throw new IllegalArgumentException("Invalid row number !");
			columns[i] = database[index];
			i++;
		}
		return columns;
	}

	private void addData(String[] row) throws IOException {
		checkNumberOfAttributes(row, schema);
		for (int i=0; i < row.length; i++) {
			switch(schema[i]) {
				case INT: {
					database[i].append(Integer.parseInt(row[i]));
					break;
				}
				case STRING: {
					database[i].append(row[i]);
					break;
				}
				case DOUBLE: {
					database[i].append(Double.parseDouble(row[i]));
					break;
				}
				case BOOLEAN: {
					database[i].append(Boolean.parseBoolean(row[i]));
					break;
				}
				default: throw new IllegalArgumentException("Invalid type");
			}
		}
	}
}
