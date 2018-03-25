package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PAXStore extends Store {
	private DBPAXpage[] database;
	private DataType[] schema;
	private String filename;
	private String delimiter;
	private int tuplesPerPage;

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
	}

	@Override
	public void load() {
		try {
			int numberLines = countLines(filename);
			int numberPages = (int) Math.ceil(numberLines/(double)tuplesPerPage);
			database = new DBPAXpage[numberPages+1];

			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line;
			int currentPage = 0;
			int currentTuplePerPage = 0;
			Object[][] paxPage = new Object[schema.length][tuplesPerPage];
			String[] tuple;
			Object[] fields;

			while ((line = br.readLine()) != null) {
				if (currentTuplePerPage == tuplesPerPage) { // Move to next page
					database[currentPage] = new DBPAXpage(paxPage, schema, currentTuplePerPage);
					currentPage++;
					currentTuplePerPage = 0;
					paxPage = new Object[schema.length][tuplesPerPage]; // Prepare next PAX Page
				}

				tuple = line.split(delimiter);
				checkNumberOfAttributes(tuple, schema);
				fields = parseDataWithType(tuple, schema);
				for (int attribute=0; attribute < fields.length; attribute++) {
					paxPage[attribute][currentTuplePerPage] = fields[attribute];
				}
				currentTuplePerPage++;
			}

			if (currentTuplePerPage != 0) { // The last page might not be entirely filled
				database[currentPage] = new DBPAXpage(paxPage, schema, currentTuplePerPage);
				currentPage++;
			}

			database[currentPage] = new DBPAXpage(); // EOF

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public DBTuple getRow(int rowNumber) {
		int pageNumber = rowNumber/tuplesPerPage;
		int realRowNumber = rowNumber % tuplesPerPage;
		if (pageNumber < 0 || pageNumber >= database.length)
			throw new IllegalArgumentException("Invalid row number !");

		Object[] tuple = database[pageNumber].getRow(realRowNumber);

		return tuple == null ? new DBTuple() : new DBTuple(tuple, schema);
	}
}
