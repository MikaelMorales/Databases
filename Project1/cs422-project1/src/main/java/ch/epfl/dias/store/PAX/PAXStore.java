package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PAXStore extends Store {
	private final List<DBPAXpage> database = new ArrayList<DBPAXpage>();
	private DataType[] schema;
	private String filename;
	private String delimiter;
	private int tuplesPerPage;

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.schema = schema.clone();
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
	}

	@Override
	public void load() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line;
			int currentPage = 0;
			int currentTuplePerPage = 0;
			while ((line = br.readLine()) != null) {
			    if (currentTuplePerPage == tuplesPerPage) { // Move to next page
			        currentPage++;
			        currentTuplePerPage = 0;
                }
                if (currentTuplePerPage == 0) { // Initialize empty page
			        database.add(new DBPAXpage(schema, tuplesPerPage));
                }

				String[] tuple = line.split(delimiter);
				addData(tuple, currentPage);
                currentTuplePerPage++;
            }

			database.add(new DBPAXpage()); // EOF
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public DBTuple getRow(int rowNumber) {
	    int pageNumber = rowNumber/tuplesPerPage;
        int realRowNumber = rowNumber % tuplesPerPage;
		if (pageNumber < 0 || pageNumber >= database.size())
			throw new IllegalArgumentException("Invalid row number !");

		Object[] tuple = database.get(pageNumber).getRow(realRowNumber);

		return tuple == null ? new DBTuple() : new DBTuple(tuple, schema);
	}

	private void addData(String[] tuple, int currentPage) throws IOException {
        checkNumberOfAttributes(tuple, schema);
        Object[] fields = parseDataWithType(tuple, schema);
        database.get(currentPage).append(fields);
    }
}
