package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	private Store database;
	private int currentLine;

	public Scan(Store store) {
		database = store;
	}

	@Override
	public void open() {
		currentLine = 0;
		database.load();
	}

	@Override
	public DBTuple next() {
		DBTuple row = database.getRow(currentLine);
		currentLine++;
		return row;
	}

	@Override
	public void close() {
		database = null;
	}
}