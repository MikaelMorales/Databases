package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

import java.util.Arrays;

public class Scan implements VectorOperator {

	private Store database;
	private int vectorSize;
	private DBColumn[] columns;
	private int currentRow;

	public Scan(Store store, int vectorSize) {
		if (vectorSize == 0)
			throw new IllegalArgumentException("Vector size can't be equal to 0 !");

		this.database = store;
		this.vectorSize = vectorSize;
	}
	
	@Override
	public void open() {
		database.load();
		currentRow = 0;
		columns = database.getColumns(null);
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] result = getSubColumns(currentRow, currentRow+vectorSize, columns);
		currentRow += vectorSize;
		return result;
	}

	@Override
	public void close() {
		database = null;
		currentRow = 0;
	}

	private DBColumn[] getSubColumns(int from, int to, DBColumn[] columns) {
		DBColumn[] newColumns = new DBColumn[columns.length];
		for (int i=0; i < columns.length; i++) {
			if (from >= columns[i].attributes.length) {
				newColumns[i] = new DBColumn();
			} else {
				to = to > columns[i].attributes.length ? columns[i].attributes.length : to;
				Object[] newArray = Arrays.copyOfRange(columns[i].attributes, from, to);
				newColumns[i] = new DBColumn(newArray, columns[i].type);
			}
		}
		return newColumns;
	}
}
