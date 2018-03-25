package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements BlockOperator {

	private ColumnStore database;

	public Scan(ColumnStore store) {
		this.database = store;
	}

	@Override
	public DBColumn[] execute() {
		return database.getColumns(null);
	}
}
