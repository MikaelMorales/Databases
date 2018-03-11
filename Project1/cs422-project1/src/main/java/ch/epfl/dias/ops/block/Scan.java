package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import java.util.stream.IntStream;

public class Scan implements BlockOperator {

	private ColumnStore database;

	public Scan(ColumnStore store) {
		this.database = store;
	}

	@Override
	public DBColumn[] execute() {
		int[] a = IntStream.range(0, database.schema.length+1).toArray();
		return database.getColumns(a);
	}
}
