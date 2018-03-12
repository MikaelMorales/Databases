package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements BlockOperator {

	private BlockOperator child;
	private int[] columns;

	public Project(BlockOperator child, int[] columns) {
		this.child = child;
		this.columns = columns;
	}

	public DBColumn[] execute() {
		DBColumn[] c = child.execute();
		DBColumn[] filtered = new DBColumn[columns.length];
		for (int i=0; i < columns.length; i++) {
			filtered[i] = c[columns[i]];
		}
		return filtered;
	}
}
