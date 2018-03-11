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
		DBColumn[] filtered = new DBColumn[columns.length+1];
		for (int i=0; i < columns.length; i++) {
			filtered[i] = c[i];
		}
		filtered[filtered.length-1] = new DBColumn();
		return filtered;
	}
}
