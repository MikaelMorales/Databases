package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements VectorOperator {

	private VectorOperator child;
	private int[] fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] c = child.next();
		DBColumn[] filtered = new DBColumn[fieldNo.length];
		for (int i=0; i < fieldNo.length; i++) {
			filtered[i] = c[fieldNo[i]];
			if (c[fieldNo[i]].eof)
				filtered[i].setEOF(true);
		}
		return filtered;
	}

	@Override
	public void close() {
		child.close();
	}
}
