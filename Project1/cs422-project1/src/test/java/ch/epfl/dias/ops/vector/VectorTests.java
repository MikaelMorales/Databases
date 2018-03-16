package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author Morales Mikael <mikael@pocketcampus.org>
 */
public class VectorTests {
    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;

    ColumnStore columnstoreData;
    ColumnStore columnstoreOrder;
    ColumnStore columnstoreLineItem;

    ColumnStore columnstoreBigOrder;
    ColumnStore columnstoreBigLineItem;

    int smallDataVectorSize = 2;
    int bigDataVectorSize = 20;

    @Before
    public void init() {

        schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
                DataType.INT, DataType.INT, DataType.INT, DataType.INT };

        orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
                DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

        lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
                DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
                DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

        columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        columnstoreBigLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
        columnstoreBigOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
        columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
        columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
    }

    @Test
    public void spTestData() {
		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 3);
    }

    @Test
    public void spTestOrder() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 6);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 1);
    }

    @Test
    public void spTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 3);
    }

    @Test
    public void joinTest1() {
		/*
		 * SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);

		/* Filtering on both sides */
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ, 0, 3);

        ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selOrder, selLineitem, 0, 0);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT,
                DataType.INT, 0);

        agg.open();
        DBColumn[] result = join.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 3);
    }

    @Test
    public void joinTest2() {
		/*
		 * SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);

		/* Filtering on both sides */
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ, 0, 3);

        ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selLineitem, selOrder, 0, 0);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT,
                DataType.INT, 0);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 3);
    }

    @Test
    public void testGE() {
		/* SELECT COUNT(*) FROM data WHERE col0 >= 4 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GE, 0, 4);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 7);
    }

    @Test
    public void testLE() {
		/* SELECT COUNT(*) FROM data WHERE col0 <= 6 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.LE, 0, 6);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 6);
    }

    @Test
    public void testGT() {
		/* SELECT COUNT(*) FROM data WHERE col0 > 3 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GT, 0, 3);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 7);
    }

    @Test
    public void testLT() {
		/* SELECT COUNT(*) FROM data WHERE col0 < 3 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.LT, 0, 3);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 2);
    }

    @Test
    public void testNT() {
		/* SELECT COUNT(*) FROM data WHERE col0 <> 3 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.NE, 0, 3);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 9);
    }

    @Test
    public void testDoubleMax(){
	    /* SELECT MAX(col4) FROM data*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MAX, DataType.DOUBLE, 4);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        double output = result[0].getAsDouble()[0];

        assertTrue(output == 49);
    }

    @Test
    public void testDoubleMin(){
	    /* SELECT MIN(col4) FROM data*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MIN, DataType.DOUBLE, 4);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        double output = result[0].getAsDouble()[0];
        assertTrue(output == 8);
    }

    @Test
    public void testDoubleAvg(){
	    /* SELECT AVG(col4) FROM data*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 4);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        double output = result[0].getAsDouble()[0];

        assertTrue(output == 30.4);
    }

    @Test
    public void testDoubleSum(){
	    /* SELECT SUM(col4) FROM data*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.SUM, DataType.DOUBLE, 4);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        double output = result[0].getAsDouble()[0];
        assertTrue(output == 304);
    }

    @Test
    public void testIntMax(){
	    /* SELECT MAX(col3) FROM data*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MAX, DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 76910);
    }

    @Test
    public void testIntMin(){
	    /* SELECT MIN(col3) FROM data*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MIN, DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 6348);
    }

    @Test
    public void testIntAvg(){
	    /* SELECT AVG(col3) FROM data*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        double output = result[0].getAsDouble()[0];
        assertTrue(output == 38449.6);
    }

    @Test
    public void testIntSum(){
	    /* SELECT SUM(col3) FROM data*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 384496);
    }

    @Test
    public void testIntSelect(){
	    /* SELECT col3 FROM data WHERE col0 = 3*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 3);

        ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(sel, new int[]{3});

        proj.open();
        DBColumn[] result = proj.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 4);
    }

    @Test
    public void testDoubleSelect(){
	    /* SELECT col5 FROM lineitem WHERE col1 = 1284483*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 1, 1284483);

        ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(sel, new int[]{5});

        proj.open();
        DBColumn[] result = proj.next();

        // This query should return only one result
        double output = result[0].getAsDouble()[0];
        assertTrue(output == 39620.34);
    }

    @Test
    public void testStringSelect(){
	    /* SELECT col5 FROM lineitem WHERE col1 = 1284483*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 1, 1284483);

        ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(sel, new int[]{15});

        proj.open();
        DBColumn[] result = proj.next();

        // This query should return only one result
        String output = result[0].getAsString()[0];
        assertTrue("nal foxes wake.".equals(output));
    }

    @Test
    public void joinTestBig1() {
		/*
		 * SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */
        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreBigOrder, bigDataVectorSize);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreBigLineItem, bigDataVectorSize);

		/* Filtering on both sides */
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ, 0, 3);

        ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selOrder, selLineitem, 0, 0);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT,
                DataType.INT, 0);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 6);
    }

    @Test
    public void joinTestBig2() {
		/*
		 * SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */
        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreBigOrder, bigDataVectorSize);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreBigLineItem, bigDataVectorSize);

		/* Filtering on both sides */
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ, 0, 3);

        ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selLineitem, selOrder, 0, 0);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT,
                DataType.INT, 0);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 6);
    }

    @Test
    public void testDoubleAvgIntRes(){
        /* SELECT AVG(col4) FROM data*/
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, smallDataVectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.AVG, DataType.INT, 4);

        agg.open();
        DBColumn[] result = agg.next();
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 30);
    }
}
