package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class VectorTests {
    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;
    DataType[] lineItemBigSchema;
    DataType[] orderBigSchema;

    ColumnStore colstoreData;
    ColumnStore colstoreOrder;
    ColumnStore colstoreLineItem;
    ColumnStore colstoreLineItemBig;
    ColumnStore colstoreOrderBig;

    private static final int VECTOR_SIZE = 3;
    private static final int VECTOR_SIZE_BIG = 10;

    private static final int MAX_VECTOR_SIZE = 15;

    @Before
    public void init()  {

        schema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT };

        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};

        orderBigSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
                DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING};

        lineItemBigSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
                DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
                DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING};

        colstoreData = new ColumnStore(schema, "input/data.csv", ",");
        colstoreData.load();
        colstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
        colstoreOrder.load();
        colstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        colstoreLineItem.load();

        colstoreLineItemBig = new ColumnStore(lineItemBigSchema, "input/lineitem_big.csv", "\\|");
        colstoreOrderBig = new ColumnStore(orderBigSchema, "input/orders_big.csv", "\\|");
    }

    @Test
    public void spTestData() {
		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreData, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                    DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];
            assertTrue(output == 3);
        }
    }

    @Test
    public void spTestOrder() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreOrder, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 6);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                    DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];

            assertTrue(output == 1);
        }
    }

    @Test
    public void spTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 3);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                    DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];

            assertTrue(output == 3);
        }
    }

    @Test
    public void joinTest1() {
		/*
		 * SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            for (int j = 1; j < MAX_VECTOR_SIZE; j++) {
                ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(colstoreOrder, i);
                ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, j);

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
                assertTrue(output == 3);
            }
        }
    }

    @Test
    public void joinTest2() {
		/*
		 * SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            for (int j = 1; j < MAX_VECTOR_SIZE; j++) {
                ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
                ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(colstoreOrder, j);

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
        }
    }

    @Test
    public void testGE() {
		/* SELECT COUNT(*) FROM data WHERE col0 >= 4 */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreData, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GE, 0, 4);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                    DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];
            assertTrue(output == 7);
        }
    }

    @Test
    public void testLE() {
		/* SELECT COUNT(*) FROM data WHERE col0 <= 6 */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreData, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.LE, 0, 6);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                    DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];

            assertTrue(output == 6);
        }
    }

    @Test
    public void testGT() {
		/* SELECT COUNT(*) FROM data WHERE col0 > 3 */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreData, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GT, 0, 3);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                    DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];

            assertTrue(output == 7);
        }
    }

    @Test
    public void testLT() {
		/* SELECT COUNT(*) FROM data WHERE col0 < 3 */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreData, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.LT, 0, 3);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                    DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];

            assertTrue(output == 2);
        }
    }

    @Test
    public void testNT() {
		/* SELECT COUNT(*) FROM data WHERE col0 <> 3 */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreData, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.NE, 0, 3);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                    DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];

            assertTrue(output == 9);
        }
    }

    @Test
    public void testDoubleMax() {
	    /* SELECT MAX(col4) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MAX, DataType.DOUBLE, 4);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            double output = result[0].getAsDouble()[0];

            assertTrue(output == 49);
        }
    }

    @Test
    public void testDoubleMin() {
	    /* SELECT MIN(col4) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MIN, DataType.DOUBLE, 4);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            double output = result[0].getAsDouble()[0];

            assertTrue(output == 8);
        }
    }

    @Test
    public void testDoubleAvg() {
	    /* SELECT AVG(col4) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 4);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            double output = result[0].getAsDouble()[0];

            assertTrue(output == 30.4);
        }
    }

    @Test
    public void testDoubleSum() {
	    /* SELECT SUM(col4) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.SUM, DataType.DOUBLE, 4);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            double output = result[0].getAsDouble()[0];
            assertTrue(output == 304);
        }
    }

    @Test
    public void testIntMax() {
	    /* SELECT MAX(col3) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MAX, DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];
            assertTrue(output == 76910);
        }
    }

    @Test
    public void testIntMin() {
	    /* SELECT MIN(col3) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MIN, DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];
            assertTrue(output == 6348);
        }
    }

    @Test
    public void testIntAvg() {
	    /* SELECT AVG(col3) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            double output = result[0].getAsDouble()[0];
            assertTrue(output == 38449.6);
        }
    }

    @Test
    public void testIntSum() {
	    /* SELECT SUM(col3) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, 2);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];
            assertTrue(output == 384496);
        }
    }

    @Test
    public void testIntSelect() {
	    /* SELECT col3 FROM data WHERE col0 = 3*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreData, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 3);

            ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(sel, new int[]{3});

            proj.open();
            DBColumn[] result = proj.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];
            assertTrue(output == 4);
        }
    }

    @Test
    public void testDoubleSelect() {
	    /* SELECT col5 FROM lineitem WHERE col1 = 1284483*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 1, 1284483);

            ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(sel, new int[]{5});

            proj.open();
            DBColumn[] result = proj.next();

            // This query should return only one result
            double output = result[0].getAsDouble()[0];
            assertTrue(output == 39620.34);
        }
    }

    @Test
    public void testStringSelect() {
	    /* SELECT col5 FROM lineitem WHERE col1 = 1284483*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 1, 1284483);

            ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(sel, new int[]{15});

            proj.open();
            DBColumn[] result = proj.next();

            // This query should return only one result
            String output = result[0].getAsString()[0];
            assertTrue("nal foxes wake.".equals(output));
        }
    }

    @Test
    public void bigJoinTest1() {
		 /* SELECT COUNT(*) FROM order_big JOIN lineitem_big ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;*/
        colstoreOrderBig.load();
        colstoreLineItemBig.load();

        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(colstoreOrderBig, VECTOR_SIZE_BIG);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(colstoreLineItemBig, VECTOR_SIZE_BIG);

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
    public void bigJoinTest2() {
		 /* SELECT COUNT(*) FROM lineitem_big JOIN order_big ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3; */
        colstoreOrderBig.load();
        colstoreLineItemBig.load();

        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(colstoreOrderBig, VECTOR_SIZE_BIG);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(colstoreLineItemBig, VECTOR_SIZE_BIG);

		/* Filtering on both sides */
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ, 0, 3);

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
    public void testDoubleSumWithIntRes(){
	    /* SELECT SUM(col4) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, 4);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];
            assertTrue(output == 304);
        }
    }

    @Test
    public void testDoubleSum2(){
	    /* SELECT SUM(col4) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.SUM, DataType.DOUBLE, 5);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            double output = result[0].getAsDouble()[0];
            assertTrue(Math.ceil(output) == 454891); // should be equal to 454890.8
        }
    }

    @Test
    public void testDoubleSumWithIntRes2(){
	    /* SELECT SUM(col4) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, 5);

            agg.open();
            DBColumn[] result = agg.next();

            // This query should return only one result
            int output = result[0].getAsInteger()[0];
            assertTrue(output == 454890);
        }
    }

    @Test
    public void testDoubleAvgIntRes(){
	    /* SELECT AVG(col4) FROM data*/
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.AVG, DataType.INT, 4);

            agg.open();
            DBColumn[] result = agg.next();
            int output = result[0].getAsInteger()[0];
            assertTrue(output == 30);
        }
    }

    @Test
    public void testTooSelective() {
        /* SELECT COUNT(*) FROM data where col0 = 15; */
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scanData = new ch.epfl.dias.ops.vector.Scan(colstoreData, i);

            ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scanData, BinaryOp.EQ, 0, 15);

            ch.epfl.dias.ops.vector.ProjectAggregate agg = new  ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 0);

            agg.open();
            //This query should return only one result
            DBColumn[] result = agg.next();
            int output = result[0].getAsInteger()[0];
            assertTrue(output == 0);
        }
    }

    @Test
    public void testJoinOrder1() {
        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(colstoreOrder, 1);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, 1);

        /*Filtering on both sides */
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ, 0, 3);

        ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selOrder, selLineitem, 0, 0);
        ch.epfl.dias.ops.vector.Project project = new ch.epfl.dias.ops.vector.Project(join, new int[]{8, 24});
        project.open();

        DBColumn[] result = project.next();
        assertTrue("sly final accounts boost. carefully regular ideas cajole carefully. depos".equals(result[0].getAsString()[0]));
        assertTrue("ongside of the furiously brave acco".equals(result[1].getAsString()[0]));

        result = project.next();
        assertTrue("sly final accounts boost. carefully regular ideas cajole carefully. depos".equals(result[0].getAsString()[0]));
        assertTrue(" unusual accounts. eve".equals(result[1].getAsString()[0]));

        result = project.next();
        assertTrue("sly final accounts boost. carefully regular ideas cajole carefully. depos".equals(result[0].getAsString()[0]));
        assertTrue("nal foxes wake.".equals(result[1].getAsString()[0]));

        result = project.next();
        assertTrue(result[0].eof);
    }

    @Test
    public void testJoinOrder2() {
        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(colstoreOrder, 1);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, 1);

        /*Filtering on both sides */
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ, 0, 3);

        ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selLineitem, selOrder, 0, 0);
        Project project = new Project(join, new int[]{15, 24});
        project.open();
        DBColumn[] result = project.next();
        assertTrue("ongside of the furiously brave acco".equals(result[0].getAsString()[0]));
        assertTrue("sly final accounts boost. carefully regular ideas cajole carefully. depos".equals(result[1].getAsString()[0]));

        assertTrue(" unusual accounts. eve".equals(result[0].getAsString()[1]));
        assertTrue("sly final accounts boost. carefully regular ideas cajole carefully. depos".equals(result[1].getAsString()[1]));

        assertTrue("nal foxes wake.".equals(result[0].getAsString()[2]));
        assertTrue("sly final accounts boost. carefully regular ideas cajole carefully. depos".equals(result[1].getAsString()[2]));

        result = project.next();
        assertTrue(result[0].eof);
    }

    @Test
    public void testMinString() {
       for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MIN, DataType.STRING, 8);

            agg.open();
            DBColumn[] result = agg.next();

            String output = result[0].getAsString()[0];
            assertTrue("A".equals(output));
        }
    }

    @Test
    public void testMaxString() {
        for (int i = 1; i < MAX_VECTOR_SIZE; i++) {
            ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(colstoreLineItem, i);
            ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MAX, DataType.STRING, 8);

            agg.open();
            DBColumn[] result = agg.next();

            String output = result[0].getAsString()[0];
            assertTrue("R".equals(output));
        }
    }
}