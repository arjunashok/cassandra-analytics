package org.apache.cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static junit.framework.TestCase.assertTrue;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Base class for resiliency tests. Contains helper methods for data generation and validation
 */
public abstract class ResiliencyTestBase extends IntegrationTestBase
{
    private static final String createTableStmt = "create table if not exists %s (id int, course text, marks int, primary key (id));";
    private static final String retrieveRows = "select * from " + TEST_KEYSPACE + ".%s";
    private static final int rowCount = 1000;

    public QualifiedTableName initializeSchema(Session session)
    {
        return initializeSchema(session, ImmutableMap.of("datacenter1", 1));
    }

    public QualifiedTableName initializeSchema(Session session, Map<String, Integer> rf)
    {
        createTestKeyspace(rf);
        return createTestTable(createTableStmt);
    }

    public SparkConf generateSparkConf()
    {
        SparkConf sparkConf = new SparkConf()
                              .setAppName("Integration test Spark Cassandra Bulk Reader Job")
                              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                              .set("spark.master", "local[8]");
        BulkSparkConf.setupSparkConf(sparkConf, true);
        KryoRegister.setup(sparkConf);
        return sparkConf;
    }

    public SparkSession generateSparkSession(SparkConf sparkConf)
    {
        return SparkSession.builder()
                           .config(sparkConf)
                           .getOrCreate();
    }

    public Dataset<org.apache.spark.sql.Row> generateData(SparkSession spark)
    {
        SQLContext sql = spark.sqlContext();
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("course", StringType, false)
                            .add("marks", IntegerType, false);

        List<org.apache.spark.sql.Row> rows = IntStream.range(0, rowCount)
                                                       .mapToObj(recordNum -> {
                                                           String course = "course" + recordNum;
                                                           ArrayList<Object> values = new ArrayList<>(Arrays.asList(recordNum, course, recordNum));
                                                           return RowFactory.create(values.toArray());
                                                       }).collect(Collectors.toList());
        return sql.createDataFrame(rows, schema);
    }

    public void validateData(Session session, String tableName)
    {
        ResultSet resultSet = session.execute(String.format(retrieveRows, tableName));
        Set<String> rows = new HashSet<>();
        for (Row row : resultSet.all())
        {
            if (row.isNull("id") || row.isNull("course") || row.isNull("marks"))
            {
                throw new RuntimeException("Unrecognized row in table");
            }

            int id = row.getInt("id");
            String course = row.getString("course");
            int marks = row.getInt("marks");
            rows.add(id + ":" + course + ":" + marks);
        }

        for (int i=0; i<rowCount; i++)
        {
            String expectedRow = i + ":course" + i + ":" + i;
            rows.remove(expectedRow);
        }
        assertTrue(rows.isEmpty());
    }
}
