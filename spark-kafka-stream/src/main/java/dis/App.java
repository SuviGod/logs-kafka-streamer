package dis;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class App {
    private static final Logger logger = LogManager.getLogger(App.class);

    // Configuration parameters
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String KAFKA_TOPIC_KILLS = "game.kills";
    private static final String KAFKA_TOPIC_DAMAGES = "game.damages";
    private static final String CHECKPOINT_LOCATION = "./checkpoint";

    // PostgreSQL configuration
    private static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "admin";
    private static final String POSTGRES_DRIVER = "org.postgresql.Driver";

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("SparkKafkaCSVProcessing")
                .master("local[*]")
                .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
                .getOrCreate();

        logger.info("Spark session initialized");

        StructType killsSchema = defineKillsSchema();
        Dataset<Row> killsKafkaDF = readFromKafka(spark, KAFKA_TOPIC_KILLS);
        Dataset<Row> processedKillsDF = processKillsData(killsKafkaDF, killsSchema);
        
        // Use foreachBatch to process kills data
        processedKillsDF
            .writeStream()
            .foreachBatch((batchDF, batchId) -> {
                if (!batchDF.isEmpty()) {
                    // Calculate kill stats for this batch
                    Dataset<Row> playerKillStatsDF = calculatePlayerStats(batchDF);
                    
                    // Write kill stats to PostgreSQL
                    writeStaticDataToPostgres(playerKillStatsDF, "player_stats");
                    
                    logger.info("Processed batch {} for kills data", batchId);
                }
            })
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_LOCATION + "/kills")
            .start();

        StructType damagesSchema = defineDamagesSchema();
        Dataset<Row> damagesKafkaDF = readFromKafka(spark, KAFKA_TOPIC_DAMAGES);
        Dataset<Row> processedDamagesDF = processDamagesData(damagesKafkaDF, damagesSchema);
        
        // Use foreachBatch to process damages data
        processedDamagesDF
            .writeStream()
            .foreachBatch((batchDF, batchId) -> {
                if (!batchDF.isEmpty()) {
                    // Calculate damage stats for this batch
                    Dataset<Row> playerDamageStatsDF = calculatePlayerDamageStats(batchDF);
                    
                    // Write damage stats to PostgreSQL
                    writeStaticDataToPostgres(playerDamageStatsDF, "player_damage_stats");
                    
                    logger.info("Processed batch {} for damages data", batchId);
                }
            })
            .outputMode("update")
            .option("checkpointLocation", CHECKPOINT_LOCATION + "/damages")
            .start();

        // Wait for the queries to terminate
        spark.streams().awaitAnyTermination();
    }

    /**
     * Define the schema for the CS:GO game kills data
     * Based on the structure of liquid-vs-natus-vincere-m1-anubis_kills.csv
     */
    private static StructType defineKillsSchema() {
        return new StructType(new StructField[]{
                DataTypes.createStructField("frame", DataTypes.IntegerType, false),
                DataTypes.createStructField("tick", DataTypes.IntegerType, false),
                DataTypes.createStructField("round", DataTypes.IntegerType, false),
                DataTypes.createStructField("killer_name", DataTypes.StringType, true),
                DataTypes.createStructField("killer_steamid", DataTypes.LongType, true),
                DataTypes.createStructField("killer_side", DataTypes.IntegerType, true),
                DataTypes.createStructField("killer_team_name", DataTypes.StringType, true),
                DataTypes.createStructField("victim_name", DataTypes.StringType, true),
                DataTypes.createStructField("victim_steamid", DataTypes.LongType, true),
                DataTypes.createStructField("victim_side", DataTypes.IntegerType, true),
                DataTypes.createStructField("victim_team_name", DataTypes.StringType, true),
                DataTypes.createStructField("assister_name", DataTypes.StringType, true),
                DataTypes.createStructField("assister_steamid", DataTypes.LongType, true),
                DataTypes.createStructField("assister_side", DataTypes.IntegerType, true),
                DataTypes.createStructField("assister_team_name", DataTypes.StringType, true),
                DataTypes.createStructField("weapon_name", DataTypes.StringType, true),
                DataTypes.createStructField("weapon_type", DataTypes.StringType, true),
                DataTypes.createStructField("headshot", DataTypes.IntegerType, true),
                DataTypes.createStructField("penetrated_objects", DataTypes.IntegerType, true),
                DataTypes.createStructField("is_flash_assist", DataTypes.IntegerType, true),
                DataTypes.createStructField("killer_controlling_bot", DataTypes.IntegerType, true),
                DataTypes.createStructField("victim_controlling_bot", DataTypes.IntegerType, true),
                DataTypes.createStructField("assister_controlling_bot", DataTypes.IntegerType, true),
                DataTypes.createStructField("killer_x", DataTypes.DoubleType, true),
                DataTypes.createStructField("killer_y", DataTypes.DoubleType, true),
                DataTypes.createStructField("killer_z", DataTypes.DoubleType, true),
                DataTypes.createStructField("is_killer_airborne", DataTypes.IntegerType, true),
                DataTypes.createStructField("is_killer_blinded", DataTypes.IntegerType, true),
                DataTypes.createStructField("victim_x", DataTypes.DoubleType, true),
                DataTypes.createStructField("victim_y", DataTypes.DoubleType, true),
                DataTypes.createStructField("victim_z", DataTypes.DoubleType, true),
                DataTypes.createStructField("is_victim_airborne", DataTypes.IntegerType, true),
                DataTypes.createStructField("is_victim_blinded", DataTypes.IntegerType, true),
                DataTypes.createStructField("is_victim_inspecting_weapon", DataTypes.IntegerType, true),
                DataTypes.createStructField("assister_x", DataTypes.DoubleType, true),
                DataTypes.createStructField("assister_y", DataTypes.DoubleType, true),
                DataTypes.createStructField("assister_z", DataTypes.DoubleType, true),
                DataTypes.createStructField("is_trade_kill", DataTypes.IntegerType, true),
                DataTypes.createStructField("is_trade_death", DataTypes.IntegerType, true),
                DataTypes.createStructField("is_through_smoke", DataTypes.IntegerType, true),
                DataTypes.createStructField("is_no_scope", DataTypes.IntegerType, true),
                DataTypes.createStructField("distance", DataTypes.DoubleType, true),
                DataTypes.createStructField("match_checksum", DataTypes.StringType, true),
                DataTypes.createStructField("timestamp_seconds", DataTypes.LongType, true)
        });
    }
    
    /**
     * Define the schema for the CS:GO game damages data
     * Based on the structure of liquid-vs-natus-vincere-m1-anubis_damages.csv
     */
    private static StructType defineDamagesSchema() {
        return new StructType(new StructField[]{
                DataTypes.createStructField("frame", DataTypes.IntegerType, false),
                DataTypes.createStructField("tick", DataTypes.IntegerType, false),
                DataTypes.createStructField("round", DataTypes.IntegerType, false),
                DataTypes.createStructField("health", DataTypes.IntegerType, true),
                DataTypes.createStructField("armor", DataTypes.IntegerType, true),
                DataTypes.createStructField("victim_health", DataTypes.IntegerType, true),
                DataTypes.createStructField("victim_new_health", DataTypes.IntegerType, true),
                DataTypes.createStructField("victim_armor", DataTypes.IntegerType, true),
                DataTypes.createStructField("victim_new_armor", DataTypes.IntegerType, true),
                DataTypes.createStructField("attacker_steamid", DataTypes.LongType, true),
                DataTypes.createStructField("attacker_side", DataTypes.IntegerType, true),
                DataTypes.createStructField("attacker_team_name", DataTypes.StringType, true),
                DataTypes.createStructField("is_attacker_controlling_bot", DataTypes.IntegerType, true),
                DataTypes.createStructField("victim_steamid", DataTypes.LongType, true),
                DataTypes.createStructField("victim_side", DataTypes.IntegerType, true),
                DataTypes.createStructField("victim_team_name", DataTypes.StringType, true),
                DataTypes.createStructField("is_victim_controlling_bot", DataTypes.IntegerType, true),
                DataTypes.createStructField("weapon_name", DataTypes.StringType, true),
                DataTypes.createStructField("weapon_class", DataTypes.StringType, true),
                DataTypes.createStructField("hitgroup", DataTypes.IntegerType, true),
                DataTypes.createStructField("weapon_unique_id", DataTypes.StringType, true),
                DataTypes.createStructField("match_checksum", DataTypes.StringType, true),
                DataTypes.createStructField("timestamp_seconds", DataTypes.LongType, true)
        });
    }

    /**
     * Read streaming data from Kafka
     */
    private static Dataset<Row> readFromKafka(SparkSession spark, String topic) {
        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", false)
                .load();
    }

    /**
     * Process the streaming kills data
     * Parses CS:GO game kill data from Kafka messages
     */
    private static Dataset<Row> processKillsData(Dataset<Row> kafkaDF, StructType schema) {
        // Extract the value from Kafka message and cast to string
        Dataset<Row> valueDF = kafkaDF.selectExpr("CAST(value AS STRING) as csv_data");

        // Parse CSV string into structured data using SQL expression
        String schemaDDL = schema.toDDL();

        return valueDF.selectExpr(
            "from_csv(csv_data, '" + schemaDDL + "', map('header', 'true', 'delimiter', ',')) as parsed_data"
        ).select("parsed_data.*");
    }
    
    /**
     * Process the streaming damages data
     * Parses CS:GO game damage data from Kafka messages
     */
    private static Dataset<Row> processDamagesData(Dataset<Row> kafkaDF, StructType schema) {
        // Extract the value from Kafka message and cast to string
        Dataset<Row> valueDF = kafkaDF.selectExpr("CAST(value AS STRING) as csv_data");

        // Parse CSV string into structured data using SQL expression
        String schemaDDL = schema.toDDL();
        Dataset<Row> parsedDF = valueDF.selectExpr(
            "from_csv(csv_data, '" + schemaDDL + "', map('header', 'true', 'delimiter', ',')) as parsed_data"
        ).select("parsed_data.*");

        // Apply transformations specific to damage data
        return parsedDF
                // Calculate damage dealt
                .withColumn("damage_dealt", col("victim_health").minus(col("victim_new_health")));
    }

    /**
     * Calculate player kill statistics
     * Aggregates kill data to provide insights on player performance
     * Now works with static DataFrames inside foreachBatch
     */
    private static Dataset<Row> calculatePlayerStats(Dataset<Row> killsDF) {
        // Since we're now working with a static DataFrame (inside foreachBatch),
        // multiple aggregations are allowed
        Dataset<Row> aggKillsDF = killsDF
            .groupBy("killer_steamid")
            .agg(
                // Basic kill counts
                count("*").as("kills"),
                sum(when(col("headshot").equalTo(1), 1).otherwise(0)).as("headshots")
            );

        Dataset<Row> aggDeathsDF = killsDF
            .groupBy("victim_steamid")
            .agg(
                count("*").as("deaths")
            );

        Dataset<Row> aggAssistsDF = killsDF
            .groupBy("assister_steamid")
            .agg(
                count("*").as("assists")
            );

        // Join all statistics together - use explicit column references to avoid ambiguity
        Dataset<Row> joinedDF = aggKillsDF
            .join(aggDeathsDF, aggKillsDF.col("killer_steamid").equalTo(aggDeathsDF.col("victim_steamid")), "full_outer")
            .drop(aggDeathsDF.col("victim_steamid"))
            .join(aggAssistsDF, aggKillsDF.col("killer_steamid").equalTo(aggAssistsDF.col("assister_steamid")), "full_outer")
            .drop(aggAssistsDF.col("assister_steamid"));
            
        // Get player names from the original DataFrame
        Dataset<Row> additionalColumnsDF = killsDF
            .select("killer_steamid", "killer_name", "timestamp_seconds")
            .withColumnRenamed("killer_name", "player_name")
            .dropDuplicates("killer_steamid");
            
        // Join with player names - use explicit column references to avoid ambiguity
        joinedDF = joinedDF
            .join(additionalColumnsDF, joinedDF.col("killer_steamid").equalTo(additionalColumnsDF.col("killer_steamid")), "left_outer")
            .drop(additionalColumnsDF.col("killer_steamid"))
            // Fill nulls with zeros for metrics
            .na().fill(0, new String[]{"kills", "deaths", "assists", "headshots"})
            // Calculate derived metrics
            .withColumn("headshot_percentage",
                when(col("kills").gt(0),
                    round(col("headshots").divide(col("kills")).multiply(100), 2)
                ).otherwise(0.0)
            )
            .withColumn("kd_ratio",
                when(col("deaths").gt(0),
                    round(col("kills").divide(col("deaths")), 2)
                ).otherwise(col("kills")) // If deaths=0, kd_ratio = kills
            )
            .withColumnRenamed("killer_steamid", "player_id");

        return joinedDF.select("player_id", "player_name", "kills", "deaths", "assists", "kd_ratio", "headshot_percentage", "timestamp_seconds");
    }

    /**
     * Calculate player damage statistics
     * Aggregates damage data to provide insights on player performance
     */
    private static Dataset<Row> calculatePlayerDamageStats(Dataset<Row> damagesDF) {
        return damagesDF
            .groupBy("attacker_steamid")
            .agg(
                sum("damage_dealt").as("total_damage"),
                approx_count_distinct("round").as("rounds")
            )
            .withColumn("damage_per_round",
                when(col("rounds").gt(0),
                    round(col("total_damage").divide(col("rounds")), 2)
                ).otherwise(0.0)
            )
            .withColumnRenamed("attacker_steamid", "player_id");
    }

    /**
     * Write static data to PostgreSQL (for use within foreachBatch)
     */
    private static void writeStaticDataToPostgres(Dataset<Row> df, String table) {
        // Add timestamp column to track when the statistics were recorded
        Dataset<Row> batchWithTimestamp = df.withColumn("recorded_at", current_timestamp());

        // Create connection properties
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", POSTGRES_USER);
        connectionProperties.put("password", POSTGRES_PASSWORD);
        connectionProperties.put("driver", POSTGRES_DRIVER);

        // Create the table if it doesn't exist
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD)) {

            // Get the schema of the DataFrame with timestamp
            StructType schema = batchWithTimestamp.schema();

            // Build the CREATE TABLE statement
            StringBuilder createTableSQL = new StringBuilder();
            createTableSQL.append("CREATE TABLE IF NOT EXISTS ").append(table).append(" (");

            // Add columns based on DataFrame schema
            for (int i = 0; i < schema.fields().length; i++) {
                StructField field = schema.fields()[i];
                String fieldName = field.name();
                String sqlType = getSQLType(field.dataType());

                createTableSQL.append(fieldName).append(" ").append(sqlType);

                // Add comma if not the last field
                if (i < schema.fields().length - 1) {
                    createTableSQL.append(", ");
                }
            }

            createTableSQL.append(")");

            // Execute the CREATE TABLE statement
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSQL.toString());
                logger.info("Created table if not exists: {}", table);
            }
        } catch (Exception e) {
            logger.error("Error creating table: {}", table, e);
        }

        // Save the batch data to PostgreSQL
        batchWithTimestamp
            .write()
            .mode("append")  // Use append mode to add new data
            .jdbc(POSTGRES_URL, table, connectionProperties);

        logger.info("Data written to PostgreSQL table: {}", table);
    }
    
    /**
     * Convert Spark DataType to SQL type for PostgreSQL
     */
    private static String getSQLType(org.apache.spark.sql.types.DataType dataType) {
        if (dataType == DataTypes.StringType) {
            return "TEXT";
        } else if (dataType == DataTypes.IntegerType) {
            return "INTEGER";
        } else if (dataType == DataTypes.LongType) {
            return "BIGINT";
        } else if (dataType == DataTypes.DoubleType) {
            return "DOUBLE PRECISION";
        } else if (dataType == DataTypes.FloatType) {
            return "REAL";
        } else if (dataType == DataTypes.BooleanType) {
            return "BOOLEAN";
        } else if (dataType instanceof org.apache.spark.sql.types.DecimalType) {
            return "DECIMAL";
        } else if (dataType == DataTypes.TimestampType) {
            return "TIMESTAMP";
        } else if (dataType == DataTypes.DateType) {
            return "DATE";
        } else if (dataType instanceof org.apache.spark.sql.types.ArrayType) {
            return "TEXT"; // Store arrays as JSON text
        } else {
            return "TEXT"; // Default to TEXT for complex types
        }
    }
}