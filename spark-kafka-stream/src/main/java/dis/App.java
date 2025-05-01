package dis;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
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

        // Define schemas for the CS:GO game data
        StructType killsSchema = defineKillsSchema();
        StructType damagesSchema = defineDamagesSchema();

        // Read from Kafka - kills topic
        Dataset<Row> killsKafkaDF = readFromKafka(spark, KAFKA_TOPIC_KILLS);
        
        // Read from Kafka - damages topic
        Dataset<Row> damagesKafkaDF = readFromKafka(spark, KAFKA_TOPIC_DAMAGES);

        // Process the kills data
        Dataset<Row> processedKillsDF = processKillsData(killsKafkaDF, killsSchema);

        // Process the damages data
        Dataset<Row> processedDamagesDF = processDamagesData(damagesKafkaDF, damagesSchema);

        // Calculate player statistics
        Dataset<Row> playerKillStatsDF = calculatePlayerKillStats(processedKillsDF);
        Dataset<Row> playerDamageStatsDF = calculatePlayerDamageStats(processedDamagesDF);
        Dataset<Row> teamKillStatsDF = calculateTeamKillStats(processedKillsDF);
        Dataset<Row> teamDamageStatsDF = calculateTeamDamageStats(processedDamagesDF);
        Dataset<Row> weaponStatsDF = calculateWeaponStats(processedKillsDF, processedDamagesDF);

        // Write statistics to PostgreSQL
        StreamingQuery killsQuery = writeToPostgres(processedKillsDF, "kills");
        StreamingQuery damagesQuery = writeToPostgres(processedDamagesDF, "damages");
        StreamingQuery playerKillStatsQuery = writeToPostgres(playerKillStatsDF, "player_kill_stats");
        StreamingQuery playerDamageStatsQuery = writeToPostgres(playerDamageStatsDF, "player_damage_stats");
        StreamingQuery teamKillStatsQuery = writeToPostgres(teamKillStatsDF, "team_kill_stats");
        StreamingQuery teamDamageStatsQuery = writeToPostgres(teamDamageStatsDF, "team_damage_stats");
        StreamingQuery weaponStatsQuery = writeToPostgres(weaponStatsDF, "weapon_stats");

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
                DataTypes.createStructField("match_checksum", DataTypes.StringType, true)
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
                DataTypes.createStructField("match_checksum", DataTypes.StringType, true)
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
        Dataset<Row> parsedDF = valueDF.selectExpr(
            "from_csv(csv_data, '" + schemaDDL + "', map('header', 'true', 'delimiter', ',')) as parsed_data"
        ).select("parsed_data.*");

        // Apply transformations specific to game data
        // Add processing timestamp
        // Calculate kill distance in meters (divide by 10 to convert game units)
        // Create a weapon category column
        // Flag for special kills

        return parsedDF
                // Add processing timestamp
                .withColumn("processing_timestamp", current_timestamp())
                // Calculate kill distance in meters (divide by 10 to convert game units)
                .withColumn("distance_meters", col("distance").divide(10))
                // Create a weapon category column
                .withColumn("weapon_category",
                    when(col("weapon_type").equalTo("rifle"), "primary")
                    .when(col("weapon_type").equalTo("pistol"), "secondary")
                    .when(col("weapon_type").equalTo("smg"), "primary")
                    .when(col("weapon_type").equalTo("shotgun"), "primary")
                    .when(col("weapon_type").equalTo("machinegun"), "primary")
                    .when(col("weapon_type").equalTo("sniper"), "primary")
                    .otherwise("other"))
                // Flag for special kills
                .withColumn("special_kill",
                    when(col("headshot").equalTo(1), true)
                    .when(col("is_through_smoke").equalTo(1), true)
                    .when(col("is_no_scope").equalTo(1), true)
                    .when(col("penetrated_objects").gt(0), true)
                    .otherwise(false));
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
        // Add processing timestamp
        // Calculate damage dealt
        // Calculate armor damage
        // Determine if it was a lethal hit
        // Categorize hitgroup (1=head, 2=chest, etc.)

        return parsedDF
                // Add processing timestamp
                .withColumn("processing_timestamp", current_timestamp())
                // Calculate damage dealt
                .withColumn("damage_dealt", col("victim_health").minus(col("victim_new_health")))
                // Calculate armor damage
                .withColumn("armor_damage", col("victim_armor").minus(col("victim_new_armor")))
                // Determine if it was a lethal hit
                .withColumn("is_lethal_hit", col("victim_new_health").equalTo(0))
                // Categorize hitgroup (1=head, 2=chest, etc.)
                .withColumn("hitgroup_name",
                    when(col("hitgroup").equalTo(1), "head")
                    .when(col("hitgroup").equalTo(2), "chest")
                    .when(col("hitgroup").equalTo(3), "stomach")
                    .when(col("hitgroup").equalTo(4), "left_arm")
                    .when(col("hitgroup").equalTo(5), "right_arm")
                    .when(col("hitgroup").equalTo(6), "left_leg")
                    .when(col("hitgroup").equalTo(7), "right_leg")
                    .otherwise("other"));
    }

    /**
     * Calculate player kill statistics
     * Aggregates kill data to provide insights on player performance
     */
    private static Dataset<Row> calculatePlayerKillStats(Dataset<Row> killsDF) {
        // Group by killer and calculate various kill statistics
        // Basic kill counts
        // Victim-based statistics
        // Weapon statistics
        // Round statistics
        // Calculate average kill distance
        // Calculate derived metrics
        // Sort by total kills descending

        return killsDF
            .groupBy("killer_steamid", "killer_name", "killer_team_name")
            .agg(
                // Basic kill counts
                count("*").as("total_kills"),
                sum(when(col("headshot").equalTo(1), 1).otherwise(0)).as("headshot_kills"),
                sum(when(col("is_through_smoke").equalTo(1), 1).otherwise(0)).as("through_smoke_kills"),
                sum(when(col("is_no_scope").equalTo(1), 1).otherwise(0)).as("no_scope_kills"),
                sum(when(col("penetrated_objects").gt(0), 1).otherwise(0)).as("wallbang_kills"),
                sum(when(col("is_flash_assist").equalTo(1), 1).otherwise(0)).as("flash_assisted_kills"),
                sum(when(col("is_trade_kill").equalTo(1), 1).otherwise(0)).as("trade_kills"),

                // Victim-based statistics
                approx_count_distinct("victim_steamid").as("unique_victims"),

                // Weapon statistics
                collect_set("weapon_name").as("weapons_used"),

                // Round statistics
                approx_count_distinct("round").as("rounds_with_kills"),

                // Calculate average kill distance
                avg("distance").as("avg_kill_distance")
            )
            // Calculate derived metrics
            .withColumn("headshot_percentage",
                when(col("total_kills").gt(0),
                    round(col("headshot_kills").divide(col("total_kills")).multiply(100), 2)
                ).otherwise(0.0)
            )
            .withColumn("special_kills",
                col("headshot_kills")
                .plus(col("through_smoke_kills"))
                .plus(col("no_scope_kills"))
                .plus(col("wallbang_kills"))
            )
            .withColumn("special_kill_percentage",
                when(col("total_kills").gt(0),
                    round(col("special_kills").divide(col("total_kills")).multiply(100), 2)
                ).otherwise(0.0)
            );
    }

    /**
     * Calculate player damage statistics
     * Aggregates damage data to provide insights on player performance
     */
    private static Dataset<Row> calculatePlayerDamageStats(Dataset<Row> damagesDF) {
        // Group by attacker and calculate various damage statistics
        // Basic damage counts
        // Hitgroup statistics
        // Victim-based statistics
        // Weapon statistics
        // Round statistics
        // Average damage per hit
        // Calculate derived metrics
        // Sort by total damage dealt descending

        return damagesDF
            .groupBy("attacker_steamid", "attacker_team_name")
            .agg(
                // Basic damage counts
                count("*").as("total_hits"),
                sum("damage_dealt").as("total_damage_dealt"),
                sum("armor_damage").as("total_armor_damage"),
                sum(when(col("is_lethal_hit").equalTo(true), 1).otherwise(0)).as("lethal_hits"),

                // Hitgroup statistics
                sum(when(col("hitgroup").equalTo(1), 1).otherwise(0)).as("headshots"),
                sum(when(col("hitgroup").equalTo(2), 1).otherwise(0)).as("chest_hits"),
                sum(when(col("hitgroup").equalTo(3), 1).otherwise(0)).as("stomach_hits"),
                sum(when(col("hitgroup").equalTo(4).or(col("hitgroup").equalTo(5)), 1).otherwise(0)).as("arm_hits"),
                sum(when(col("hitgroup").equalTo(6).or(col("hitgroup").equalTo(7)), 1).otherwise(0)).as("leg_hits"),

                // Victim-based statistics
                approx_count_distinct("victim_steamid").as("unique_victims"),

                // Weapon statistics
                collect_set("weapon_name").as("weapons_used"),

                // Round statistics
                approx_count_distinct("round").as("rounds_with_damage"),

                // Average damage per hit
                avg("damage_dealt").as("avg_damage_per_hit")
            )
            // Calculate derived metrics
            .withColumn("headshot_percentage",
                when(col("total_hits").gt(0),
                    round(col("headshots").divide(col("total_hits")).multiply(100), 2)
                ).otherwise(0.0)
            )
            .withColumn("lethal_hit_percentage",
                when(col("total_hits").gt(0),
                    round(col("lethal_hits").divide(col("total_hits")).multiply(100), 2)
                ).otherwise(0.0)
            );
    }

    // Add these two new methods
    /**
     * Calculate team kill statistics
     * Aggregates kill data by team
     */
    private static Dataset<Row> calculateTeamKillStats(Dataset<Row> killsDF) {
        return killsDF
            .groupBy("killer_team_name")
            .agg(
                count("*").as("total_kills"),
                sum(when(col("headshot").equalTo(1), 1).otherwise(0)).as("headshot_kills"),
                approx_count_distinct("killer_steamid").as("active_players"),
                avg("distance").as("avg_kill_distance"),
                max("distance").as("max_kill_distance"),
                sum(when(col("special_kill"), 1).otherwise(0)).as("special_kills")
            )
            .withColumn("headshot_percentage", 
                when(col("total_kills").gt(0), 
                    round(col("headshot_kills").divide(col("total_kills")).multiply(100), 2)
                ).otherwise(0.0)
            )
            .withColumn("special_kill_percentage", 
                when(col("total_kills").gt(0), 
                    round(col("special_kills").divide(col("total_kills")).multiply(100), 2)
                ).otherwise(0.0)
            );
    }

    /**
     * Calculate team damage statistics
     * Aggregates damage data by team
     */
    private static Dataset<Row> calculateTeamDamageStats(Dataset<Row> damagesDF) {
        return damagesDF
            .groupBy("attacker_team_name")
            .agg(
                count("*").as("total_hits"),
                sum("damage_dealt").as("total_damage_dealt"),
                approx_count_distinct("attacker_steamid").as("active_players"),
                avg("damage_dealt").as("avg_damage_per_hit")
            );
    }

    /**
     * Calculate team statistics
     * Aggregates data by team to show overall team performance
     * Note: This method is no longer used directly due to streaming limitations
     */
    private static Dataset<Row> calculateTeamStats(Dataset<Row> killsDF, Dataset<Row> damagesDF) {
        // This method is kept for reference but should not be used with streaming data
        // Use calculateTeamKillStats and calculateTeamDamageStats separately instead
        
        // Team kill statistics
        Dataset<Row> teamKillStats = calculateTeamKillStats(killsDF);
        
        // Team damage statistics
        Dataset<Row> teamDamageStats = calculateTeamDamageStats(damagesDF);
        
        // Join the two datasets on team name
        // Use coalesce to handle null values from outer join

        return teamKillStats
            .join(teamDamageStats,
                  teamKillStats.col("killer_team_name").equalTo(teamDamageStats.col("attacker_team_name")),
                  "outer")
            .select(
                teamKillStats.col("killer_team_name").as("team_name"),
                teamKillStats.col("total_kills"),
                teamKillStats.col("headshot_kills"),
                teamKillStats.col("special_kills"),
                teamKillStats.col("avg_kill_distance"),
                teamKillStats.col("max_kill_distance"),
                teamDamageStats.col("total_hits"),
                teamDamageStats.col("total_damage_dealt"),
                teamDamageStats.col("avg_damage_per_hit"),
                // Use coalesce to handle null values from outer join
                coalesce(teamKillStats.col("active_players"), teamDamageStats.col("active_players")).as("active_players")
            )
            .withColumn("headshot_percentage",
                when(col("total_kills").gt(0),
                    round(col("headshot_kills").divide(col("total_kills")).multiply(100), 2)
                ).otherwise(0.0)
            )
            .withColumn("special_kill_percentage",
                when(col("total_kills").gt(0),
                    round(col("special_kills").divide(col("total_kills")).multiply(100), 2)
                ).otherwise(0.0)
            )
            .withColumn("damage_per_kill",
                when(col("total_kills").gt(0),
                    round(col("total_damage_dealt").divide(col("total_kills")), 2)
                ).otherwise(0.0)
            );
    }

    /**
     * Calculate weapon usage statistics
     * Analyzes which weapons are most effective
     */
    private static Dataset<Row> calculateWeaponStats(Dataset<Row> killsDF, Dataset<Row> damagesDF) {
        // Weapon kill statistics

        return killsDF
            .groupBy("weapon_name", "weapon_type")
            .agg(
                count("*").as("total_kills"),
                sum(when(col("headshot").equalTo(1), 1).otherwise(0)).as("headshot_kills"),
                approx_count_distinct("killer_steamid").as("unique_users"),
                avg("distance").as("avg_kill_distance"),
                max("distance").as("max_kill_distance"),
                sum(when(col("special_kill"), 1).otherwise(0)).as("special_kills")
            )
            .withColumn("headshot_percentage",
                when(col("total_kills").gt(0),
                    round(col("headshot_kills").divide(col("total_kills")).multiply(100), 2)
                ).otherwise(0.0)
            )
            .withColumn("special_kill_percentage",
                when(col("total_kills").gt(0),
                    round(col("special_kills").divide(col("total_kills")).multiply(100), 2)
                ).otherwise(0.0)
            );
    }

    /**
     * Write streaming data to PostgreSQL
     */
    private static StreamingQuery writeToPostgres(Dataset<Row> df, String tableName) throws TimeoutException {
        return df
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    // Add timestamp column to track when the statistics were recorded
                    Dataset<Row> batchWithTimestamp = batchDF.withColumn("recorded_at", current_timestamp());
                    
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
                        createTableSQL.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
                        
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
                            logger.info("Created table if not exists: " + tableName);
                        }
                    } catch (Exception e) {
                        logger.error("Error creating table: " + tableName, e);
                    }
                    
                    // Save the batch data to PostgreSQL
                    batchWithTimestamp
                        .write()
                        .mode("append")  // Use append mode to add new data
                        .jdbc(POSTGRES_URL, tableName, connectionProperties);
                    
                    logger.info("Batch " + batchId + " written to PostgreSQL table: " + tableName);
                })
                .outputMode("update")
                .option("checkpointLocation", CHECKPOINT_LOCATION + "/" + tableName)
                .start();
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