package dz.spark;

import org.apache.spark.sql.*;

public class LimitsController implements Runnable{
    //TODO: Добавить проверку на изменения в БД, вынести запрос значений в отдельный метод
    @Override
    public void run() {
        while(true) {
            SparkSession spark = SparkSession
                    .builder()
                    .master("local")
                    .appName("SparkTrafficCounter")
                    .getOrCreate();

            //Подключение базы
            Dataset<Row> jdbcDF = spark.read()
                    .format("jdbc")
                    .option("driver", "org.postgresql.Driver")
                    .option("url", "jdbc:postgresql://localhost:5432/traffic_limits")
                    .option("dbtable", "limits_per_hour")
                    //В option user и password необходимо указать ваши логин и пароль для доступа к PostgreSQL
                    .option("user", "postgres")
                    .option("password", "postgres")
                    .load();

            //Представление для чистого SQL-запроса
            jdbcDF.createOrReplaceTempView("view");

            //Значение минимального объема пакетов с учетом даты
            PacketCounter.min = spark.sql("SELECT limit_value FROM view WHERE limit_name = 'min' " +
                    "AND effective_date = (SELECT MAX(effective_date) FROM view)").first().getLong(0);

            //Значение максимального объема пакетов с учетом даты
            PacketCounter.max = spark.sql("SELECT limit_value FROM view WHERE limit_name = 'max' " +
                    "AND effective_date = (SELECT MAX(effective_date) FROM view)").first().getLong(0);

            //Сон на 20 минут
            try {
                Thread.sleep(120_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
