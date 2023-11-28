package io.github.genie.id.generator.repository.mysql;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.genie.id.generator.repository.mysql.core.IdGenerator;
import io.github.genie.id.generator.repository.mysql.core.support.DefaultIdGeneratorFactory;

import java.time.Duration;

class MySqlRepositoryTest {


    public static void main(String[] args) throws
            ClassNotFoundException, InterruptedException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql:///id_generator");
        config.setUsername("root");
        config.setPassword("root");
        config.addDataSourceProperty("connectionTimeout", "1000"); // 连接超时：1秒
        config.addDataSourceProperty("idleTimeout", "60000"); // 空闲超时：60秒
        config.addDataSourceProperty("maximumPoolSize", "2"); // 最大连接数：10
        HikariDataSource source = new HikariDataSource(config);

        // MysqlConnectionPoolDataSource source = new MysqlConnectionPoolDataSource();
        // source.setURL("jdbc:mysql:///id_generator");
        // source.setUser("root");
        // source.setPassword("root");
        // source.setConnectTimeout(1000);

        MySqlRepository repository = new MySqlRepository(source::getConnection);
        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory(repository);

        IdGenerator test = factory.getIdGenerator("test");
        for (int i = 0; i < 10; i++) {
            System.out.println(test.nextId());
        }
        Thread.sleep(Duration.ofMinutes(10).toMillis());
        source.close();
        repository.close();
    }

}