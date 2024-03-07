package io.github.genie.id.generator.repository.jdbc;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.genie.id.generator.core.IdGenerator;
import io.github.genie.id.generator.core.IdGeneratorFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

class MySqlRepositoryTest {


    public static void main(String[] args) throws
            ClassNotFoundException, InterruptedException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql:///id_generator");
        config.setUsername("root");
        config.setPassword("root");
        config.addDataSourceProperty("connectionTimeout", "1000");
        config.addDataSourceProperty("idleTimeout", "60000");
        config.addDataSourceProperty("maximumPoolSize", "2");
        HikariDataSource source = new HikariDataSource(config);

        // MysqlConnectionPoolDataSource source = new MysqlConnectionPoolDataSource();
        // source.setURL("jdbc:mysql:///id_generator");
        // source.setUser("root");
        // source.setPassword("root");
        // source.setConnectTimeout(1000);

//        JdbcRepository repository = new JdbcRepository(source::getConnection);
//        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory(repository);

        IdGeneratorFactory factory = new MysqlConfigurationCenter(source::getConnection);

        IdGenerator test = factory.getIdGenerator("test");
        int size = 1000000;
        long[] ids = new long[size];
        long l = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            ids[i] = test.nextId();
        }
        System.out.println(System.currentTimeMillis() - l);
        Set<Long> set = new HashSet<>();
        for (long id : ids) {
            set.add(id);
        }
        System.out.println(set.size());
        Thread.sleep(Duration.ofSeconds(100).toMillis());
        source.close();
//        repository.close();
    }

}