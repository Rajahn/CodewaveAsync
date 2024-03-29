package edu.vt.ranhuo.asynccore;

import edu.vt.ranhuo.asynccore.config.TaskConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.IOException;
import java.net.URL;
import java.util.UUID;

@Slf4j
public class RedissonTest {

    private RedissonClient redissonClient;

    @Before
    public void init() throws IOException {
        final URL resource = RedissonTest.class.getClassLoader().getResource("redisson.yml");
        redissonClient = Redisson.create(Config.fromYAML(resource));
    }

    @Test
    public void config() {
        final TaskConfig config = TaskConfig.builder().redissonClient(redissonClient).build();
        log.info(String.valueOf(config));
    }

    @Test
    public void getUUID32() {
        log.info(String.valueOf(UUID.randomUUID()));
    }

}
