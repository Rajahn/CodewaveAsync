package edu.vt.ranhuo.asynctest;

import edu.vt.ranhuo.asynccore.config.TaskConfig;
import edu.vt.ranhuo.asynccore.config.TaskContext;
import edu.vt.ranhuo.asynccore.enums.QueueType;
import edu.vt.ranhuo.asyncmaster.context.IMaster;
import edu.vt.ranhuo.asyncmaster.context.Master;
import edu.vt.ranhuo.asyncslave.context.ISlave;
import edu.vt.ranhuo.asyncslave.context.Slave;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.IOException;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
@Slf4j
public class simpletest {
    private final AtomicBoolean stopper = new AtomicBoolean();
    private final Random rand = new Random();
    private final String prefix = "test:codewave";
    private final long heartbeatInterval = 10 * 1000;
    private final int expirationCount = 3;
    private TaskContext context;
    private TaskConfig config;
    private IMaster<String> master;
    private ISlave<String> slave;

    final URL resource = simpletest.class.getClassLoader().getResource("redisson.yml");
    final RedissonClient redissonClient = Redisson.create(Config.fromYAML(resource));

    public simpletest() throws IOException {
    }

    public IMaster createMaster(){
        TaskConfig taskConfig =  TaskConfig(redissonClient);
        IMaster<String> master = new Master(taskConfig);
        return master;
    }

    public ISlave createSlave(){
        TaskConfig taskConfig =  TaskConfig(redissonClient);
        ISlave<String> slave = new Slave(taskConfig);
        return slave;
    }

    private TaskConfig TaskConfig(RedissonClient redissonClient) {
        config = TaskConfig.builder().prefix(prefix).redissonClient(redissonClient).queueNums(3)
                .heartbeatInterval(heartbeatInterval).expirationCount(expirationCount).build();
        return config;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        simpletest testAsync = new simpletest();
        IMaster master = testAsync.createMaster();
        ISlave slave = testAsync.createSlave();
        master.send(1,"test-task1, hahaha");
        master.send(1,"test-task2, hahaha");
        master.send(1,"test-task3, hahaha");

        Thread.sleep(10000);
//        slave.consume();
//        slave.consume();
//        slave.consume();
//        slave.consume();

    }

}
@Data
class Task {
    private int id;
    private String task_input;
}
