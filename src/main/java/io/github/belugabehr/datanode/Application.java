package io.github.belugabehr.datanode;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.github.belugabehr.datanode.meta.block.BlockReportScanner;
import io.github.belugabehr.datanode.registration.RegistrationService;

@SpringBootApplication
public class Application implements CommandLineRunner {

  @Autowired
  private RegistrationService registrationService;

  @Autowired
  private BlockReportScanner blockReportScanner;

  @Override
  public void run(final String... args) throws Exception {
    registrationService.register();
    blockReportScanner.scan(TimeUnit.SECONDS, 0L);
  }

  @Bean
  public ThreadPoolTaskExecutor globalTaskExecutor() {
    ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
    pool.setCorePoolSize(1);
    pool.setMaxPoolSize(Integer.MAX_VALUE);
    pool.setDaemon(true);
    pool.setAwaitTerminationSeconds(60);
    pool.setWaitForTasksToCompleteOnShutdown(true);
    pool.setKeepAliveSeconds(60);
    pool.setThreadGroupName("GlobalAsync");
    pool.setThreadNamePrefix("global-async-");
    return pool;
  }

  @Bean
  public ScheduledExecutorService globalScheduledTaskExecutor() {
    return MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(4,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("global-schedule-%d").build()));
  }

  public static void main(final String[] args) throws InterruptedException {
    SpringApplication app = new SpringApplication(Application.class);
    app.setBannerMode(Banner.Mode.OFF);
    app.run(args);
  }
}