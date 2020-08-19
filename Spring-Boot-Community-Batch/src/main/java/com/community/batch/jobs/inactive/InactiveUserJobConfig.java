package com.community.batch.jobs.inactive;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.Grade;
import com.community.batch.domain.enums.UserStatus;
import com.community.batch.jobs.inactive.listener.InactiveJobListener;
import com.community.batch.jobs.inactive.listener.InactiveStepListener;
import com.community.batch.jobs.readers.QueueItemReader;
import com.community.batch.repository.UserRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

// 실행할 JOB 배치
@Slf4j
@AllArgsConstructor
@Configuration
public class InactiveUserJobConfig {

    private final static int CHUNK_SIZE = 5;

    private final EntityManagerFactory entityManagerFactory;

    @Bean // Job 을 빈등록 한다는 의미
    public Job inactiveUserJob(JobBuilderFactory jobBuilderFactory , InactiveJobListener inactiveJobListener, Step partitionerStep) {
        return jobBuilderFactory.get("inactiveUserJob") // inactiveUserJob 이라는 Job 생성
                .preventRestart() // 재실행, 중복실행 방지
                .listener(inactiveJobListener) // 리스너 등록
                .start(partitionerStep) // step 실행
                .build(); // 빌드
    }

    @Bean
    @JobScope
    public Step partitionerStep(StepBuilderFactory stepBuilderFactory, Step inactiveJobStep){
        return stepBuilderFactory
                .get("partitionerStep")
                .partitioner("partitionerStep", new InactiveUserPartitioner())
                .gridSize(5)
                .step(inactiveJobStep)
                .taskExecutor(taskExecutor())
                .build();
    }
/*
    @Bean
    public Flow multiFlow(Step inactiveJobStep){
        Flow flows[] = new Flow[5];
        IntStream.range(0, flows.length).forEach(i -> flows[i] =
                new FlowBuilder<Flow>("MultiFlow"+i).from(inactiveJobFlow(inactiveJobStep)).end());
        FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("MultiFLowTest");
        return flowBuilder
                .split(taskExecutor())
                .add(flows)
                .build();
    }

    public Flow inactiveJobFlow(Step inactiveJobStep){
        FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("inactiveJobFlow");
        return flowBuilder
                .start(new InactiveJobExecutionDecider())
                .on(FlowExecutionStatus.FAILED.getName()).end()
                .on(FlowExecutionStatus.COMPLETED.getName()).to(inactiveJobStep).end();
    }
*/
    @Bean // 스탭을 빈 등록한다는 의미
    public Step inactiveJobStep(StepBuilderFactory stepBuilderFactory, InactiveStepListener inactiveStepListener, ListItemReader<User> inactiveUserReader, TaskExecutor taskExecutor){
        return stepBuilderFactory.get("inactiveUserStep")// inactiveUserStep 이라는 Step 생성
                .<User, User> chunk(CHUNK_SIZE) // 10 사이즈마다.
                .reader(inactiveUserReader) // 읽기
                .processor(inactiveUserProcessor()) // 프로세서
                .writer(inactiveUserWriter()) // 작성
                .taskExecutor(taskExecutor)
                .throttleLimit(2)
                .listener(inactiveStepListener)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor(){
        return new SimpleAsyncTaskExecutor("Batch_Task");
    }

    /*
    @Bean
    @StepScope
    public QueueItemReader<User> inactiveUserReader(){ // 매개변수에 UserRepository userRepository 추가로 넣어주자
        List<User> oldUsers =
                userRepository.findByUpdatedDateBeforeAndStatusEquals(LocalDateTime.now().minusYears(1), UserStatus.ACTIVE);
        return new QueueItemReader<>(oldUsers); // 여러개를 받아왔구
    }
    */

    @Bean
    @StepScope
    public ListItemReader<User> inactiveUserReader(@Value("#{stepExecutionContext[grade]}") String grade, UserRepository userRepository){
        log.info(Thread.currentThread().getName());
        List<User> inactiveUsers = userRepository.findByUpdatedDateBeforeAndStatusEqualsAndGradeEquals(LocalDateTime.now().minusYears(1), UserStatus.ACTIVE, Grade.valueOf(grade));
        return new ListItemReader<>(inactiveUsers); // 여러개를 받아왔구
    }

    @Bean(destroyMethod="")
    @StepScope
    public JpaPagingItemReader<User> inactiveUserJpaReader(){
        JpaPagingItemReader<User> jpaPagingItemReader = new JpaPagingItemReader(){
            @Override
            public int getPage() {
                return 0;
            }
        };
        jpaPagingItemReader.setQueryString(
                "select u from User as u where u.updatedDate < :updatedDate and u.status = :status");
        Map<String, Object> map = new HashMap<>();
        LocalDateTime now = LocalDateTime.now();
        map.put("updatedDate", now.minusYears(1));
        map.put("status", UserStatus.ACTIVE);

        jpaPagingItemReader.setParameterValues(map);
        jpaPagingItemReader.setEntityManagerFactory(entityManagerFactory);
        jpaPagingItemReader.setPageSize(CHUNK_SIZE);
        return jpaPagingItemReader;
    }


    public ItemProcessor<User, User> inactiveUserProcessor(){
        // return User::setInactive;
        // 최신 람다 표현식
        // 인터페이스를 사용하는데 전달된 인자의 메소드만 사용할 경우 이렇게 사용해도 된다.
        // 즉 new ItemProcessor 인터페이스가 1개 있는데
        // 그 인터페이스의 인자로 User 객체가 들어가고
        // 메서드 내부에 사용되는 것이 User 객체의 setInactive 메서드라는 것이다.

        return new ItemProcessor<User, User>() {
            @Override
            public User process(User user) throws Exception {
                return user.setInactive(); // 받아온 아이템들을 inactive 처리
            }
        };
    }

    private JpaItemWriter<User> inactiveUserWriter(){ // 저징 구문 입력
        JpaItemWriter<User> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }

}