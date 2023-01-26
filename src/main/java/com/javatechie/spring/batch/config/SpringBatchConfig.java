package com.javatechie.spring.batch.config;

import com.javatechie.spring.batch.entity.Customer;
import com.javatechie.spring.batch.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor

public class SpringBatchConfig {

   private JobBuilderFactory jobBuilderFactory;

   private StepBuilderFactory stepBuilderFactory;

   private CustomerRepository customerRepository;

   @Bean
   public FlatFileItemReader<Customer> flatFileItemReader(){

       FlatFileItemReader<Customer> flatFileItemReader = new FlatFileItemReader<>();
       flatFileItemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
       flatFileItemReader.setName("csvCustomerReader");
       flatFileItemReader.setLinesToSkip(1);
       flatFileItemReader.setLineMapper(lineMapper());
       return flatFileItemReader;

   }

   @Bean
    public LineMapper<Customer> lineMapper() {

       DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

       //LineTokenizer will take care of mapping rows from customer csv file
       DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
       lineTokenizer.setDelimiter(",");
       lineTokenizer.setStrict(false);
       lineTokenizer.setNames("id", "firstName", "lastName",	"email",	"gender",	"contactNo",	"country",	"dob");

       //we will set field mapper to our entity Customer
       BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
       fieldSetMapper.setTargetType(Customer.class);

       //assigne linetokenizer and fieldsetmapper to linemapper
       lineMapper.setLineTokenizer(lineTokenizer);
       lineMapper.setFieldSetMapper(fieldSetMapper);

       return lineMapper;

   }


   @Bean
   public CustomerProcessor customerProcessor(){
       return new CustomerProcessor();
   }


   @Bean
   public RepositoryItemWriter<Customer> repositoryItemWriter(){

       RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
       writer.setRepository(customerRepository);
       writer.setMethodName("save");
       return writer;
   }

   @Bean
   public Step step1() {

       return stepBuilderFactory.get("csv-first-step").<Customer,Customer>chunk(10)
               .reader(flatFileItemReader())
               .processor(customerProcessor())
               .writer(repositoryItemWriter())
               .taskExecutor(taskExecutor())
               .build();
   }

   @Bean
   public Job importCSVJob(){

       return jobBuilderFactory.get("ImportCustomersInfo")
               .flow(step1())
               .end().build();
   }

   @Bean
   public TaskExecutor taskExecutor(){
       SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
       taskExecutor.setConcurrencyLimit(10);
       return  taskExecutor;
   }



}
