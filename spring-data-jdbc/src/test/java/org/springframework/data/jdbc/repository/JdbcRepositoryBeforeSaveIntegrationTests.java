package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import lombok.Value;
import lombok.With;

@ContextConfiguration
@ExtendWith(SpringExtension.class)
@ActiveProfiles("hsql")
public class JdbcRepositoryBeforeSaveIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryBeforeSaveIntegrationTests.class;
		}
	}

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired ImmutableWithGeneratedIdEntityRepository immutableWithManualIdEntityRepository;
	@Autowired MutableWithGeneratedIdEntityRepository mutableWithGeneratedIdEntityRepository;
	@Autowired MutableWithGeneratedImmutableIdEntityRepository mutableWithGeneratedImmutableIdEntityRepository;
	@Autowired ImmutableWithGeneratedMutableIdEntityRepository immutableWithGeneratedMutableIdEntityRepository;

	@Test
	public void immutableWithGeneratedId() {

		ImmutableWithGeneratedIdEntity entity = new ImmutableWithGeneratedIdEntity(null, "immutable");
		ImmutableWithGeneratedIdEntity saved = immutableWithManualIdEntityRepository.save(entity);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getName()).isEqualTo("fromBeforeSaveCallback"); // actual is "immutable"

		assertThat(immutableWithManualIdEntityRepository.findAll()).hasSize(1);
	}

	@Test
	public void mutableWithGeneratedId() {

		MutableWithGeneratedIdEntity entity = new MutableWithGeneratedIdEntity(null, "immutable");
		MutableWithGeneratedIdEntity saved = mutableWithGeneratedIdEntityRepository.save(entity);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getName()).isEqualTo("fromBeforeSaveCallback");

		assertThat(mutableWithGeneratedIdEntityRepository.findAll()).hasSize(1);
	}

	@Test
	public void mutableWithGeneratedImmutableId() {

		MutableWithGeneratedImmutableIdEntity entity = new MutableWithGeneratedImmutableIdEntity(null, "immutable");
		MutableWithGeneratedImmutableIdEntity saved = mutableWithGeneratedImmutableIdEntityRepository.save(entity);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getName()).isEqualTo("fromBeforeSaveCallback");

		assertThat(mutableWithGeneratedImmutableIdEntityRepository.findAll()).hasSize(1);
	}

	@Test
	public void immutableWithGeneratedMutableId() {

		ImmutableWithGeneratedMutableIdEntity entity = new ImmutableWithGeneratedMutableIdEntity(null, "immutable");
		ImmutableWithGeneratedMutableIdEntity saved = immutableWithGeneratedMutableIdEntityRepository.save(entity);

		assertThat(saved.getId()).isNotNull();
		assertThat(saved.getName()).isEqualTo("fromBeforeSaveCallback");

		assertThat(immutableWithGeneratedMutableIdEntityRepository.findAll()).hasSize(1);
	}

	private interface ImmutableWithGeneratedIdEntityRepository extends CrudRepository<ImmutableWithGeneratedIdEntity, Long> {}

	@Value
	@With
	static class ImmutableWithGeneratedIdEntity {
		@Id Long id;
		String name;
	}

	private interface MutableWithGeneratedIdEntityRepository extends CrudRepository<MutableWithGeneratedIdEntity, Long> {}

	@Data
	@AllArgsConstructor
	static class MutableWithGeneratedIdEntity {
		@Id private Long id;
		private String name;
	}

	private interface MutableWithGeneratedImmutableIdEntityRepository extends CrudRepository<MutableWithGeneratedImmutableIdEntity, Long> {}

	@Data
	@AllArgsConstructor
	static class MutableWithGeneratedImmutableIdEntity {
		@Id private final Long id;
		private String name;
	}

	private interface ImmutableWithGeneratedMutableIdEntityRepository extends CrudRepository<ImmutableWithGeneratedMutableIdEntity, Long> {}

	@Data
	@AllArgsConstructor
	static class ImmutableWithGeneratedMutableIdEntity {
		@Id private Long id;
		@With
		private final String name;
	}

	@Configuration
	@ComponentScan("org.springframework.data.jdbc.testing")
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = CrudRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	static class TestConfiguration {

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryBeforeSaveIntegrationTests.class;
		}

		/**
		 * {@link NamingStrategy} that harmlessly uppercases the table name, demonstrating how to inject one while not
		 * breaking existing SQL operations.
		 */
		@Bean
		NamingStrategy namingStrategy() {

			return new NamingStrategy() {

				@Override
				public String getTableName(Class<?> type) {
					return type.getSimpleName().toUpperCase();
				}
			};
		}

		@Bean
		BeforeSaveCallback<ImmutableWithGeneratedIdEntity> nameSetterImmutable() {
			return (aggregate, aggregateChange) -> aggregate.withName("fromBeforeSaveCallback");
		}

		@Bean
		BeforeSaveCallback<MutableWithGeneratedIdEntity> nameSetterMutable() {
			return (aggregate, aggregateChange) -> {
				aggregate.setName("fromBeforeSaveCallback");
				return aggregate;
			};
		}

		@Bean
		BeforeSaveCallback<MutableWithGeneratedImmutableIdEntity> nameSetterMutableWithImmutableId() {
			return (aggregate, aggregateChange) -> {
				aggregate.setName("fromBeforeSaveCallback");
				return aggregate;
			};
		}

		@Bean
		BeforeSaveCallback<ImmutableWithGeneratedMutableIdEntity> nameSetterImmutableWithMutableId() {
			return (aggregate, aggregateChange) -> aggregate.withName("fromBeforeSaveCallback");
		}
	}
}
