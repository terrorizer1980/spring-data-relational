/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

/**
 * Integration tests for {@link JdbcAggregateTemplate}.
 *
 * @author Jens Schauder
 * @author Thomas Lang
 * @author Mark Paluch
 */
@ContextConfiguration
@Transactional
@RunWith(SpringRunner.class)
public class JdbcAggregateTemplateWithSchemaIntegrationTests {

	@Autowired JdbcAggregateOperations template;
	@Autowired NamedParameterJdbcOperations jdbcTemplate;

	@Test // DATAJDBC-276
	public void saveAndLoadAnEntityWithListOfElementsWithoutId() {

		ListParent entity = new ListParent();
		entity.name = "name";

		ElementNoId element = new ElementNoId();
		element.content = "content";

		entity.content.add(element);

		template.save(entity);

		ListParent reloaded = template.findById(entity.id, ListParent.class);

		assertThat(reloaded.content).extracting(e -> e.content).containsExactly("content");
	}

	@Test // DATAJDBC-223
	public void saveAndLoadLongChainOfListsWithoutIds() {

		NoIdListChain4 saved = template.save(createNoIdTree());

		assertThat(saved.four).describedAs("Something went wrong during saving").isNotNull();

		NoIdListChain4 reloaded = template.findById(saved.four, NoIdListChain4.class);
		assertThat(reloaded).isEqualTo(saved);
	}

	@Test // DATAJDBC-223
	public void shouldDeleteChainOfListsWithoutIds() {

		NoIdListChain4 saved = template.save(createNoIdTree());
		template.deleteById(saved.four, NoIdListChain4.class);

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(count("NO_ID_LIST_CHAIN4")).describedAs("Chain4 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_LIST_CHAIN3")).describedAs("Chain3 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_LIST_CHAIN2")).describedAs("Chain2 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_LIST_CHAIN1")).describedAs("Chain1 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_LIST_CHAIN0")).describedAs("Chain0 elements got deleted").isEqualTo(0);
		});
	}

	/**
	 * creates an instance of {@link NoIdListChain4} with the following properties:
	 * <ul>
	 * <li>Each element has two children with indices 0 and 1.</li>
	 * <li>the xxxValue of each element is a {@literal v} followed by the indices used to navigate to the given instance.
	 * </li>
	 * </ul>
	 */
	private static NoIdListChain4 createNoIdTree() {

		NoIdListChain4 chain4 = new NoIdListChain4();
		chain4.fourValue = "v";

		IntStream.of(0, 1).forEach(i -> {

			NoIdListChain3 c3 = new NoIdListChain3();
			c3.threeValue = chain4.fourValue + i;
			chain4.chain3.add(c3);

			IntStream.of(0, 1).forEach(j -> {

				NoIdListChain2 c2 = new NoIdListChain2();
				c2.twoValue = c3.threeValue + j;
				c3.chain2.add(c2);

				IntStream.of(0, 1).forEach(k -> {

					NoIdListChain1 c1 = new NoIdListChain1();
					c1.oneValue = c2.twoValue + k;
					c2.chain1.add(c1);

					IntStream.of(0, 1).forEach(m -> {

						NoIdListChain0 c0 = new NoIdListChain0();
						c0.zeroValue = c1.oneValue + m;
						c1.chain0.add(c0);
					});
				});
			});
		});

		return chain4;
	}

	@Test // DATAJDBC-223
	public void saveAndLoadLongChainOfMapsWithoutIds() {

		NoIdMapChain4 saved = template.save(createNoIdMapTree());

		assertThat(saved.four).isNotNull();

		NoIdMapChain4 reloaded = template.findById(saved.four, NoIdMapChain4.class);
		assertThat(reloaded).isEqualTo(saved);
	}

	@Test // DATAJDBC-223
	public void shouldDeleteChainOfMapsWithoutIds() {

		NoIdMapChain4 saved = template.save(createNoIdMapTree());
		template.deleteById(saved.four, NoIdMapChain4.class);

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(count("NO_ID_MAP_CHAIN4")).describedAs("Chain4 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_MAP_CHAIN3")).describedAs("Chain3 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_MAP_CHAIN2")).describedAs("Chain2 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_MAP_CHAIN1")).describedAs("Chain1 elements got deleted").isEqualTo(0);
			softly.assertThat(count("NO_ID_MAP_CHAIN0")).describedAs("Chain0 elements got deleted").isEqualTo(0);
		});
	}

	private static NoIdMapChain4 createNoIdMapTree() {

		NoIdMapChain4 chain4 = new NoIdMapChain4();
		chain4.fourValue = "v";

		IntStream.of(0, 1).forEach(i -> {

			NoIdMapChain3 c3 = new NoIdMapChain3();
			c3.threeValue = chain4.fourValue + i;
			chain4.chain3.put(asString(i), c3);

			IntStream.of(0, 1).forEach(j -> {

				NoIdMapChain2 c2 = new NoIdMapChain2();
				c2.twoValue = c3.threeValue + j;
				c3.chain2.put(asString(j), c2);

				IntStream.of(0, 1).forEach(k -> {

					NoIdMapChain1 c1 = new NoIdMapChain1();
					c1.oneValue = c2.twoValue + k;
					c2.chain1.put(asString(k), c1);

					IntStream.of(0, 1).forEach(it -> {

						NoIdMapChain0 c0 = new NoIdMapChain0();
						c0.zeroValue = c1.oneValue + it;
						c1.chain0.put(asString(it), c0);
					});
				});
			});
		});

		return chain4;
	}

	private static String asString(int i) {
		return "_" + i;
	}

	private Long count(String tableName) {
		return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM DEMO." + tableName, emptyMap(), Long.class);
	}

	@Data
	static class LegoSet {

		@Column("id1") @Id private Long id;

		private String name;

		private Manual manual;
		@Column("alternative") private Manual alternativeInstructions;
	}

	@Data
	static class Manual {

		@Column("id2") @Id private Long id;
		private String content;

	}

	static class ChildNoId {
		private String content;
	}

	static class ListParent {

		@Column("id4") @Id private Long id;
		String name;
		List<ElementNoId> content = new ArrayList<>();
	}

	static class ElementNoId {
		private String content;
	}

	/**
	 * One may think of ChainN as a chain with N further elements
	 */
	@EqualsAndHashCode
	static class NoIdListChain0 {
		String zeroValue;
	}

	@EqualsAndHashCode
	static class NoIdListChain1 {
		String oneValue;
		List<NoIdListChain0> chain0 = new ArrayList<>();
	}

	@EqualsAndHashCode
	static class NoIdListChain2 {
		String twoValue;
		List<NoIdListChain1> chain1 = new ArrayList<>();
	}

	@EqualsAndHashCode
	static class NoIdListChain3 {
		String threeValue;
		List<NoIdListChain2> chain2 = new ArrayList<>();
	}

	@EqualsAndHashCode
	static class NoIdListChain4 {
		@Id Long four;
		String fourValue;
		List<NoIdListChain3> chain3 = new ArrayList<>();
	}

	/**
	 * One may think of ChainN as a chain with N further elements
	 */
	@EqualsAndHashCode
	static class NoIdMapChain0 {
		String zeroValue;
	}

	@EqualsAndHashCode
	static class NoIdMapChain1 {
		String oneValue;
		Map<String, NoIdMapChain0> chain0 = new HashMap<>();
	}

	@EqualsAndHashCode
	static class NoIdMapChain2 {
		String twoValue;
		Map<String, NoIdMapChain1> chain1 = new HashMap<>();
	}

	@EqualsAndHashCode
	static class NoIdMapChain3 {
		String threeValue;
		Map<String, NoIdMapChain2> chain2 = new HashMap<>();
	}

	@EqualsAndHashCode
	static class NoIdMapChain4 {
		@Id Long four;
		String fourValue;
		Map<String, NoIdMapChain3> chain3 = new HashMap<>();
	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		Class<?> testClass() {
			return JdbcAggregateTemplateWithSchemaIntegrationTests.class;
		}

		@Bean
		NamingStrategy namingStrategy() {
			return new NamingStrategy() {
				@Override
				public String getSchema() {
					return "demo";
				}
			};
		}

		@Bean
		JdbcAggregateOperations operations(ApplicationEventPublisher publisher, RelationalMappingContext context,
				DataAccessStrategy dataAccessStrategy, JdbcConverter converter) {
			return new JdbcAggregateTemplate(publisher, context, converter, dataAccessStrategy);
		}
	}
}
