/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.socialsignin.spring.data.dynamodb.repository.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.socialsignin.spring.data.dynamodb.query.Query;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

/**
 * @author Michael Lavelle
 */
public abstract class AbstractDynamoDBQuery<T, ID extends Serializable> implements RepositoryQuery {

	protected final DynamoDBMapper dynamoDBMapper;
	private final DynamoDBQueryMethod<T, ID> method;

	public AbstractDynamoDBQuery(DynamoDBMapper dynamoDBMapper, DynamoDBQueryMethod<T, ID> method) {
		this.dynamoDBMapper = dynamoDBMapper;
		this.method = method;
	}

	protected QueryExecution<T, ID> getExecution() {

		if (method.isCollectionQuery()) {
			return new CollectionExecution();
		} else if (method.isPageQuery()) {
			return new PagedExecution(method.getParameters());
		} else if (method.isModifyingQuery()) {
			throw new UnsupportedOperationException("Modifying queries not yet supported");
		} else {
			return new SingleEntityExecution();
		}
	}

	protected abstract Query<T> doCreateQuery(Object[] values);

	protected Query<T> doCreateQueryWithPermissions(Object values[]) {
		Query<T> query = doCreateQuery(values);
		query.setScanEnabled(method.isScanEnabled());
		return query;
	}

	private interface QueryExecution<T, ID extends Serializable> {
		public Object execute(AbstractDynamoDBQuery<T, ID> query, Object[] values);
	}

	class CollectionExecution implements QueryExecution<T, ID> {

		@Override
		public Object execute(AbstractDynamoDBQuery<T, ID> dynamoDBQuery, Object[] values) {
			Query<T> query = dynamoDBQuery.doCreateQueryWithPermissions(values);
			return query.getResultList();
		}

	}

	/**
	 * Executes the {@link AbstractStringBasedJpaQuery} to return a
	 * {@link org.springframework.data.domain.Page} of entities.
	 */
	class PagedExecution implements QueryExecution<T, ID> {

		private final Parameters<?, ?> parameters;

		public PagedExecution(Parameters<?, ?> parameters) {

			this.parameters = parameters;
		}

		private int scanThroughResults(Iterator<T> iterator, int resultsToScan) {
			int processed = 0;
			while (iterator.hasNext() && processed < resultsToScan) {
				iterator.next();
				processed++;
			}
			return processed;
		}

		private List<T> readPageOfResults(Iterator<T> iterator, int pageSize) {
			int processed = 0;
			List<T> resultsPage = new ArrayList<T>();
			while (iterator.hasNext() && processed < pageSize) {
				resultsPage.add(iterator.next());
				processed++;
			}
			return resultsPage;
		}

		@Override
		public Object execute(AbstractDynamoDBQuery<T, ID> dynamoDBQuery, Object[] values) {

			ParameterAccessor accessor = new ParametersParameterAccessor(parameters, values);
			Pageable pageable = accessor.getPageable();
			Query<T> query = dynamoDBQuery.doCreateQueryWithPermissions(values);
			List<T> results = query.getResultList();
			return createPage(results, pageable);
		}

		private Page<T> createPage(List<T> allResults, Pageable pageable) {

			Iterator<T> iterator = allResults.iterator();
			int processedCount = 0;
			if (pageable.getOffset() > 0) {
				processedCount = scanThroughResults(iterator, pageable.getOffset());
				if (processedCount < pageable.getOffset())
					return new PageImpl<T>(new ArrayList<T>());
			}
			List<T> results = readPageOfResults(iterator, pageable.getPageSize());
			// Scan ahead to retrieve the next page count
			int nextPageItemCount = scanThroughResults(iterator, pageable.getPageSize());
			boolean hasMoreResults = nextPageItemCount > 0;
			int totalProcessed = processedCount + results.size();
			// Set total count to be the number already returned, or the number
			// returned added to the count of the next page
			// This allows paging to determine next/page prev page correctly,
			// even though we are unable to return
			// the actual count of total results due to the way DynamoDB scans
			// results
			int totalCount = hasMoreResults ? (totalProcessed + nextPageItemCount) : totalProcessed;
			return new PageImpl<T>(results, pageable, totalCount);

		}
	}

	class SingleEntityExecution implements QueryExecution<T, ID> {

		@Override
		public Object execute(AbstractDynamoDBQuery<T, ID> dynamoDBQuery, Object[] values) {

			return dynamoDBQuery.doCreateQueryWithPermissions(values).getSingleResult();

		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.repository.query.RepositoryQuery#execute(java
	 * .lang.Object[])
	 */
	public Object execute(Object[] parameters) {

		return getExecution().execute(this, parameters);
	}

	@Override
	public DynamoDBQueryMethod<T, ID> getQueryMethod() {
		return this.method;
	}

}
