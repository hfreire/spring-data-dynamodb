/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.socialsignin.spring.data.dynamodb.query;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

/**
 * @author Michael Lavelle
 */
public class SingleEntityLoadByHashAndRangeKeyQuery<T> extends AbstractSingleEntityQuery<T> implements Query<T> {

	private Object hashKey;
	private Object rangeKey;

	public SingleEntityLoadByHashAndRangeKeyQuery(DynamoDBMapper dynamoDBMapper, Class<T> clazz, Object hashKey, Object rangeKey) {
		super(dynamoDBMapper, clazz);
		this.hashKey = hashKey;
		this.rangeKey = rangeKey;
	}

	@Override
	public T getSingleResult() {
		return dynamoDBMapper.load(clazz, hashKey, rangeKey);
	}

}
