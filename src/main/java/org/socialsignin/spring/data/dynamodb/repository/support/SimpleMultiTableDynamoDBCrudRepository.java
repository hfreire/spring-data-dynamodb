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
package org.socialsignin.spring.data.dynamodb.repository.support;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import org.socialsignin.spring.data.dynamodb.repository.MultiTableDynamoDBCrudRepository;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.List;

public class SimpleMultiTableDynamoDBCrudRepository<T, ID extends Serializable> extends SimpleDynamoDBPagingAndSortingRepository<T, ID>
        implements MultiTableDynamoDBCrudRepository<T, ID> {


    public SimpleMultiTableDynamoDBCrudRepository(DynamoDBEntityInformation<T, ID> entityInformation, DynamoDBMapper dynamoDBMapper, EnableScanPermissions enableScanPermissions) {
        super(entityInformation, dynamoDBMapper, enableScanPermissions);
    }

    @Override
    public List<T> findAllInTable(String tableName) {
        assertScanEnabled(enableScanPermissions.isFindAllUnpaginatedScanEnabled(), "findAll");
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        return dynamoDBMapper.scan(domainType, scanExpression, new DynamoDBMapperConfig(new TableNameOverride(tableName)));
    }

    @Override
    public T saveInTable(String tableName, T t) {
        dynamoDBMapper.save(t, new DynamoDBMapperConfig(new DynamoDBMapperConfig.TableNameOverride(tableName)));
        return t;
    }

    @Override
    public T findOneInTable(String tableName, ID id) {
        if (entityInformation.isRangeKeyAware()) {
            return dynamoDBMapper.load(domainType, entityInformation.getHashKey(id), entityInformation.getRangeKey(id));
        } else {
            return dynamoDBMapper.load(domainType, entityInformation.getHashKey(id), new DynamoDBMapperConfig(new TableNameOverride(tableName)));
        }
    }

    @Override
    public void deleteInTable(String tableName, ID id) {
        Assert.notNull(id, "The given id must not be null!");

        T entity = findOne(id);
        if (entity == null) {
            throw new EmptyResultDataAccessException(String.format("No %s entity with id %s exists!", domainType, id), 1);
        }
        dynamoDBMapper.delete(entity, new DynamoDBMapperConfig(new TableNameOverride(tableName)));
    }
}
