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

import java.lang.reflect.Method;

import org.socialsignin.spring.data.dynamodb.repository.EnableScan;
import org.springframework.util.ReflectionUtils;

/**
 * 
 * @author Michael Lavelle
 * 
 */
public class EnableScanAnnotationPermissions implements EnableScanPermissions {

	private boolean findAllUnpaginatedScanEnabled = false;
	private boolean countUnpaginatedScanEnabled = false;
	private boolean deleteAllUnpaginatedScanEnabled = false;

	public EnableScanAnnotationPermissions(Class<?> repositoryInterface) {
		// Check to see if global EnableScan is declared at interface level
		if (repositoryInterface.isAnnotationPresent(EnableScan.class)) {
			this.findAllUnpaginatedScanEnabled = true;
			this.countUnpaginatedScanEnabled = true;
			this.deleteAllUnpaginatedScanEnabled = true;
		} else {
			// Check declared methods for EnableScan annotation
			for (Method method : ReflectionUtils.getAllDeclaredMethods(repositoryInterface)) {

				if (!method.isAnnotationPresent(EnableScan.class) || method.getParameterTypes().length > 0) {
					// Only consider methods which have the EnableScan
					// annotation and which accept no parameters
					continue;
				}

				if (method.getName().equals("findAll")) {
					findAllUnpaginatedScanEnabled = true;
					continue;
				}

				if (method.getName().equals("deleteAll")) {
					deleteAllUnpaginatedScanEnabled = true;
					continue;
				}

				if (method.getName().equals("count")) {
					countUnpaginatedScanEnabled = true;
					continue;
				}

			}
		}

	}

	@Override
	public boolean isFindAllUnpaginatedScanEnabled() {
		return findAllUnpaginatedScanEnabled;

	}

	@Override
	public boolean isDeleteAllUnpaginatedScanEnabled() {
		return deleteAllUnpaginatedScanEnabled;
	}

	@Override
	public boolean isCountUnpaginatedScanEnabled() {
		return countUnpaginatedScanEnabled;
	}

}
