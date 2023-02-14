/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.sql.language.validate;


import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.util.Conformance;

/**
 * Abstract base class for implementing {@link Conformance}.
 *
 * Every method in {@code SqlConformance} is implemented, and behaves the same as in {@link ConformanceEnum#DEFAULT}.
 */
public abstract class SqlAbstractConformance implements Conformance {

    @Override
    public boolean isLiberal() {
        return ConformanceEnum.DEFAULT.isLiberal();
    }


    @Override
    public boolean isGroupByAlias() {
        return ConformanceEnum.DEFAULT.isGroupByAlias();
    }


    @Override
    public boolean isGroupByOrdinal() {
        return ConformanceEnum.DEFAULT.isGroupByOrdinal();
    }


    @Override
    public boolean isHavingAlias() {
        return ConformanceEnum.DEFAULT.isHavingAlias();
    }


    @Override
    public boolean isSortByOrdinal() {
        return ConformanceEnum.DEFAULT.isSortByOrdinal();
    }


    @Override
    public boolean isSortByAlias() {
        return ConformanceEnum.DEFAULT.isSortByAlias();
    }


    @Override
    public boolean isSortByAliasObscures() {
        return ConformanceEnum.DEFAULT.isSortByAliasObscures();
    }


    @Override
    public boolean isFromRequired() {
        return ConformanceEnum.DEFAULT.isFromRequired();
    }


    @Override
    public boolean isBangEqualAllowed() {
        return ConformanceEnum.DEFAULT.isBangEqualAllowed();
    }


    @Override
    public boolean isMinusAllowed() {
        return ConformanceEnum.DEFAULT.isMinusAllowed();
    }


    @Override
    public boolean isApplyAllowed() {
        return ConformanceEnum.DEFAULT.isApplyAllowed();
    }


    @Override
    public boolean isInsertSubsetColumnsAllowed() {
        return ConformanceEnum.DEFAULT.isInsertSubsetColumnsAllowed();
    }


    @Override
    public boolean allowNiladicParentheses() {
        return ConformanceEnum.DEFAULT.allowNiladicParentheses();
    }


    @Override
    public boolean allowExplicitRowValueConstructor() {
        return ConformanceEnum.DEFAULT.allowExplicitRowValueConstructor();
    }


    @Override
    public boolean allowExtend() {
        return ConformanceEnum.DEFAULT.allowExtend();
    }


    @Override
    public boolean isLimitStartCountAllowed() {
        return ConformanceEnum.DEFAULT.isLimitStartCountAllowed();
    }


    @Override
    public boolean isPercentRemainderAllowed() {
        return ConformanceEnum.DEFAULT.isPercentRemainderAllowed();
    }


    @Override
    public boolean allowGeometry() {
        return ConformanceEnum.DEFAULT.allowGeometry();
    }


    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        return ConformanceEnum.DEFAULT.shouldConvertRaggedUnionTypesToVarying();
    }


    @Override
    public boolean allowExtendedTrim() {
        return ConformanceEnum.DEFAULT.allowExtendedTrim();
    }

}
