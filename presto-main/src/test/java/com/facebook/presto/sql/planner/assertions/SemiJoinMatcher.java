/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

final class SemiJoinMatcher
        implements Matcher
{
    private final String sourceSymbolAlias;
    private final String filteringSymbolAlias;
    private final String outputAlias;
    private final SemiJoinNode.DistributionType distributionType;

    SemiJoinMatcher(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias)
    {
        this(sourceSymbolAlias, filteringSymbolAlias, outputAlias, null);
    }

    SemiJoinMatcher(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, SemiJoinNode.DistributionType distributionType)
    {
        this.sourceSymbolAlias = requireNonNull(sourceSymbolAlias, "sourceSymbolAlias is null");
        this.filteringSymbolAlias = requireNonNull(filteringSymbolAlias, "filteringSymbolAlias is null");
        this.outputAlias = requireNonNull(outputAlias, "outputAlias is null");
        this.distributionType = distributionType;
    }

    @Override
    public boolean matches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        if (node instanceof SemiJoinNode) {
            SemiJoinNode semiJoinNode = (SemiJoinNode) node;
            if (distributionType != null && (!semiJoinNode.getDistributionType().isPresent() || semiJoinNode.getDistributionType().get() != distributionType)) {
                return false;
            }
            expressionAliases.put(sourceSymbolAlias, semiJoinNode.getSourceJoinSymbol().toSymbolReference());
            expressionAliases.put(filteringSymbolAlias, semiJoinNode.getFilteringSourceJoinSymbol().toSymbolReference());
            expressionAliases.put(outputAlias, semiJoinNode.getSemiJoinOutput().toSymbolReference());
            return true;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("filteringSymbolAlias", filteringSymbolAlias)
                .add("sourceSymbolAlias", sourceSymbolAlias)
                .add("outputAlias", outputAlias)
                .toString();
    }
}
