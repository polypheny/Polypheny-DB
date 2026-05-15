/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.shared.filter;

import java.util.List;

/**
 * Filter bundle used by the nested Parquet join executor.
 * <p>
 * A nested join has more filter buckets than a normal scan because filters can
 * come from two different planner locations and can use two different column
 * coordinate systems:
 * <ul>
 *     <li>
 *         <b>Join-owned filters</b> are filters attached above or directly to
 *         the {@code P_JOIN}. Their {@link ParquetAdapterFilter#columnIndex()}
 *         values use the joined output row, i.e. {@code leftFields} followed by
 *         {@code rightFields}. These filters are split into {@link #parentFilters()},
 *         {@link #childFilters()}, and {@link #adapterFilters()}.
 *     </li>
 *     <li>
 *         <b>Scan-owned filters</b> are filters pushed into the left or right
 *         {@code P_SCAN} before the join is created. Their
 *         {@link ParquetAdapterFilter#columnIndex()} values use the physical
 *         table column index of that scan's table. These filters are stored in
 *         {@link #parentScanFilters()} and {@link #childScanFilters()} and are
 *         intentionally not remapped through the join projection.
 *     </li>
 * </ul>
 *
 * <h2>Example</h2>
 * For a query like:
 *
 * <pre>{@code
 * SELECT o.order_id, i.quantity
 * FROM pon__orders o
 * JOIN pon__orders__items i
 *   ON i.__polypheny_parent_row_id = o.__polypheny_row_id
 * WHERE o.total_price >= 0 AND i.quantity > 3
 * }</pre>
 *
 * If the join output fields are:
 *
 * <pre>{@code
 * leftFields  = [__polypheny_row_id (#0), order_id (#1)]
 * rightFields = [__polypheny_parent_row_id (#1), quantity (#5)]
 * }</pre>
 *
 * then a join-owned filter on {@code right.quantity} would use the joined row
 * index {@code 3}: two left fields plus the second right field. That kind of
 * filter belongs in {@link #childFilters()} when {@code leftIsParent == true}.
 * <p>
 * A scan-owned filter on the same {@code quantity} column uses the physical
 * table index {@code 5}, regardless of whether {@code quantity} is currently
 * the second projected field of the child scan. That kind of filter belongs in
 * {@link #childScanFilters()}.
 * <p>
 * This distinction is important for projected scans. A filter such as
 * {@code o.total_price >= 0} can be applied even when {@code total_price} is
 * not part of the scan output, because the scan-owned filter keeps physical
 * column {@code total_price (#5)} while {@code leftFields} can still expose
 * only {@code __polypheny_row_id} and {@code order_id}.
 *
 * <h2>Bucket summary</h2>
 * <ul>
 *     <li>{@link #parentFilters()} - join-owned filters that reference only
 *     parent-side joined-row fields.</li>
 *     <li>{@link #childFilters()} - join-owned filters that reference only
 *     child-side joined-row fields.</li>
 *     <li>{@link #adapterFilters()} - join-owned filters that cannot be assigned
 *     to exactly one side, for example an {@code OR} crossing parent and child
 *     fields. These are evaluated after joined rows are produced.</li>
 *     <li>{@link #nativeFilters()} - filters that can be given to the Parquet
 *     reader. In nested joins this is currently parent-side only because the
 *     reader is opened on the parent/root file before repeated child rows are
 *     expanded.</li>
 *     <li>{@link #parentScanFilters()} - scan-owned physical-index filters for
 *     the parent table.</li>
 *     <li>{@link #childScanFilters()} - scan-owned physical-index filters for
 *     the child table.</li>
 * </ul>
 */
public class JoinFiltersContainer extends FiltersContainer {

    public static JoinFiltersContainer empty = new JoinFiltersContainer( List.of(), List.of(), List.of(), List.of() );

    private final List<ParquetAdapterFilter> parentFilters;
    private final List<ParquetAdapterFilter> childFilters;
    private final List<ParquetAdapterFilter> parentScanFilters;
    private final List<ParquetAdapterFilter> childScanFilters;


    /**
     * Creates a join filter container for join-owned filters only.
     * <p>
     * Use this constructor when all filters are expressed in joined-row
     * coordinates and there are no filters inherited from the input scans.
     * The provided filter lists usually come from {@link JoinFiltersSplitter}.
     *
     * @param parentFilters join-owned filters that reference only parent fields,
     * still using joined-row column indexes.
     * @param childFilters join-owned filters that reference only child fields,
     * still using joined-row column indexes.
     * @param adapterFilters join-owned filters that must be evaluated after a
     * joined row exists, for example filters spanning both sides.
     * @param readerFilters join-owned filters that can also be pushed to the
     * Parquet reader as native predicates.
     */
    public JoinFiltersContainer( List<ParquetAdapterFilter> parentFilters, List<ParquetAdapterFilter> childFilters, List<ParquetAdapterFilter> adapterFilters, List<ParquetAdapterFilter> readerFilters ) {
        this( parentFilters, childFilters, adapterFilters, readerFilters, List.of(), List.of() );
    }


    /**
     * Creates a join filter container with both join-owned and scan-owned filters.
     * <p>
     * The first four lists have the same meaning as in
     * {@link #JoinFiltersContainer(List, List, List, List)} and use joined-row
     * column indexes when they refer to row fields.
     * <p>
     * The last two lists are different: they contain filters copied from the
     * input scans, so their column indexes are physical table column indexes.
     * For example, if {@code pon__orders.total_price} is physical column
     * {@code #5}, the parent scan filter keeps {@code columnIndex = 5} even if
     * the parent join output only contains columns {@code #0} and {@code #1}.
     *
     * @param parentFilters join-owned filters that reference only parent fields,
     * using joined-row column indexes.
     * @param childFilters join-owned filters that reference only child fields,
     * using joined-row column indexes.
     * @param adapterFilters join-owned filters evaluated after joined rows are
     * produced.
     * @param readerFilters filters that can be evaluated by the Parquet reader.
     * @param parentScanFilters scan-owned filters for the parent table, using
     * parent physical table column indexes.
     * @param childScanFilters scan-owned filters for the child table, using
     * child physical table column indexes.
     */
    public JoinFiltersContainer( List<ParquetAdapterFilter> parentFilters, List<ParquetAdapterFilter> childFilters, List<ParquetAdapterFilter> adapterFilters, List<ParquetAdapterFilter> readerFilters, List<ParquetAdapterFilter> parentScanFilters, List<ParquetAdapterFilter> childScanFilters ) {
        super( adapterFilters, readerFilters );
        this.parentFilters = parentFilters == null ? List.of() : List.copyOf( parentFilters );
        this.childFilters = childFilters == null ? List.of() : List.copyOf( childFilters );
        this.parentScanFilters = parentScanFilters == null ? List.of() : List.copyOf( parentScanFilters );
        this.childScanFilters = childScanFilters == null ? List.of() : List.copyOf( childScanFilters );
    }


    /**
     * Gets join-owned filters that reference only parent-side fields.
     * <p>
     * These filters use joined-row column indexes, not physical table indexes.
     * If {@code leftIsParent == true}, parent field {@code leftFields[1]} has
     * joined index {@code 1}. If {@code leftIsParent == false}, parent fields
     * start after all child/left fields.
     *
     * @return parent-only join-owned filters.
     */
    public List<ParquetAdapterFilter> parentFilters() {
        return parentFilters;
    }


    /**
     * Gets join-owned filters that reference only child-side fields.
     * <p>
     * These filters use joined-row column indexes. For example, when the parent
     * is on the left and there are two left fields, the first right/child field
     * has joined index {@code 2}.
     *
     * @return child-only join-owned filters.
     */
    public List<ParquetAdapterFilter> childFilters() {
        return childFilters;
    }


    /**
     * Gets scan-owned filters for the parent table.
     * <p>
     * These filters use parent physical table column indexes and are evaluated
     * against parent groups before child rows are expanded. They are the right
     * place for predicates that were pushed into the parent {@code P_SCAN}, such
     * as {@code pon__orders.total_price (#5) >= 0}.
     *
     * @return parent scan filters using physical table indexes.
     */
    public List<ParquetAdapterFilter> parentScanFilters() {
        return parentScanFilters;
    }


    /**
     * Gets scan-owned filters for the child table.
     * <p>
     * These filters use child physical table column indexes and are evaluated
     * against each child group after the parent row has been expanded to its
     * repeated child rows. They are the right place for predicates that were
     * pushed into the child {@code P_SCAN}, such as
     * {@code pon__orders__items.quantity (#5) > 3}.
     *
     * @return child scan filters using physical table indexes.
     */
    public List<ParquetAdapterFilter> childScanFilters() {
        return childScanFilters;
    }

}
